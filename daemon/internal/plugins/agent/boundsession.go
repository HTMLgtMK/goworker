package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"

	runtimeagent "github.com/tinguo/goworker/ai-runtime/agent"
	"github.com/tinguo/goworker/ai-runtime/session"
	"github.com/tinguo/goworker/daemon/internal/core/model"
)

// 会话创建、重开与归档的跨层流程见 ../../../docs/README.md。
// 创建和加载的管理操作由调用方经 SessionGate 串行化；历史和归档探测不要求该门禁。
// 绑定登记与 runtime 收口还由 lifecycleMu 协调。
var _ model.BoundSessionSource = (*AgentPlugin)(nil)
var _ model.BoundSessionLoader = (*AgentPlugin)(nil)

// boundSessionUnknownErr preserves both daemon and runtime not-found sentinels so
// protocol and routing callers can classify the same missing session.
func boundSessionUnknownErr(sessionID string) error {
	return fmt.Errorf("agent: session %q: %w (%w)", sessionID, model.ErrSessionUnknown, runtimeagent.ErrSessionNotFound)
}

// CreateBoundSession runs under the caller's SessionGate. It validates cwd before
// creating persistent state and closes the new store if runtime mounting fails.
func (p *AgentPlugin) CreateBoundSession(cwd string) (string, error) {
	if p.repo == nil {
		return "", fmt.Errorf("agent: session persistence disabled")
	}
	declared, err := normalizeSessionCWD(cwd)
	if err != nil {
		return "", err
	}
	if err := p.validateSessionCWD(declared); err != nil {
		return "", err
	}
	st, err := p.repo.Create(context.Background(), session.CreateOptions{CWD: declared})
	if err != nil {
		return "", fmt.Errorf("agent: create session: %w", err)
	}
	rt, err := p.mountRuntime(st)
	if err != nil {
		// 注册失败（同 id 冲突仅理论可达——Create 的 id 是 128-bit 随机数）：
		// 回收刚建好的 store，不留孤儿缓存条目。失败只 warn：创建已失败，收口
		// 是尽力而为的卫生动作。
		if cerr := p.repo.CloseSession(st.SessionID()); cerr != nil {
			slog.Warn("agent: discard created session store failed", "session", st.SessionID(), "err", cerr)
		}
		return "", err
	}
	p.declareSessionCWD(rt, declared)
	return st.SessionID(), nil
}

// LoadBoundSession reloads a registered session or opens and mounts a persisted one.
// Invalid cwd is recorded rather than blocking access to existing history. Reopened
// stores also mark non-terminal runs left by the previous process as interrupted.
func (p *AgentPlugin) LoadBoundSession(sessionID, cwd string) error {
	if rt := p.RouteSession(sessionID); rt != nil {
		// 分支 1：reload 已注册会话的视图。
		if s := rt.Session(); s != nil {
			if err := s.ReloadFromStore(); err != nil {
				return fmt.Errorf("agent: reload session %s: %w", sessionID, err)
			}
		}
		if normalized := normalizeSessionCWDSilent(cwd); normalized != "" {
			p.declareSessionCWD(rt, normalized)
		}
		return nil
	}
	// 分支 2：重开持久化但不在册的会话。纯内存降级（无 repo）下「不在册」即
	// 「不存在」——无持久化事实可恢复，诚实报未知。
	if p.repo == nil {
		return boundSessionUnknownErr(sessionID)
	}
	st, err := p.repo.Open(sessionID)
	if err != nil {
		if errors.Is(err, session.ErrSessionNotFound) {
			return boundSessionUnknownErr(sessionID)
		}
		return fmt.Errorf("agent: reopen session %s: %w", sessionID, err)
	}
	rt, err := p.mountRuntime(st)
	if err != nil {
		return err
	}
	// P2-7 恢复扫描（startSession 同款）：重开边界的 store 若挂着上一进程遗留
	// 的非终态 run journal，追加 interrupted 标记。失败仅 warn 不阻塞重开。
	if marked, err := p.repo.MarkInterruptedRuns(context.Background(), st.SessionID()); err != nil {
		slog.Warn("agent: mark interrupted runs failed on reopen", "err", err, "session", st.SessionID())
	} else if len(marked) > 0 {
		slog.Warn("agent: marked interrupted runs from previous process on reopen", "count", len(marked), "session", st.SessionID(), "runs", strings.Join(marked, ","))
	}
	// cwd 恢复：实参非空优先（客户端重申），否则 workspace 记录（A1 访问器；
	// 从未声明过则维持未声明——执行域回退既有语义，A4 的 tmp 注入接管空 cwd）。
	restore := normalizeSessionCWDSilent(cwd)
	if restore == "" {
		restore = st.Workspace()
	}
	p.declareSessionCWD(rt, restore)
	return nil
}

func (p *AgentPlugin) LoadBoundSessionForConnection(sessionID, cwd, connectionID string) ([]model.SessionUpdate, error) {
	p.lifecycleMu.Lock()
	defer p.lifecycleMu.Unlock()

	if err := p.LoadBoundSession(sessionID, cwd); err != nil {
		return nil, err
	}
	history, err := p.SessionHistory(sessionID)
	if err != nil {
		p.tryReleaseSessionLocked(sessionID)
		return nil, err
	}
	if p.control != nil {
		p.bindConnectionSessionLocked(connectionID, sessionID)
	}
	return history, nil
}

// SessionHistory replays a registered session's full history before session/load. It reads
// a read-only store snapshot through replayUpdates. Unknown sessions error; in-memory or
// closed persistence returns an empty slice.
func (p *AgentPlugin) SessionHistory(sessionID string) ([]model.SessionUpdate, error) {
	rt := p.RouteSession(sessionID)
	if rt == nil {
		return nil, boundSessionUnknownErr(sessionID)
	}
	store := p.storeFor(rt)
	if store == nil {
		return []model.SessionUpdate{}, nil
	}
	return replayUpdates(store.ActiveView(), checkpointAnchors(store)), nil
}

// SessionArchived 实现 model.BoundSessionSource：repo 层只读探测会话是否已落
// archived 终态标记（Plan A1 记录模型）。探测是 session/load 只读分流的前提
// （handler 无门禁调用——终态重放是纯只读，与在飞 prompt 无共享可变状态），
// repo.Open 内部自锁（r.mu + flock），与门内操作无锁序交叉。
func (p *AgentPlugin) SessionArchived(sessionID string) bool {
	if p.repo == nil {
		return false
	}
	st, err := p.repo.Open(sessionID)
	if err != nil {
		return false
	}
	return st.Archived()
}

// mountRuntime builds a runtime with session-specific store and journal dependencies,
// restores archived state, then registers it with the manager.
func (p *AgentPlugin) mountRuntime(st *session.Store) (*runtimeagent.SessionRuntime, error) {
	deps := p.deps
	deps.Store = st
	journalStore := session.NewRunJournalStore(p.repo, st)
	deps.RunJournal = journalStore
	deps.RunEvents = journalStore
	rt := runtimeagent.NewSessionRuntime(st.SessionID(), runtimeagent.NewSession(deps))
	if st.Archived() {
		if err := rt.Archive(nil); err != nil {
			return nil, fmt.Errorf("agent: restore archived runtime %s: %w", rt.ID, err)
		}
	}
	if err := p.manager.Register(rt); err != nil {
		return nil, fmt.Errorf("agent: register session runtime %s: %w", rt.ID, err)
	}
	return rt, nil
}

// declareSessionCWD applies the declared cwd, injecting a temporary workspace when
// none is declared. Validation failures are retained on the runtime as cwd errors.
func (p *AgentPlugin) declareSessionCWD(rt *runtimeagent.SessionRuntime, cwd string) {
	if cwd == "" {
		tmp, err := p.injectTmpWorkspace(rt.ID)
		if err != nil {
			rt.DeclareCWD("", err)
			slog.Warn("agent: inject default workspace failed", "session", rt.ID, "err", err)
			return
		}
		p.applySessionCWD(rt, tmp, false)
		return
	}
	p.applySessionCWD(rt, cwd, true)
}

// applySessionCWD validates and applies cwd to the runtime, session, and optionally
// the store. Store-write failures are logged; the in-memory cwd still reflects validation.
func (p *AgentPlugin) applySessionCWD(rt *runtimeagent.SessionRuntime, cwd string, record bool) {
	if err := p.validateSessionCWD(cwd); err != nil {
		rt.DeclareCWD("", err)
		slog.Warn("agent: reject session cwd", "session", rt.ID, "cwd", cwd, "err", err)
		return
	}
	if record {
		if st := p.storeFor(rt); st != nil && st.Workspace() != cwd {
			if err := st.DeclareWorkspace(cwd); err != nil {
				slog.Warn("agent: declare workspace record failed", "session", rt.ID, "cwd", cwd, "err", err)
			}
		}
	}
	rt.DeclareCWD(cwd, nil)
	if s := rt.Session(); s != nil {
		s.SetCWD(cwd)
	}
}

// normalizeSessionCWD 规范化声明的 cwd（TrimSpace + Abs——cmd 与工具执行要求
// 绝对路径，回显也取规范化值，SetSession 同款）。空串合法（= 未声明）。
func normalizeSessionCWD(cwd string) (string, error) {
	cwd = strings.TrimSpace(cwd)
	if cwd == "" {
		return "", nil
	}
	abs, err := filepath.Abs(cwd)
	if err != nil {
		return "", fmt.Errorf("agent: invalid session cwd %q: %w", cwd, err)
	}
	return abs, nil
}

// normalizeSessionCWDSilent 是 normalizeSessionCWD 的声明面变体：Abs 失败时
// 原样返回未规范化值而非报错——load 面的 cwd 无效走 declareSessionCWD 的
// 校验/标记路径（validateSessionCWD 对不可解析路径同样报错），不在这里二次
// 分叉错误形态。
func normalizeSessionCWDSilent(cwd string) string {
	normalized, err := normalizeSessionCWD(cwd)
	if err != nil {
		return strings.TrimSpace(cwd)
	}
	return normalized
}

// ArchiveSession runs under the caller's SessionGate and lifecycleMu. It fences the
// archive with the control lease; runtime admission prevents new runs before persistence.
func (p *AgentPlugin) ArchiveSession(sessionID, clientID string) error {
	if p.repo == nil {
		return fmt.Errorf("agent: session persistence disabled")
	}
	if p.control == nil {
		return fmt.Errorf("agent: archive session: control plane unavailable")
	}
	p.lifecycleMu.Lock()
	defer p.lifecycleMu.Unlock()

	// ① 寻址（先于 fence：未知会话诚实拒绝，ErrSessionUnknown 契约）。
	//    在册 runtime 的 store 经实例唯一缓存（runtime 持有的活实例，不重复
	//    打开）；不在册走 repo.Open（flock 空闲即活实例，与 LoadBoundSession
	//    重开同链）。
	rt := p.RouteSession(sessionID)
	var st *session.Store
	if rt != nil {
		st = p.storeFor(rt)
		if st == nil {
			// 纯内存降级的 runtime（id 非 store id）：无持久化事实可标记，
			// 终态无处落——诚实报错而非静默 no-op。
			return fmt.Errorf("agent: session %q has no persisted store (纯内存降级，无法归档)", sessionID)
		}
	} else {
		opened, err := p.repo.Open(sessionID)
		if err != nil {
			if errors.Is(err, session.ErrSessionNotFound) {
				return boundSessionUnknownErr(sessionID)
			}
			return fmt.Errorf("agent: archive session %s: %w", sessionID, err)
		}
		st = opened
	}
	// ②-④ fencing 临界区：lease 校验、Runtime admission 与 store 归档在同一生命周期
	// 临界区内完成。Runtime 先进入只读状态，任何后续 Run 提交都会被拒绝；store
	// 写失败则回滚 admission，避免出现内存只读但持久化未归档的半状态。
	err := p.control.WithControl(sessionID, clientID, func() error {
		if rt != nil {
			if p.isSuspendedRelease(sessionID) {
				return fmt.Errorf("agent: session %q is busy: pending release after a run just finished (retry once the session settles)", sessionID)
			}
			if err := rt.Archive(st.MarkArchived); err != nil {
				if errors.Is(err, runtimeagent.ErrRunActive) {
					return fmt.Errorf("agent: session %q is busy: run in flight (archive after the run reaches terminal state): %w", sessionID, err)
				}
				return err
			}
			return nil
		}
		return st.MarkArchived()
	})
	// ⑤ 不在册探测 store 用后即释（fence 拒绝路径同样释放——探测句柄不残留）。
	if rt == nil {
		if cerr := p.repo.CloseSession(sessionID); cerr != nil {
			slog.Warn("agent: archived session store release failed", "session", sessionID, "err", cerr)
		}
	}
	if err != nil {
		return err
	}
	slog.Info("agent: session archived", "session", sessionID, "client", clientID)
	return nil
}
