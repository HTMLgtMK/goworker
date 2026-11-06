package acp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	aiacp "github.com/tinguo/goworker/ai-acp"
	"github.com/tinguo/goworker/ai-acp/protocol"
	"github.com/tinguo/goworker/ai-core/core"
	"github.com/tinguo/goworker/daemon/internal/agentcontrol"
	"github.com/tinguo/goworker/daemon/internal/core/acpworker"
	"github.com/tinguo/goworker/daemon/internal/core/model"
)

// Evaluator 执行一条用户输入（装配层通常注入 Engine.Eval）。
type Evaluator = func(ctx *model.Context, input string) error

// FrontendHandler 是 daemon 主会话 ACP 入口的默认 handler：每连接一份，实现
// session/new、session/prompt、session/list、session/load 与 modes 协商的全部
// daemon 语义：
//
//   - CreateSession（Plan A2b1「session/new 真新建」）：经 BoundSessionSource
//     新建持久化会话（repo.Create 创建即声明 workspace + 新 runtime 注册）并
//     下发命令补全，把连接级 SessionBinding 指向新会话（store 稳定 id）——
//     **current 不动**（plan A2「current 收缩为 CLI/stdin 便利指针」），发起方
//     此前绑定的会话由登记面的改绑评估走引用计数（A2a 机制）；
//   - Prompt（A2b2 起退出全局门禁）：known/read-only 守卫 → 连接级 superseded
//     守卫（PR2：只认本连接自己的声明，其他连接的 session/new|load 不再顶掉本
//     连接）→ 连接级 Binding 随 ctx 注入执行链（prompt 路由到 binding 指向的
//     会话，总方案 §六）→ 命令输出围栏 + token→update 映射、HITL 授权经
//     Reporter.RequestPermission（model.DecideViaACP）。执行互斥下沉 per-runtime
//     gate（插件 withSession 的 AcquireRun）：同 session prompt 串行、跨 session
//     并行——TOCTOU 论证见 Prompt 函数头；
//   - LoadSession（Plan A2b1 重写）：绑定/已注册会话 reload+重放（进可 prompt
//     集合，Binding 改绑登记——共享会话多连接同绑），持久化不在册会话重开
//     （repo.Open 拿活实例 → 新 runtime → cwd 恢复 → 重放），归档终态只读重放
//     （进只读集合，prompt 诚实报错，Binding 不变），未知会话报错；
//   - ListSessions/SessionModes：domain → wire 映射转发。
//
// 它放在本包而非装配层：session 语义是 daemon domain 逻辑，所有壳层共用；
// 装配层只接线。gate 由装配层创建、全 daemon 一份（见 coordinator.go）——
// A2b2 起它只守会话管理操作（session/new、session/load 可 prompt 分支；prompt
// 已退出），短临界区互斥，见 coordinator.go 的职责声明。
//
// PR2 语义变更声明（总方案 §六，重构方案明确要求的可见变化）：连接级
// SessionBinding 取代全局 CurrentSession 成为标准 ACP 方法的会话选择事实源
// （§二不变量 6：会话选择属于调用入口，不存在 Daemon 全局 CurrentSession 事实
// 源）。任何连接的 session/new|load 不再影响另一连接；被 Binding 放弃的会话
// 不删除、其后台 Run 不取消、不自动获得其 control lease（§6.2 四条纪律，
// agentcontrol.SessionBinding 注释）。x-goworker/* 显式携带 sessionId 的多会话
// 路由（CR4）不变；无 Binding 的调用方（CLI/stdio/测试）回落 current 语义。
//
// Plan A2b1 语义变更声明（多会话化的最后一公里）：后端实现
// model.BoundSessionSource（生产 = AgentPlugin）时，session/new 不再走
// SetSession 的「落 current」单活动语义而是真新建（定案需求 #2 的 ACP 面），
// session/load 的判定源从全局 CurrentSessionID 换成「连接绑定 + manager 注册
// 表 + repo 持久化事实」——多会话下「当前」是连接相对的。无此能力的后端
// （简化壳/测试桩）落既有单活动降级路径（下方 LoadSession 尾段），PR2 行为
// 不变（SetSession 声明已从两处路径整体移除，cwd 声明归 BoundSessionSource）。
type FrontendHandler struct {
	evaluate Evaluator
	commands func() []model.Command
	sessions model.SessionSource
	// gate 是全局会话管理门禁(A2b2 起职责收窄,coordinator.go):只守会话管理
	// 操作(session/new、session/load 可 prompt 分支;A3 的 session/archive 落地
	// 后加入)。prompt 已退出门禁——执行互斥下沉 per-runtime gate(插件 withSession
	// 的 AcquireRun),在飞 prompt 不再阻塞管理操作,反之亦然。
	gate *SessionGate

	// binding 是本连接的会话选择器(PR2,总方案 §6.1,agentcontrol.SessionBinding):
	// session/new|load-current 声明成功后 Set(声明落地的已注册 runtime id,即
	// store 稳定 SessionID;纯内存降级无稳定 id,保持空 = 回落 current 语义)。
	// Prompt 经 agentcontrol.WithBinding 把它挂上执行链 ctx,插件的 binding 形态
	// 解析点据此路由——这是「标准 ACP 会话解析优先读 Binding」的唯一通道。它只是
	// 选择器(§6.2):Set/清除不删除会话、不取消后台 Run、不动 control lease、
	// 不影响其他连接。零值可用,连接私有,无需额外加锁(类型自带 RWMutex)。
	//
	// Plan A2 生命周期半边:每次 Set 处成对调用 binder.BindConnectionSession 把
	// 本连接的绑定登记进 agentcontrol.BindingRegistry(生命周期引用计数的唯一
	// 事实源;改绑即替换、/new 收口清除,断连 ReleaseConnection 清账)——登记
	// 只服务引用计数,路由/解析仍只读本选择器(边界收窄论证见 agentcontrol
	// bindings.go / sessions.go 文件头)。
	binding agentcontrol.SessionBinding

	// binder 是连接级会话绑定登记面(Plan A2,工厂内对 sessions type-assert
	// model.ConnectionBinder 获得,RunControlSource 同款先例)。nil = 后端无
	// 生命周期登记能力(简化壳/测试桩):Set 照常、登记跳过,行为与 PR2 一致。
	binder model.ConnectionBinder

	// multi 是 session/new|load 的多会话声明面(Plan A2b1,工厂内对 sessions
	// type-assert model.BoundSessionSource 获得,runs/binder 同款先例)。nil =
	// 后端无多会话能力(简化壳/测试桩):CreateSession 不新建(Binding 保持空 =
	// 回落 current 语义)、LoadSession 落既有单活动降级路径——「session/new
	// 声明 transport 会话、load 判定 current」的 PR2 语义。多会话语义、可选
	// 接口形态与降级论证见 model.BoundSessionSource 注释。
	multi model.BoundSessionSource

	// connectionID 是本连接的稳定句柄(P5-7,工厂内 crypto/rand 生成):x-goworker
	// control lease 的持有者寻址面,连接退出时经 Close 归还(P5-8/P5-9 的泵清理
	// 与能力解绑同钩子)。仅在本连接内自洽,跨连接无意义。
	connectionID string
	// runs 是 x-goworker 扩展协议的消费面(P5-6 的 model.RunControlSource,工厂内
	// 对 sessions type-assert 获得)。nil = 后端无 run control 能力(纯
	// SessionSource 插件/测试桩):全部 x-goworker 方法诚实报错,与 loader/lister
	// 降级同型。
	runs model.RunControlSource
	// events 是 x-goworker 订阅泵的下行通知面(P5-8):registerXGWMethods 注入本
	// 连接的 *aiacp.Server(内嵌 *Peer,Notify 导出;注册先于 server.Start,无
	// 并发面,happens-before 经 Start 的 goroutine 创建成立)。nil = 未注册
	// (不经 wire 的直调形态):attach 诚实报错,泵无从建立。
	events xgwEventSink

	// providers 是 x-goworker/providers 的数据面(model.ProviderCatalogSource,
	// 工厂内对 sessions type-assert 获得,runs/binder/multi 同款先例)。nil =
	// 后端无目录能力(简化壳/测试桩):方法诚实报错,客户端按 -32601 降级平铺
	// modes 的既有约定不变。
	providers model.ProviderCatalogSource

	// providerManage 是 x-goworker/provider/detail|upsert|remove 的数据面
	// (model.ProviderManageSource,工厂内 type-assert,providers 同款先例)。
	// nil = 后端无管理能力:方法诚实报错,客户端按 -32601 降级。
	providerManage model.ProviderManageSource

	// sessionDelete 是 x-goworker/session/delete 的可选物理删除面。与 multi
	// 分开，保持旧 BoundSessionSource 测试桩/简化后端可编译。
	sessionDelete model.SessionDeleteSource

	// subs 是本连接的会话订阅表(P5-8):sessionID → 订阅回收句柄,每连接每会话
	// 至多一订阅(重复 attach 先 cancel 旧泵再建新);泵退出自清按句柄指针身份
	// 判重,换代/断连(Close)统一回收。与 promptable/readOnly 共用 h.mu。
	//
	// 锁序(I2 生命周期屏障后;DE2-I1 后 source 侧是按会话 fencing guard):
	// h.mu 临界区内会嵌套调用 source 的 session guard——attach 提交段的
	// AcquireControl 与 detach 的 ReleaseControl 都与订阅表更新放在同一临界区
	// (订阅表更新与 control lease 操作的事务语义,评审 I2 建议四)。guard.mu
	// 是 source 侧叶锁(runcontrol.go,每会话一把),不回调 handler、不取
	// h.mu,嵌套单向 h.mu→session guard,Close 侧同向,无环;其余临界区仍互
	// 不嵌套。
	//
	// C1 修复清退了旧的 controlSessions 候选集:approval/respond 的会话归属
	// 守卫在 source 侧按服务端记录校验(第三轮评审 I4 起 run/start|cancel 的
	// lease 守卫同样内移 source,与操作提交同临界区原子完成),handler 无需
	// 本地簿记。
	subs map[string]*xgwSubscription

	// closed 是 I2 的连接生命周期屏障:Close 在 h.mu 内置位,此后 attach 的提交
	// 段一律拒绝——Close 返回之后不会再发生新的订阅提交或 control lease 获取
	// (不复活订阅与 lease;评审 I2 的 Close/Attach 竞态:attach 的提交段与
	// Close 的「closed 置位 + 清表」同以 h.mu 串行,二者必有一个全序)。
	closed bool

	// preCommitHook 是 attach 的测试钩子(第三轮评审 I2 验收,仅测试注入,
	// 生产恒 nil):在 hub.Subscribe 成功返回之后、进入提交临界区(h.mu)之前
	// 同步调用。该间隙是生产真实存在的锁外边界——Close/detach 可在此并发发生,
	// 钩子内跑 Close/detach(它们需要 h.mu,此时本 goroutine 未持锁,不会自锁)
	// 复现的正是生产可达的交错;提交临界区本身保持纯原子(无中途解锁)。钩子
	// 须在无并发 attach 运行时设置/清除(测试串行段使用),避免钩子字段自身的
	// 读写竞争。
	preCommitHook func()

	mu         sync.Mutex
	promptable map[string]struct{} // 可 prompt 集合：session/new 与当前会话 load 记入
	readOnly   map[string]struct{} // 只读集合：归档 load 记入，prompt 诚实报错
	// active 是本连接最近一次有效声明的会话 id(PR2 前是 daemon 全局单值——plugin
	// 的 activeTransportSession;PR2 起声明归属连接,守卫只认本连接自己的声明,
	// 其他连接的 session/new|load 不再顶掉本连接)。A2b2 起守卫无回落:原「空则
	// 回落全局 ActiveSessionID」分支随全局门禁一并删除(见 Prompt 内注释)——
	// 空 = 本连接尚未声明,任何 id 的 prompt 都先过 membership 守卫(本连接
	// remember 过才在 promptable 集合),未声明即 unknown session 诚实拒绝。
	// 与 promptable/readOnly 共用 h.mu。注意:A2b1 起声明落地是
	// BoundSessionSource(boundTo = store 稳定 id,记入 Binding),transport 会话
	// id(session/new 的服务端签发 id)只进本字段与 promptable——load 分支
	// remember 的是 store id,连接的活动身份随 load 换轨到 store id(与 PR2 的
	// load-current 行为一致)。
	active string
}

var (
	_ aiacp.SessionHandler       = (*FrontendHandler)(nil)
	_ aiacp.SessionCreator       = (*FrontendHandler)(nil)
	_ aiacp.SessionLoader        = (*FrontendHandler)(nil)
	_ aiacp.SessionLister        = (*FrontendHandler)(nil)
	_ aiacp.SessionModesProvider = (*FrontendHandler)(nil)
)

// NewFrontendHandlerFactory 返回每连接创建一个 FrontendHandler 的工厂。sessions
// 可为 nil（会话能力不可用时降级：list/load/modes 诚实报错或不支持，prompt 仍可路由）。
// commands 可为 nil（不下发命令补全）。evaluate 不可为 nil（Prompt 直接调用），
// 装配层须保证（如注入 Engine.Eval）。gate 不可为 nil：单活动会话模型的执行门禁，
// 由装配层创建一次、被本工厂闭包捕获，跨所有连接共享——每连接一份的话 TOCTOU
// 守卫等于没装。
//
// P5-7：工厂内生成 connectionID（crypto/rand hex），并对 sessions type-assert
// model.RunControlSource 得到 x-goworker 消费面（断言失败 → runs=nil，方法降级
// 为诚实报错）。type-assert 只对非 nil 接口进行：nil SessionSource 直接跳过。
func NewFrontendHandlerFactory(evaluate Evaluator, commands func() []model.Command, sessions model.SessionSource, gate *SessionGate) HandlerFactory {
	return func() aiacp.SessionHandler {
		handler := &FrontendHandler{
			evaluate:     evaluate,
			commands:     commands,
			sessions:     sessions,
			gate:         gate,
			connectionID: newConnectionID(),
		}
		if sessions != nil {
			// R3(第四轮评审):断言面是 model.RunControlSource——刻意不含
			// StartRunSync/CancelRunSync(那是 agentcontrol.InternalRunner 受限
			// 内部面,仅宿主组装面注入可信本地命令 Adapter,见 capability.go 方法
			// 集纪律)。h.runs 的静态类型拿不到 InternalRunner ⇒ ACP/x-goworker
			// 面结构性无法绕过 fencing:run/start|cancel 一律走 StartRun/CancelRun
			// 的 Connection Lease 临界区,同步宿主入口对本协议面不可达。
			if runs, ok := sessions.(model.RunControlSource); ok {
				handler.runs = runs
			}
			// Plan A2 生命周期半边:连接级会话绑定登记面(可选能力,缺省 nil 时
			// CreateSession/LoadSession 只做 PR2 的 Binding Set、不登记)。
			if binder, ok := sessions.(model.ConnectionBinder); ok {
				handler.binder = binder
			}
			// Plan A2b1 会话面多会话化:session/new 真新建与 session/load 的
			// reload/重开/归档只读判定面(可选能力,缺省 nil 时落 PR2 单活动
			// 降级路径)。
			if multi, ok := sessions.(model.BoundSessionSource); ok {
				handler.multi = multi
			}
			// provider 模型目录(x-goworker/providers 的数据面,可选能力,缺省
			// nil 时方法诚实报错)。
			if providers, ok := sessions.(model.ProviderCatalogSource); ok {
				handler.providers = providers
			}
			// provider 配置管理(x-goworker/provider/* 的数据面,可选能力,缺省
			// nil 时方法诚实报错)。
			if manage, ok := sessions.(model.ProviderManageSource); ok {
				handler.providerManage = manage
			}
			if deleter, ok := sessions.(model.SessionDeleteSource); ok {
				handler.sessionDelete = deleter
			}
		}
		return handler
	}
}

// remember 记入可 prompt 集合并登记为本连接的活动声明；rememberReadOnly 记入
// 只读集合。归档会话只读：load 成功记入只读集合，prompt 该 id 诚实报错而非掩盖
// 成未知会话。PR2:活动声明归连接(h.active)——superseded 守卫据此只拦本连接
// 自己的过期声明。
func (h *FrontendHandler) remember(sessionID string) {
	h.mu.Lock()
	if h.promptable == nil {
		h.promptable = map[string]struct{}{}
	}
	h.promptable[sessionID] = struct{}{}
	// 同一会话只能归属一个集合：若先以归档身份 load 进只读集合、后又被恢复为
	// 当前会话再 load，不清掉旧标记会让 Prompt 永远误判 read-only（评审 MEDIUM-2）。
	delete(h.readOnly, sessionID)
	h.active = sessionID
	h.mu.Unlock()
}

func (h *FrontendHandler) rememberReadOnly(sessionID string) {
	h.mu.Lock()
	if h.readOnly == nil {
		h.readOnly = map[string]struct{}{}
	}
	h.readOnly[sessionID] = struct{}{}
	h.mu.Unlock()
}

// membership 返回 sessionID 的集合归属（可 prompt / 只读）。
func (h *FrontendHandler) membership(sessionID string) (promptable, readOnly bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	_, promptable = h.promptable[sessionID]
	_, readOnly = h.readOnly[sessionID]
	return promptable, readOnly
}

// sessionBusyError 是管理操作（session/new、session/load 可 prompt 分支）在
// 全局管理门禁被另一管理操作占住时收到的诚实拒绝（A2b2 起 busy 只发生在管理
// 操作之间——prompt 已退出全局门禁，执行互斥下沉 per-runtime gate，在飞 prompt
// 不再阻塞 new/load；plan A2「busy 语义改善」）。busy 是瞬态错误：另一管理
// 操作的临界区只含注册表/repo 的状态突变（无 LLM 调用、无连接 I/O），稍后
// 重试即可。
func sessionBusyError() *protocol.RPCError {
	return &protocol.RPCError{
		Code:    -32000,
		Message: "acp frontend: session busy: a prompt is in flight on the active session; cancel it or retry",
	}
}

// CreateSession 实现 aiacp.SessionCreator（Plan A2b1 重写——「session/new 真
// 新建」，定案需求 #2 的 ACP 面）：门禁内经 BoundSessionSource 新建持久化会话
// （repo.Create 创建即声明 workspace + 新 runtime 注册进 manager），响应返回前
// 把命令清单作为 available_commands_update 下发（/ 补全）。声明成功后：
//   - 连接级 Binding 指向新会话（store 稳定 SessionID）；
//   - 生命周期登记（ConnectionBinder）随之改绑——发起方此前绑定的会话在此失去
//     引用，由实现侧评估收口（A2a 引用计数机制）；
//   - **current 不动**：新会话只是本连接的路由目标（plan A2「current 收缩为
//     CLI/stdin 便利指针」）。旧实现在此调 SetSession（把 cwd 落到 current 的
//     单活动语义），A2b1 移除——CLI/stdin 完全不走此路径，零影响。
func (h *FrontendHandler) CreateSession(_ context.Context, sessionID string, req protocol.NewSessionRequest, reporter aiacp.Reporter) error {
	// 真新建突变 manager 注册表与 repo（管理面共享状态），prompt 在飞时诚实报
	// busy 而非排队：客户端取消在飞 prompt 或稍后重试即可。SessionGate 本步
	// 不动（A2b1 保持管理操作互斥；A2b2 收缩后本使用点语义不变）。
	if !h.gate.tryAcquire() {
		return sessionBusyError()
	}
	boundTo := ""
	if h.multi != nil {
		id, err := h.multi.CreateBoundSession(req.Cwd)
		if err != nil {
			h.gate.release()
			return &protocol.RPCError{
				Code:    -32000,
				Message: fmt.Sprintf("acp frontend: create session: %v", err),
			}
		}
		boundTo = id
	}
	// 临界区只包共享状态突变，手动释放（不能用 defer：release 双还会偷走下一个
	// 持有者的 token）。Update 是连接 I/O，客户端停摆时管道写会无限阻塞——
	// 门禁绝不能被一个卡死的客户端攥住，否则全 daemon 的 prompt 都堵在这。
	h.gate.release()

	// 声明成功才记入活动身份与可 prompt 集合（A2b1：创建可失败——sandbox 拒绝
	// 等；失败路径绝不留下「promptable 里有、实际无可路由会话」的僵尸声明，
	// 否则对该 transport id 的 prompt 会通过 known 守卫、以空 Binding 静默回落
	// current，串到别的会话——比报 unknown session 更糟）。remember 只动连接
	// 私有状态（h.mu），门禁外执行与 Binding.Set 同位。无多会话能力的后端没有
	// 失败分支，行为与 PR2 一致（照常 remember、Binding 保持空）。
	h.remember(sessionID)
	// Binding 是连接私有状态，门禁外 Set 即可（§6.2:不影响任何共享会话状态,
	// 也不影响其他连接）。无多会话能力的后端 boundTo 为空 = Binding 保持空 =
	// 回落 current 语义（PR2 降级形态，与纯内存降级同形）。
	h.binding.Set(boundTo)
	// Plan A2 生命周期半边:Set 处成对登记(clientID → boundTo,改绑即替换——
	// 本连接此前绑定的会话在此失去引用,由实现侧评估收口)。boundTo 为空(降级
	// 后端,无稳定 id)不登记,与 Binding 保持空一致。
	if h.binder != nil && boundTo != "" {
		h.binder.BindConnectionSession(h.connectionID, boundTo)
	}

	if h.commands != nil {
		if body := availableCommandsUpdateBody(h.commands()); body.Raw != nil {
			reporter.Update(sessionID, body)
		}
	}
	return nil
}

// deviceToolSource 按 initialize 协商的 clientCapabilities 门控：
// 对端声明 x-device 且 reporter 提供反向 Call 通道时，返回惰性工具源；
// 否则 nil（无设备工具，零往返）。
//
// P5-9：后端实现 model.ClientToolBinder（工厂 type-assert h.sessions）时优先走
// 能力代理面——工具绑定 CapabilityRegistry 到本连接，断连后设备工具调用等待
// 重绑（默认 45s）而非立即报错给模型（P5 显式可见变化 #1）；type-assert 失败
// （简化壳/测试桩无 binder）回落原始 relay 路径，行为不变。能力门控（caps.Has）
// 在两种形态之前：未声明 x-device 的客户端（VS Code 等）零变化。DeviceCaller
// 是结构类型（model 不 import acpworker），acpworker.DeviceCaller 天然满足。
func (h *FrontendHandler) deviceToolSource(reporter aiacp.Reporter) func(ctx context.Context) []core.Tool {
	capable, ok := reporter.(interface {
		ClientCapabilities() protocol.ClientCapabilities
	})
	if !ok {
		return nil
	}
	caller, ok := reporter.(acpworker.DeviceCaller)
	if !ok {
		return nil
	}
	if !capable.ClientCapabilities().Has(acpworker.CapabilityDeviceTools) {
		return nil
	}
	// type-assert 对 nil 接口安全（zero, false）：sessions 为 nil 直接落回落路径。
	if binder, ok := h.sessions.(model.ClientToolBinder); ok {
		return binder.ClientToolSource(h.connectionID, caller)
	}
	return func(ctx context.Context) []core.Tool {
		pctx, cancel := context.WithTimeout(ctx, acpworker.ProbeTimeout)
		defer cancel()
		return acpworker.RelayDeviceTools(caller, acpworker.ProbeDeviceTools(pctx, caller))
	}
}

// Prompt 实现 aiacp.SessionHandler（A2b2 起 prompt 退出全局会话门禁——
// plan A2「SessionGate 保留、职责收缩」）。流程：归档只读守卫 → 未知会话守卫
// → superseded 守卫（连接级 h.active：只认本连接自己的声明，本连接重声明后的
// 旧 id 诚实报错）→ 连接级 Binding 随 ctx 注入执行链 → 命令输出围栏 + token
// 映射 + HITL 授权 → evaluate → 闭合围栏。
//
// 为什么不再需要全局门禁（plan A2「TOCTOU 封闭论证」的展开，取代旧版「守卫与
// evaluate 都在门禁内」的论证）：
//   - 旧病灶（coordinator.go 文件头，评审 P1）：单活动模型下 session/new 经
//     SetSession 把新 cwd 应用到 current 正在执行的会话上——cwd/会话本体是
//     daemon 全局共享状态，prompt 的守卫检查与 evaluate 之间存在 TOCTOU 空档，
//     必须用 gate 把 prompt 与 session/new|load 全序化。
//   - 病灶随单活动模型一并消失（A2b1）：session/new 真新建（CreateBoundSession
//     只 Create 新 store + 注册新 runtime，**不动 current、不触碰任何在飞会话
//     的状态**）；cwd 是会话属性（runtime 级声明，boundsession.go），声明只落
//     新建/重开的目标会话。管理操作在全局临界区内只做状态突变（Create/注册/
//     摘发起方 binding/引用减一），从不改写在飞 prompt 正在执行的实例。
//   - 执行互斥下沉 per-runtime gate：prompt 经 binding → sessionRuntimeIn 解析
//     出 runtime 实例快照，执行全程持有该 runtime 的 gate（withSession →
//     AcquireRun，容量 1）——同一 session 的 prompt 串行、跨 session 并行。
//     全局 gate 临界区从不等待 runtime gate（两域无嵌套，plan A2 锁序），管理
//     操作与在飞 run 互不阻塞。
//   - 守卫的自身一致性：membership/h.active 是连接私有状态（h.mu），其他连接
//     的声明只写它们自己的连接私有状态，与本连接的检查无共享数据；本会话不会
//     在守卫通过后凭空消失——收口（tryReleaseSession）以引用归零为前提，本
//     连接仍绑着的目标会话引用非零、绝不收口。守卫通过到解析点之间的已知窗口：
//     同一连接并发重声明（ai-acp 每请求独立 goroutine，协议允许）可在守卫后、
//     解析前落地，让这轮在飞 prompt 的解析读到改绑后的 Binding——后果是该轮
//     输入路由到客户端刚声明的新会话（跨连接无此窗口：Binding 连接私有），
//     旧版以「new 永远 busy 拒绝」为代价封闭它，plan A2 明文放弃该代价（busy
//     语义改善）；解析一经完成，执行全程持 runtime gate、闭包内读注入值，
//     不再受后续改绑影响。
//   - ctx 取消（连接断开）：不再经门禁排队面，而是随请求 ctx 传入 evaluate
//     链——SDK 的 AgentRun 取消语义（ai-core 工具链对 ctx 的响应）承担「断连
//     不悬挂」，与 xgw 后台 run 的 ctx 取消同源。
func (h *FrontendHandler) Prompt(ctx context.Context, sessionID string, prompt []protocol.ContentBlock, reporter aiacp.Reporter) (string, error) {
	promptable, readOnly := h.membership(sessionID)
	if readOnly {
		// 归档会话续写防护：诚实报错，优于报 unknown session 掩盖真相
		return "", &protocol.RPCError{
			Code:    -32000,
			Message: fmt.Sprintf("acp frontend: archived session %q is read-only", sessionID),
		}
	}
	if !promptable {
		return "", &protocol.RPCError{
			Code:    -32000,
			Message: fmt.Sprintf("acp frontend: unknown session %q", sessionID),
		}
	}

	// superseded 守卫(PR2 连接级,plan A2 起是唯一形态):本连接最近声明
	// (h.active)——其他连接的 session/new|load 不再顶掉本连接(§六语义变更,
	// 声明归属连接);本连接重声明后的旧 id 仍诚实报错。旧版的「空则回落全局
	// ActiveSessionID」分支随全局门禁一并删除(A2b2):回落消费的是跨连接单值
	// (plugin.activeTransportSession),多 session 下会被别的连接的声明误拒
	// (plan A2「superseded 守卫删除空则回落全局 ActiveSessionID 分支」);
	// h.active 为空 = 本连接从未声明(不走 CreateSession 的测试桩/简化壳),
	// 直接放行——路由由 Binding(空 → current 回落)决定,守卫不参与。
	h.mu.Lock()
	local := h.active
	h.mu.Unlock()
	if local != "" && local != sessionID {
		return "", &protocol.RPCError{
			Code:    -32000,
			Message: fmt.Sprintf("acp frontend: session %q is no longer active on this connection (last declared %q); create a new session or load the session again", sessionID, local),
		}
	}

	var input string
	for _, block := range prompt {
		if block.Type != "text" {
			continue
		}
		if input != "" {
			input += "\n"
		}
		input += block.Text
	}

	output := &commandOutput{reporter: reporter, sessionID: sessionID}
	// session_info_update 的顶层 sessionId 仍按 ACP 使用 transport id；Raw 扩展字段
	// 额外带 store logical id，扩展侧用它精确 patch session/list（bridge 的 typed
	// SessionInfoUpdate 不声明该字段，但 JSON parser 会保留时优先消费）。
	infoSessionID := h.binding.Get()
	if infoSessionID == "" {
		infoSessionID = sessionID
	}
	commandContext := &model.Context{
		// PR2:连接级 Binding 挂上执行链 ctx——Engine.Eval 把 model.Context 直通
		// 命令 handler,插件的 binding 形态解析点(agentcontrol.BindingFromCtx)据此
		// 把 prompt 路由到本连接选择的会话(总方案 §六;Binding 悬空时插件诚实
		// 报错)。注入的是连接 binding 对象本身(非快照值):/new 收口经同一对象
		// 的 b.Set("") 清空本连接选择、回落 current——快照注入会切断该收口
		// (函数头 TOCTOU 论证的已知窗口由此而来)。
		// Plan A2:连接句柄随同一执行链 ctx 注入(agentcontrol.WithClientID)——
		// /new 收口(agentcontrol.ClientIDFromCtx)据此摘本连接的绑定登记;与
		// ReleaseConnection 的 clientID 同源(h.connectionID)。
		Ctx: agentcontrol.WithClientID(agentcontrol.WithBinding(ctx, &h.binding), h.connectionID),
		FrontendContext: model.FrontendContext{
			// 命令输出统一包成代码块（见 commandOutput）：命令侧仍按等宽终端排版，
			// 由这里告诉渲染器「这段别重排」。
			Writer: output.Write,
			EmitToken: func(token core.Token) {
				reporter.Update(sessionID, TokenToUpdate(token))
			},
			// Decide 接上 ACP 授权请求：沙箱中间件的 HITL 决策经
			// Reporter.RequestPermission 到客户端，nil 的静默拒绝不再吞掉危险命令提示。
			Decide: model.DecideViaACP(ctx, reporter, sessionID),
			// 首轮标题生成完成后的带外通知。用 Raw 透传 session_info_update
			//（附带 sessionId，扩展窄化需要它；SDK hook 会以当前 bridge id 兜底）。
			SessionInfo: func(title string) {
				payload, err := json.Marshal(map[string]any{
					"sessionUpdate": "session_info_update",
					"sessionId":     infoSessionID,
					"title":         title,
					"updatedAt":     time.Now().UTC().Format(time.RFC3339),
				})
				if err != nil {
					return
				}
				reporter.Update(sessionID, protocol.SessionUpdateBody{Raw: payload})
			},
			// Device tool source is gated on the initialize-time clientCapabilities
			// of this connection. Assigning nil directly is safe: Session.Run and
			// the plugin both treat a nil ToolSource as "no device tools".
			// P5-9 起优先 binder 能力代理面（断连等待重绑），装配点不变。
			ToolSource: h.deviceToolSource(reporter),
		},
		Values: make(map[string]any),
	}
	// evaluate 内部的报错也经 Writer 写出（命令自己格式化 ✘ 前缀），因此先闭合
	// 代码块再返回错误，否则已经流出去的内容会留一个未闭合的围栏。
	evalErr := h.evaluate(commandContext, input)
	output.Close()
	if evalErr != nil {
		return "", evalErr
	}
	return protocol.StopEndTurn, nil
}

// LoadSession 实现 aiacp.SessionLoader（Plan A2b1 重写——「多会话下的 load」）。
// 多会话语义（BoundSessionSource 可用）四分支，判定源从全局 CurrentSessionID
// 换成「连接绑定 + manager 注册表 + repo 持久化事实」——多会话下「当前」是
// 连接相对的（SessionBinding），插件面只回答「这个 id 的会话在哪」：
//   - 本连接已绑会话（binding.Get）/任意已注册 runtime（含别连接绑着的）→
//     reload 视图 + 重放 + 改绑登记（共享会话：多连接同绑一会话，双向都只
//     reload 不重建；binding 已指向目标时改绑幂等、登记面的 prev 评估零动作）；
//   - 持久化但不在册（如 /new 收口、断连收口后的会话）→ 重开：repo.Open 拿
//     活实例 → 新 runtime 注册 → cwd 恢复（实参非空优先，否则 workspace 记录）
//     → 改绑登记 → 历史重放；
//   - archived 终态（SessionArchived 经 repo 层探测）→ 只读重放：不进门禁
//     （纯只读快照，与在飞 prompt 无共享可变状态，既有归档分支同款）、Binding
//     不动（只读视图不是会话选择）、不注册 runtime；
//   - 未知会话 → -32000 unknown session（现状 wire 契约）。
//
// 前两支的机制相同（reload 已注册 runtime 的视图），binding 是否已指向目标只
// 影响 handler 侧登记的幂等性，不构成独立分支。归档探测（repo 层）与门内
// 重开之间无并发归档源（archive 命令是 A3 的活且必须走管理 gate），TOCTOU
// 窗口在 A2b1 封闭；A3 落地时归档须经管理互斥面，与本探测/load 的交错由该
// gate 封闭（boundsession.go SessionArchived 注释）。
//
// 后端无 BoundSessionSource（简化壳/测试桩）→ 落既有单活动降级路径（函数尾段）：
// 当前会话 reload+重放、归档只读重放、未知报错——PR2 行为（A2b1 起 SetSession
// 声明已从两处路径整体移除：cwd 声明归 BoundSessionSource，降级形态不建模）。
func (h *FrontendHandler) LoadSession(_ context.Context, req protocol.LoadSessionRequest, reporter aiacp.Reporter) error {
	if h.sessions == nil {
		return &protocol.RPCError{Code: -32000, Message: "acp frontend: session load not supported"}
	}
	if h.multi != nil {
		// 归档终态：只读重放。先探测（repo 层持久化事实，无门禁）——重放数据
		// 是 store 快照，解析失败保持未知会话语义之外的诚实报错。
		if h.multi.SessionArchived(req.SessionID) {
			history, err := h.sessions.ArchivedSessionHistory(req.SessionID)
			if err != nil {
				return fmt.Errorf("acp frontend: load archived session %q: %w", req.SessionID, err)
			}
			h.rememberReadOnly(req.SessionID)
			if h.commands != nil {
				if body := availableCommandsUpdateBody(h.commands()); body.Raw != nil {
					reporter.Update(req.SessionID, body)
				}
			}
			for _, body := range history {
				reporter.Update(req.SessionID, SessionUpdateFromDomain(body))
			}
			return nil
		}
		// 可 prompt 分支（reload/重开）：门禁内完成状态突变（注册表突变、共享
		// 视图 reload、repo 打开），busy 语义与 CreateSession 同款。临界区手动
		// 收窄到突变 + 历史快照：重放 Update 是连接 I/O，停摆客户端的阻塞写不
		// 能攥住门禁（同 CreateSession；release 不得双还）。
		if !h.gate.tryAcquire() {
			return sessionBusyError()
		}
		var (
			history       []model.SessionUpdate
			loadErr       error
			historyErr    error
			boundByLoader bool
		)
		if loader, ok := h.multi.(model.BoundSessionLoader); ok {
			history, loadErr = loader.LoadBoundSessionForConnection(req.SessionID, req.Cwd, h.connectionID)
			boundByLoader = loadErr == nil
		} else {
			loadErr = h.multi.LoadBoundSession(req.SessionID, req.Cwd)
			if loadErr == nil {
				history, historyErr = h.multi.SessionHistory(req.SessionID)
			}
		}
		h.gate.release()
		if loadErr != nil {
			if errors.Is(loadErr, model.ErrSessionUnknown) {
				return &protocol.RPCError{
					Code:    -32000,
					Message: fmt.Sprintf("acp frontend: unknown session %q", req.SessionID),
				}
			}
			return fmt.Errorf("acp frontend: load session %q: %w", req.SessionID, loadErr)
		}
		if historyErr != nil {
			return fmt.Errorf("acp frontend: load session %q history: %w", req.SessionID, historyErr)
		}

		h.remember(req.SessionID)
		// PR2:Binding 指向本连接刚加载的会话(连接私有状态,门禁外 Set;§6.2——
		// 重选不删除原会话、不取消其后台 Run、不动 control lease、不影响其他连接)。
		h.binding.Set(req.SessionID)
		if !boundByLoader && h.binder != nil {
			h.binder.BindConnectionSession(h.connectionID, req.SessionID)
		}
		if h.commands != nil {
			if body := availableCommandsUpdateBody(h.commands()); body.Raw != nil {
				reporter.Update(req.SessionID, body)
			}
		}
		for _, body := range history {
			reporter.Update(req.SessionID, SessionUpdateFromDomain(body))
		}
		return nil
	}

	// ── 降级路径（后端无 model.BoundSessionSource：简化壳/测试桩）──────────
	// 单活动语义（PR2）：load 的语义是「确认会话上下文已在」。
	if req.SessionID == h.sessions.CurrentSessionID() {
		// 与 prompt 互斥：load-current 同样改写共享会话状态（reload）。
		// prompt 在飞时诚实报 busy，归档分支只读、不碰共享状态，不需要门禁。
		// 临界区手动收窄到状态突变+历史快照：重放 Update 是连接 I/O，停摆客户端
		// 的阻塞写不能攥住门禁（同 CreateSession；release 不得双还）。
		if !h.gate.tryAcquire() {
			return sessionBusyError()
		}
		if err := h.sessions.ReloadCurrentSession(); err != nil {
			h.gate.release()
			return fmt.Errorf("acp frontend: reload current session: %w", err)
		}
		history := h.sessions.CurrentSessionHistory()
		h.gate.release()

		h.remember(req.SessionID)
		// PR2:Binding 指向本连接刚加载的会话(连接私有状态,门禁外 Set;§6.2——
		// 重选不删除原会话、不取消其后台 Run、不动 control lease、不影响其他连接)。
		h.binding.Set(req.SessionID)
		// Plan A2 生命周期半边:Set 处成对登记(与 CreateSession 同款;改绑即
		// 替换,session/load 重开把引用落回重开的会话)。
		if h.binder != nil {
			h.binder.BindConnectionSession(h.connectionID, req.SessionID)
		}
		if h.commands != nil {
			if body := availableCommandsUpdateBody(h.commands()); body.Raw != nil {
				reporter.Update(req.SessionID, body)
			}
		}
		for _, body := range history {
			reporter.Update(req.SessionID, SessionUpdateFromDomain(body))
		}
		return nil
	}
	// 归档会话：只读重放。先解析成功再入只读集合，解析失败保持未知会话语义。
	for _, info := range h.sessions.ListSessions() {
		if info.SessionID != req.SessionID {
			continue
		}
		history, err := h.sessions.ArchivedSessionHistory(req.SessionID)
		if err != nil {
			return fmt.Errorf("acp frontend: load archived session %q: %w", req.SessionID, err)
		}
		h.rememberReadOnly(req.SessionID)
		if h.commands != nil {
			if body := availableCommandsUpdateBody(h.commands()); body.Raw != nil {
				reporter.Update(req.SessionID, body)
			}
		}
		for _, body := range history {
			reporter.Update(req.SessionID, SessionUpdateFromDomain(body))
		}
		return nil
	}
	return &protocol.RPCError{
		Code:    -32000,
		Message: fmt.Sprintf("acp frontend: unknown session %q", req.SessionID),
	}
}

// ListSessions 实现 aiacp.SessionLister：转发给会话数据源；无数据源时返回空清单
// （非 nil，ACP 无 list 能力协商，空清单即「当前无可列会话」）。
func (h *FrontendHandler) ListSessions(_ context.Context, _ protocol.ListSessionsRequest) ([]protocol.SessionInfo, error) {
	if h.sessions == nil {
		return []protocol.SessionInfo{}, nil
	}
	summaries := h.sessions.ListSessions()
	infos := make([]protocol.SessionInfo, 0, len(summaries))
	for _, summary := range summaries {
		infos = append(infos, SessionInfoFromDomain(summary))
	}
	return infos, nil
}

// SessionModes 实现 aiacp.SessionModesProvider：转发给会话数据源（无数据源 =
// 无 modes，响应省略该字段）。
func (h *FrontendHandler) SessionModes() *protocol.SessionModeState {
	if h.sessions == nil {
		return nil
	}
	return SessionModeStateFromDomain(h.sessions.SessionModes())
}

// commandFence 是命令输出代码块的围栏。用 4 个反引号而非 3 个：命令输出会回显
// 用户内容（/history 回显对话、/rules 回显指令文件），其中出现 ``` 完全可能，
// 4 个反引号能让这些内容原样保留而不提前闭合代码块。
const commandFence = "````"

// commandOutput 把命令的预格式化输出包成一个 Markdown 代码块并流式发出。
//
// 为什么要包：命令输出是为等宽终端手工排版过的，而前端按 Markdown 渲染、比例
// 字体，不包的话对齐全塌。代码块在两端都是等宽字体，等于把「这段别重排」显式
// 告诉渲染器。规则与具体命令无关 —— 任何经 Writer 出来的输出一视同仁。
//
// 为什么能流式：CommonMark 规定未闭合的围栏代码块延伸到文档末尾，所以中途每次
// 增量渲染看到的都是一段合法的（尚未闭合的）代码块 —— 长命令的进度实时可见，
// 收尾补上闭合围栏定稿。不必先攒完再发。
//
// 并发约束：Writer/Close 只能在单 goroutine 上调用（engine 命令执行串行写）；
// EmitToken 走 reporter.Update，与 Writer 各走各的通道，不共享本结构状态。
//
// 围栏惰性开启：命令没有任何输出时不留空块。开启前只认「有实质内容」的写入，
// 开启后一律原样透传 —— 围栏内的空行是表格排版的一部分，不能吞。
type commandOutput struct {
	reporter  aiacp.Reporter
	sessionID string
	opened    bool
	trailing  byte // 已写出内容的最后一个字节，收尾时据此决定要不要补换行
}

// Write 实现 model.FrontendContext.Writer。
//
// 围栏开启前忽略纯空白：这条通道被 agent 路径共用（session.Run 每轮结束都会
// 无条件 Writer("\n")），不拦的话每次对话都会多出一个空代码块。
func (w *commandOutput) Write(text string) {
	if text == "" {
		return
	}
	if !w.opened {
		if strings.TrimSpace(text) == "" {
			return
		}
		w.reporter.MessageChunk(w.sessionID, commandFence+"\n")
		w.opened = true
	}
	w.reporter.MessageChunk(w.sessionID, text)
	w.trailing = text[len(text)-1]
}

// Close 闭合围栏；未写入过任何内容时不发，避免留下一个空代码块。
func (w *commandOutput) Close() {
	if !w.opened {
		return
	}
	if w.trailing != '\n' {
		w.reporter.MessageChunk(w.sessionID, "\n")
	}
	w.reporter.MessageChunk(w.sessionID, commandFence+"\n")
}
