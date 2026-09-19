package service

import (
	"fmt"
	"strconv"

	"github.com/tinguo/goworker/ai-core/core"
	runtimeagent "github.com/tinguo/goworker/ai-runtime/agent"
	"github.com/tinguo/goworker/daemon/internal/core/model"
)

// 本文件实现 /rewind 命令：回溯到历史检查点，恢复当时的完整对话视图。

// registerRewindCommand 注册 /rewind 命令到 hub。
// 由插件 Init 里调用 p.registerRewindCommand(h)。
func (p *AgentPlugin) registerRewindCommand(h *model.Hub) error {
	return h.RegisterCommand(model.Command{
		Name:        "/rewind",
		Description: "回溯到历史检查点，恢复当时的完整对话视图。无参列出检查点，/rewind <n> 回溯。",
		Handler:     p.handleRewind,
	})
}

// handleRewind 实现 /rewind 命令：
//
//	无参             → 列出最近 10 个 checkpoint（编号+时间+preview）
//	/rewind <n>      → 回溯到编号为 n 的 checkpoint
func (p *AgentPlugin) handleRewind(ctx *model.Context) error {
	if p.store == nil {
		ctx.Writer("会话持久化未启用，/rewind 不可用（检查 config.yaml 的 session.enabled）\n")
		return nil
	}

	args := ctx.Args
	if len(args) == 0 {
		return p.listRewindPoints(ctx)
	}
	return p.doRewind(ctx, args[0])
}

// listRewindPoints 列出最近 10 个检查点。
func (p *AgentPlugin) listRewindPoints(ctx *model.Context) error {
	cks := p.store.Checkpoints(10)
	if len(cks) == 0 {
		ctx.Writer("(无检查点 — 先跑几轮 /agent，每次提交会自动打 checkpoint)\n")
		return nil
	}

	ctx.Writer("最近检查点:\n")
	// Checkpoints 返回倒序（最新在前），#1 = 最新，向下递增到最旧
	for i, ck := range cks {
		preview := runtimeagent.Truncate(ck.Preview, 60)
		ctx.Writer(fmt.Sprintf("  #%-2d  %s  %s\n", i+1, ck.CreatedAt.Format("15:04:05"), preview))
	}
	return nil
}

// doRewind 执行回溯到指定编号的检查点。
func (p *AgentPlugin) doRewind(ctx *model.Context, arg string) error {
	n, err := strconv.Atoi(arg)
	if err != nil {
		ctx.Writer("用法: /rewind <n>  （n 是列表中的编号，用 /rewind 查看）\n")
		return nil
	}

	cks := p.store.Checkpoints(10)
	if n < 1 || n > len(cks) {
		ctx.Writer(fmt.Sprintf("编号 %d 越界（当前共 %d 个检查点，用 /rewind 查看列表）\n", n, len(cks)))
		return nil
	}

	// 列表 #1 = 最新，对应 cks 第一个元素（Checkpoints 返回倒序）
	ck := cks[n-1]

	// 已在目标点（比较 conversation 末尾消息 id 是否与 checkpoint.at 一致）
	conv := p.session.Conversation()
	if len(conv) > 0 && conv[len(conv)-1].MsgID == ck.At {
		ctx.Writer(fmt.Sprintf("已在检查点 #%d（%s）\n", n, ck.CreatedAt.Format("15:04:05")))
		return nil
	}

	// 执行回溯：移动 head → 刷新 session
	if err := p.store.SetHead(ck.At); err != nil {
		ctx.Writer(fmt.Sprintf("✘ 回溯失败: %v\n", err))
		return nil
	}

	if err := p.session.ReloadFromStore(); err != nil {
		ctx.Writer(fmt.Sprintf("✘ 刷新会话失败: %v\n", err))
		return nil
	}

	preview := runtimeagent.Truncate(ck.Preview, 80)
	msg := fmt.Sprintf("✔ 已回溯到 %s: %s\n", ck.CreatedAt.Format("15:04:05"), preview)

	// 检测是否恢复到 compact 折叠前的原文（详情分支）
	newConv := p.session.Conversation()
	hasCompactNode := false
	for _, m := range newConv {
		if isCompactMsg(m) {
			hasCompactNode = true
			break
		}
	}
	if hasCompactNode {
		msg += "  (已恢复 compact 折叠前的原文详情分支)\n"
	}

	ctx.Writer(msg + "\n")
	return nil
}

// isCompactMsg 判断消息是否为 compact 摘要节点（store compact 节点转成的 system 消息）。
func isCompactMsg(m core.Message) bool {
	// compact 摘要：Role=system + MsgID 以 "s_" 开头（store.Compact 生成的节点 id 格式）
	return m.Role == "system" && len(m.MsgID) >= 2 && m.MsgID[:2] == "s_"
}
