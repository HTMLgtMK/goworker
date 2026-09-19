// permission.go 是 HITL 与 ACP 两个世界之间的翻译层：把沙箱中间件抛出的
// hitl.InterruptRequest 译成 session/request_permission 的线上形状，再把用户
// 选中的 optionId 译回 hitl.Decision。
//
// 只做一次决策（once）语义：选项集固定为 allow_once / reject_once，不带
// always —— 不引入任何需要持久化的"永久允许"状态，粒度定错就是安全隐患。
//
// 放在 plugin 包是因为两个 ACP 前端都要用它：agent chat（frontend/vscode 的
// ingress）与 task worker（agent 的 acpWorker）。plugin 是两者共同依赖的层
// （且已持有 hitl.Decision 的装配点 FrontendContext.Decide）；放进任一个前端
// 都会让另一个反向依赖它。
package model

import (
	"context"
	"time"

	"github.com/tinguo/goworker/ai-dispatch/protocol"
	"github.com/tinguo/goworker/ai-runtime/hitl"
)

// HITL 选项 id：协议内约定，也是 DecisionFromOption 的识别依据。
const (
	hitlOptionAllow  = "allow_once"
	hitlOptionReject = "reject_once"
)

// PermissionAsker 是发起 ACP 授权请求所需的最小能力。dispatch.Reporter 天然
// 满足它（结构类型），所以这里不必依赖 ai-dispatch 包本身。
type PermissionAsker interface {
	RequestPermission(ctx context.Context, sessionID string, req protocol.PermissionRequest) (string, error)
}

// PermissionFromInterrupt 把一次 HITL 中断译成 ACP 授权请求。
// sessionID 由 RequestPermission 填进请求（这里留空，避免两处真相）。
//
// 呈现取舍：title 用 Command 而非 Description —— 用户要判断的是"这条命令能不能
// 跑"，risk_reason 是判断依据，拼进 title 的副行（webview 按单行渲染，多行会被
// 压扁）；裸工具名不含信息量，拿它当标题等于让用户盲签。命令为空（非 bash 类
// 工具）才退到 ToolName。
func PermissionFromInterrupt(req *hitl.InterruptRequest) protocol.PermissionRequest {
	title := req.Command
	if title == "" {
		title = req.ToolName
	}
	if req.RiskReason != "" {
		title = title + "\n" + req.RiskReason
	}
	return protocol.PermissionRequest{
		ToolCall: protocol.ToolCallInfo{
			ToolCallID: req.ID,
			Title:      title,
		},
		Options: []protocol.PermissionOption{
			{OptionID: hitlOptionAllow, Name: "Allow once", Kind: "allow_once"},
			{OptionID: hitlOptionReject, Name: "Reject", Kind: "reject_once"},
		},
	}
}

// DecisionFromOption 把用户选中的 optionId 译回 hitl.Decision。
//
// 拒绝走 DecisionReject 而非 DecisionRespond：中间件据此回填一条
// "⛔ rejected by user" 的 tool 结果，模型看得到并可以自己改路子（换个命令、
// 换种做法）—— 拒绝是可重试事件，不是终止信号。DecisionRespond 的
// "停下听我说"语义需要 Message 承载用户的话，而当前 PermissionDialog 只有按钮
// 没有输入口，选它只会得到一句无信息的 "user declined to answer"。
//
// 未知 optionId 一律落到 reject：白名单式判断，新增选项忘了在这里登记时失败
// 方向是"拒绝"，不是"放行"。
//
// 这里必须返回明确的 DecisionType。hitl 中间件的 switch 没有 default 分支，
// 零值 DecisionType("") 会穿透全部 case 落到末尾的空 MiddlewareResponse，
// 也就是放行 —— 返回 hitl.Decision{} 等于批准执行。
func DecisionFromOption(interruptID, optionID string) hitl.Decision {
	if optionID == hitlOptionAllow {
		return hitl.Decision{InterruptID: interruptID, Type: hitl.DecisionApprove}
	}
	return hitl.Decision{InterruptID: interruptID, Type: hitl.DecisionReject}
}

// DecideViaACP 把 ACP 提交方包装成 FrontendContext.Decide 需要的回调形状。
//
// 等待窗口以请求的 ExpiresAt 为界（缺失时用 hitl.DefaultTimeout）：沙箱中间件
// 到点就自行判拒，若这里还挂着等一个永远不来的应答，每超时一次就多一个悬挂的
// goroutine 和 pending 表项。用同一个截止时间主动收口，两边同时结束。
//
// 出错即拒绝：ctx 结束（连接断开/session 取消）、超时、对端应答异常，一律 reject，
// 绝不因为"问不到人"就默认放行 —— 这是 HITL 唯一不可让步的默认值方向。
func DecideViaACP(ctx context.Context, asker PermissionAsker, sessionID string) func(*hitl.InterruptRequest) hitl.Decision {
	return func(req *hitl.InterruptRequest) hitl.Decision {
		timeout := hitl.DefaultTimeout
		if !req.ExpiresAt.IsZero() {
			if remaining := time.Until(req.ExpiresAt); remaining > 0 {
				timeout = remaining
			} else {
				// 已经过期：中间件那边同样会立即判拒，不必再问。
				return hitl.Decision{InterruptID: req.ID, Type: hitl.DecisionReject}
			}
		}
		askCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		optionID, err := asker.RequestPermission(askCtx, sessionID, PermissionFromInterrupt(req))
		if err != nil {
			return hitl.Decision{InterruptID: req.ID, Type: hitl.DecisionReject}
		}
		return DecisionFromOption(req.ID, optionID)
	}
}
