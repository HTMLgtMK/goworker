package acp

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/tinguo/goworker/ai-acp/protocol"
	"github.com/tinguo/goworker/ai-core/core"
	"github.com/tinguo/goworker/daemon/internal/core/model"
)

// ---- daemon domain → ACP wire 映射 ----
//
// 这些函数把 daemon domain 类型（model.SessionSummary / SessionUpdate /
// SessionModeState / core.Token / model.Command）映射为 ACP wire DTO，是
// FrontendHandler 的序列化层。

// SessionInfoFromDomain 把 daemon 会话摘要映射为 wire SessionInfo。domain 的
// Active 是业务标记，不进 wire DTO（ACP SessionInfo 保持纯协议语义）。
func SessionInfoFromDomain(info model.SessionSummary) protocol.SessionInfo {
	return protocol.SessionInfo{
		SessionID: info.SessionID,
		Cwd:       info.Cwd,
		Title:     info.Title,
		UpdatedAt: info.UpdatedAt,
	}
}

// SessionModeStateFromDomain 把 daemon 模式状态映射为 wire 模式状态；nil 进 nil 出。
func SessionModeStateFromDomain(state *model.SessionModeState) *protocol.SessionModeState {
	if state == nil {
		return nil
	}
	modes := make([]protocol.SessionMode, 0, len(state.AvailableModes))
	for _, mode := range state.AvailableModes {
		modes = append(modes, protocol.SessionMode{
			ID:          mode.ID,
			Name:        mode.Name,
			Description: mode.Description,
		})
	}
	return &protocol.SessionModeState{
		CurrentModeID:  state.CurrentModeID,
		AvailableModes: modes,
	}
}

// SessionUpdateFromDomain 把 daemon 会话更新事件映射为 wire 更新载荷；Raw 透传
// 不重组，扩展字段不丢信息。
func SessionUpdateFromDomain(update model.SessionUpdate) protocol.SessionUpdateBody {
	body := protocol.SessionUpdateBody{
		SessionUpdate: update.Kind,
		ToolCallID:    update.ToolCallID,
		Title:         update.Title,
		Status:        update.Status,
		Raw:           append(json.RawMessage(nil), update.Raw...),
	}
	if update.Content != nil {
		body.Content = &protocol.ContentBlock{
			Type: update.Content.Type,
			Text: update.Content.Text,
		}
	}
	return body
}

// TokenToUpdate 把 agent 运行时 token 流映射为 ACP session/update。
func TokenToUpdate(token core.Token) protocol.SessionUpdateBody {
	switch token.Type {
	case core.TokenTypeThinking:
		return protocol.SessionUpdateBody{
			SessionUpdate: protocol.UpdateAgentThoughtChunk,
			Content:       &protocol.ContentBlock{Type: "text", Text: token.Content},
		}
	case core.TokenTypeToolCall:
		return protocol.SessionUpdateBody{
			SessionUpdate: protocol.UpdateToolCall,
			ToolCallID:    token.ToolCall.ID,
			Title:         token.Content,
			Status:        "pending",
		}
	case core.TokenTypeToolResult:
		return protocol.SessionUpdateBody{
			SessionUpdate: protocol.UpdateToolCallUpdate,
			ToolCallID:    token.ToolCallID,
			Title:         token.Content,
			Status:        "completed",
		}
	default:
		return protocol.SessionUpdateBody{
			SessionUpdate: protocol.UpdateAgentMessageChunk,
			Content:       &protocol.ContentBlock{Type: "text", Text: token.Content},
		}
	}
}

// availableCommand 是 available_commands_update 里的一条命令补全项。
// name 不带前导 /（UI 侧补全统一展示为 /name）。
type availableCommand struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Source      string `json:"source,omitempty"` // 固定 "daemon"，标注命令来自 daemon 引擎
}

// availableCommandsUpdate 是 ACP available_commands_update 的透传载荷，
// 供前端 chat 面板做 slash command 补全。整体经 Raw 透传，不重组。
type availableCommandsUpdate struct {
	SessionUpdate     string             `json:"sessionUpdate"` // 固定 "available_commands_update"
	AvailableCommands []availableCommand `json:"availableCommands"`
}

// availableCommandsUpdateBody 从命令清单构建完整透传载荷。
// 别名展开为独立条目（description 标注 alias of /xxx），空清单也照发（清空旧状态）。
func availableCommandsUpdateBody(cmds []model.Command) protocol.SessionUpdateBody {
	out := make([]availableCommand, 0, len(cmds))
	for _, cmd := range cmds {
		out = append(out, availableCommand{
			Name:        strings.TrimPrefix(cmd.Name, "/"),
			Description: cmd.Description,
			Source:      "daemon",
		})
		for _, alias := range cmd.Aliases {
			out = append(out, availableCommand{
				Name:        strings.TrimPrefix(alias, "/"),
				Description: fmt.Sprintf("(alias of %s)", cmd.Name),
				Source:      "daemon",
			})
		}
	}
	data, err := json.Marshal(availableCommandsUpdate{
		SessionUpdate:     protocol.UpdateAvailableCommands,
		AvailableCommands: out,
	})
	if err != nil {
		// 这些字段都是字符串/切片，json.Marshal 不会失败；真失败就跳过本次下发。
		return protocol.SessionUpdateBody{}
	}
	return protocol.SessionUpdateBody{Raw: data}
}
