package service

// x-device 扩展：client-ward 设备能力中继。
//
// 分层边界：方法名与工具描述符都属应用层扩展（x- 前缀），不进 ai-dispatch/protocol
// 标准面。worker 只做声明透传与调用转发，不感知具体能力语义 —— 新增设备能力
// 只改客户端（工具注册表），Go 侧零改动。风险档位（risk_level）由描述符声明、
// HITL 中间件按元数据裁决（见 ai-runtime/middlewares/hitl.go checkSys）。

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"time"

	"github.com/tinguo/goworker/ai-core/core"
)

const (
	// MethodDeviceTools 探测客户端声明的设备工具清单。客户端未实现
	//（method-not-found）= 无设备能力，worker 不注册任何 sys 工具。
	MethodDeviceTools = "x-device/tools"
	// MethodDeviceCall 转发一次工具执行；参数与结果均为 JSON。
	MethodDeviceCall = "x-device/call"

	sysToolPrefix = "sys_"

	// maxDeviceTools 兜底限制客户端声明的工具数，防止撑爆 system prompt。
	maxDeviceTools = 32
	// probeTimeout 探测往返的上限；超时按无能力处理。
	probeTimeout = 2 * time.Second
)

// DeviceCaller 是 client-ward 调用通道（由 dispatch.Server 实现）。
type DeviceCaller interface {
	Call(ctx context.Context, method string, params, result any) error
}

// DeviceToolDescriptor 是客户端声明的工具描述符（x-device/tools 应答元素）。
type DeviceToolDescriptor struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Schema      string `json:"schema"`     // JSON Schema 字符串；非法 schema 的条目整体跳过
	RiskLevel   string `json:"risk_level"` // never | mode | always；空/非法 = mode
}

type deviceToolsResponse struct {
	Tools []DeviceToolDescriptor `json:"tools"`
}

type deviceCallRequest struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
}

type deviceCallResponse struct {
	Result string `json:"result"`
}

var deviceToolNameRe = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)

// ProbeDeviceTools 探测客户端设备工具清单。任何失败（未实现 / 断连 / 超时 /
// 应答非法）一律返回空 —— 无能力即无工具，方向必须是收缩不是放行。
func ProbeDeviceTools(ctx context.Context, caller DeviceCaller) []DeviceToolDescriptor {
	var resp deviceToolsResponse
	if err := caller.Call(ctx, MethodDeviceTools, struct{}{}, &resp); err != nil {
		return nil
	}
	if len(resp.Tools) > maxDeviceTools {
		resp.Tools = resp.Tools[:maxDeviceTools]
	}
	clean := make([]DeviceToolDescriptor, 0, len(resp.Tools))
	for _, d := range resp.Tools {
		if d.Name == "" || !deviceToolNameRe.MatchString(d.Name) {
			continue
		}
		switch d.RiskLevel {
		case "never", "mode", "always":
		default:
			d.RiskLevel = "mode"
		}
		clean = append(clean, d)
	}
	return clean
}

// RelayDeviceTools 把客户端描述符包装成 agent 工具：
// sys_ 前缀（进 HITL 门控）+ risk_level 元数据（中间件裁决用）+ 转发 Execute。
// 参数校验职责在客户端（谁定义 schema 谁校验）；调用失败翻译成错误文本回给 LLM。
func RelayDeviceTools(caller DeviceCaller, descriptors []DeviceToolDescriptor) []core.Tool {
	tools := make([]core.Tool, 0, len(descriptors))
	for _, d := range descriptors {
		d := d
		var schema map[string]any
		if err := json.Unmarshal([]byte(d.Schema), &schema); err != nil {
			schema = map[string]any{"type": "object"} // 无合法 schema 也能注册，靠 description 引导
		}
		tools = append(tools, core.Tool{
			Name:        sysToolPrefix + d.Name,
			Description: d.Description,
			Parameters:  schema,
			Metadata:    map[string]string{"risk_level": d.RiskLevel},
			Execute: func(ctx context.Context, args map[string]any) (string, error) {
				argsJSON, err := json.Marshal(args)
				if err != nil {
					return "", fmt.Errorf("encode args: %w", err)
				}
				var resp deviceCallResponse
				if err := caller.Call(ctx, MethodDeviceCall, deviceCallRequest{Name: d.Name, Arguments: argsJSON}, &resp); err != nil {
					return "", fmt.Errorf("device call %s: %w", d.Name, err)
				}
				return resp.Result, nil
			},
		})
	}
	return tools
}

// probeDeviceTools 带默认超时的探测（SetSessionServer 用，无现成 ctx）。
func probeDeviceTools(caller DeviceCaller) []DeviceToolDescriptor {
	ctx, cancel := context.WithTimeout(context.Background(), probeTimeout)
	defer cancel()
	return ProbeDeviceTools(ctx, caller)
}
