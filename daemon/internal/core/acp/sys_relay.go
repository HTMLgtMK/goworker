package acp

// x-device 扩展：client-ward 设备能力中继。
//
// 分层边界：方法名与工具描述符都属应用层扩展（x- 前缀），不进 ai-acp/protocol。
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
	"github.com/tinguo/goworker/ai-sandbox"
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
	Name             string `json:"name"`
	Description      string `json:"description"`
	Schema           string `json:"schema"`     // JSON Schema 字符串；非法 schema 的条目整体跳过
	RiskLevel        string `json:"risk_level"` // canonical benign..reserved; legacy values are normalized at the boundary
	permissionPolicy string
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
	seen := map[string]bool{}
	for _, d := range resp.Tools {
		if d.Name == "" || !deviceToolNameRe.MatchString(d.Name) || seen[d.Name] {
			continue // 重复名剔除：toolMap 静默保留最后一个，不如源头去掉
		}
		seen[d.Name] = true
		clean = append(clean, normalizeDeviceToolDescriptor(d))
	}
	return clean
}

func normalizeDeviceToolDescriptor(d DeviceToolDescriptor) DeviceToolDescriptor {
	switch d.RiskLevel {
	case "never":
		// Legacy never explicitly opts out of the permission gate.
		d.RiskLevel = sandbox.RiskLow.String()
		d.permissionPolicy = "allow"
	case "mode":
		// Legacy mode follows the normal external-tool permission policy.
		d.RiskLevel = sandbox.RiskUnclassified.String()
		d.permissionPolicy = "mode"
	case "always":
		d.RiskLevel = sandbox.RiskCritical.String()
		d.permissionPolicy = "always_hitl"
	default:
		if _, err := sandbox.ParseRiskLevel(d.RiskLevel); err != nil {
			d.RiskLevel = sandbox.RiskUnclassified.String()
		}
		// Canonical descriptors are policy-controlled by default. The legacy
		// never escape hatch is the only descriptor that is not gated.
		if d.permissionPolicy == "" {
			d.permissionPolicy = "mode"
		}
	}
	return d
}

// RelayDeviceTools wraps client descriptors as agent tools. Metadata is kept
// on the core.Tool boundary so policy middleware can inspect it without
// leaking policy fields into the provider schema.
func RelayDeviceTools(caller DeviceCaller, descriptors []DeviceToolDescriptor) []core.Tool {
	tools := make([]core.Tool, 0, len(descriptors))
	for _, raw := range descriptors {
		d := normalizeDeviceToolDescriptor(raw)
		var schema map[string]any
		if err := json.Unmarshal([]byte(d.Schema), &schema); err != nil || schema == nil {
			// "null" unmarshals into a nil map; use a valid fallback schema.
			schema = map[string]any{"type": "object"}
		}

		metadata := map[string]string{"risk_level": d.RiskLevel}
		if d.permissionPolicy != "allow" {
			metadata["requires_permission"] = "true"
			metadata["allow_always_key"] = sysToolPrefix + d.Name
		}
		if d.permissionPolicy == "always_hitl" {
			metadata["permission_policy"] = "always_hitl"
		}

		tools = append(tools, core.Tool{
			Name:        sysToolPrefix + d.Name,
			Description: d.Description,
			Parameters:  schema,
			Metadata:    metadata,
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
