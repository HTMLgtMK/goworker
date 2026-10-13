package sandbox

import "strings"

const (
	ToolMetadataRiskLevel          = "risk_level"
	ToolMetadataRequiresPermission = "requires_permission"
	ToolMetadataAllowAlwaysKey     = "allow_always_key"
)

// ToolPolicyMetadata 是工具定义携带的策略声明，不进入 provider 的工具 schema。
type ToolPolicyMetadata struct {
	RiskLevel          RiskLevel
	RequiresPermission bool
	AllowAlwaysKey     string
	ForceHITL          bool
	ParseReason        string
}

// NormalizeToolMetadata 把工具 metadata 归一为 fail-closed 的内部策略声明。
func NormalizeToolMetadata(metadata map[string]string) ToolPolicyMetadata {
	result := ToolPolicyMetadata{RiskLevel: RiskUnclassified}
	if metadata == nil {
		result.ParseReason = "missing risk_level"
		return result
	}

	risk, exists := metadata[ToolMetadataRiskLevel]
	parsed, err := ParseRiskLevel(risk)
	if err != nil {
		if exists {
			result.ParseReason = "invalid risk_level"
		} else {
			result.ParseReason = "missing risk_level"
		}
	} else {
		result.RiskLevel = parsed
	}
	if result.ParseReason == "" && !exists {
		result.ParseReason = "missing risk_level"
	}

	if strings.EqualFold(metadata[ToolMetadataRequiresPermission], "true") {
		result.RequiresPermission = true
	}
	result.AllowAlwaysKey = strings.TrimSpace(metadata[ToolMetadataAllowAlwaysKey])
	if strings.EqualFold(metadata["permission_policy"], "always_hitl") {
		result.RequiresPermission = true
		result.ForceHITL = true
		if result.ParseReason == "" {
			result.ParseReason = "permission_policy=always_hitl"
		}
	}
	return result
}
