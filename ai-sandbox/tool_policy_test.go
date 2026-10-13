package sandbox

import "testing"

func TestNormalizeToolMetadata(t *testing.T) {
	cases := []struct {
		name           string
		metadata       map[string]string
		wantLevel      RiskLevel
		wantPermission bool
		wantForceHITL  bool
		wantReason     string
	}{
		{name: "all canonical levels", metadata: map[string]string{"risk_level": "benign"}, wantLevel: RiskBenign},
		{name: "critical remains critical", metadata: map[string]string{"risk_level": "critical", "requires_permission": "true"}, wantLevel: RiskCritical, wantPermission: true},
		{name: "missing defaults unknown", metadata: nil, wantLevel: RiskUnclassified, wantReason: "missing risk_level"},
		{name: "invalid defaults unknown", metadata: map[string]string{"risk_level": "unknown-level"}, wantLevel: RiskUnclassified, wantReason: "invalid risk_level"},
		{name: "legacy never is invalid metadata", metadata: map[string]string{"risk_level": "never"}, wantLevel: RiskUnclassified, wantReason: "invalid risk_level"},
		{name: "legacy mode is invalid metadata", metadata: map[string]string{"risk_level": "mode"}, wantLevel: RiskUnclassified, wantReason: "invalid risk_level"},
		{name: "legacy always is invalid metadata", metadata: map[string]string{"risk_level": "always"}, wantLevel: RiskUnclassified, wantReason: "invalid risk_level"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := NormalizeToolMetadata(tc.metadata)
			if got.RiskLevel != tc.wantLevel {
				t.Errorf("RiskLevel = %s, want %s", got.RiskLevel, tc.wantLevel)
			}
			if got.RequiresPermission != tc.wantPermission {
				t.Errorf("RequiresPermission = %t, want %t", got.RequiresPermission, tc.wantPermission)
			}
			if got.ForceHITL != tc.wantForceHITL {
				t.Errorf("ForceHITL = %t, want %t", got.ForceHITL, tc.wantForceHITL)
			}
			if tc.wantReason != "" && got.ParseReason == "" {
				t.Errorf("ParseReason = %q, want a reason containing %q", got.ParseReason, tc.wantReason)
			}
		})
	}
}

func TestNormalizeToolMetadataPreservesApprovalKey(t *testing.T) {
	got := NormalizeToolMetadata(map[string]string{
		"risk_level":          "medium",
		"requires_permission": "true",
		"allow_always_key":    "filesystem.write",
	})
	if got.AllowAlwaysKey != "filesystem.write" {
		t.Errorf("AllowAlwaysKey = %q, want filesystem.write", got.AllowAlwaysKey)
	}
}
