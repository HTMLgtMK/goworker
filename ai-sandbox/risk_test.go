package sandbox

import (
	"reflect"
	"testing"
)

func TestRiskLevel_StringParseRoundTrip(t *testing.T) {
	cases := []struct {
		level RiskLevel
		name  string
	}{
		{RiskBenign, "benign"},
		{RiskLow, "low"},
		{RiskMedium, "medium"},
		{RiskElevated, "elevated"},
		{RiskHigh, "high"},
		{RiskCritical, "critical"},
		{RiskUnclassified, "unclassified"},
		{RiskReserved, "reserved"},
	}
	for _, tc := range cases {
		if got := tc.level.String(); got != tc.name {
			t.Errorf("%v.String() = %q, want %q", tc.level, got, tc.name)
		}
		parsed, err := ParseRiskLevel(tc.name)
		if err != nil {
			t.Errorf("ParseRiskLevel(%q) err = %v", tc.name, err)
			continue
		}
		if parsed != tc.level {
			t.Errorf("ParseRiskLevel(%q) = %v, want %v", tc.name, parsed, tc.level)
		}
	}
}

func TestRiskLevel_ParseInvalid(t *testing.T) {
	bad := []string{"", "unknown", "risky", "4", "RiskHigh", "LOW"}
	for _, s := range bad {
		if _, err := ParseRiskLevel(s); err == nil {
			t.Errorf("ParseRiskLevel(%q) = nil, want error", s)
		}
	}
}

func TestRiskLevel_MarshalText(t *testing.T) {
	if b, err := RiskHigh.MarshalText(); err != nil || string(b) != "high" {
		t.Errorf("MarshalText(high) = %q, %v", b, err)
	}
	var lv RiskLevel
	if err := lv.UnmarshalText([]byte("unclassified")); err != nil {
		t.Fatalf("UnmarshalText err = %v", err)
	}
	if lv != RiskUnclassified {
		t.Errorf("UnmarshalText(unclassified) = %v", lv)
	}
}

func TestEffects_HasAddNames(t *testing.T) {
	var e Effects
	if e.Has(EffectFileWrite) {
		t.Error("zero Effects should not have any bit")
	}
	e = e.Add(EffectFileWrite, EffectNetwork)
	if !e.Has(EffectFileWrite) || !e.Has(EffectNetwork) || e.Has(EffectDestructive) {
		t.Errorf("Add/Has mismatch: %v", e)
	}
	want := []string{"file_write", "network"}
	if got := e.Names(); !reflect.DeepEqual(got, want) {
		t.Errorf("Names() = %v, want %v", got, want)
	}
}

func TestEffects_NamesOrderStable(t *testing.T) {
	// 全位集顺序固定：按声明顺序，不随位值波动
	all := Effects(0)
	for _, en := range effectNames {
		all = all.Add(en.f)
	}
	want := []string{"file_read", "file_write", "network", "privileged", "destructive", "secret_access", "process_spawn", "code_execution"}
	if got := all.Names(); !reflect.DeepEqual(got, want) {
		t.Errorf("all Names() = %v, want %v", got, want)
	}
}

func TestSource_String(t *testing.T) {
	cases := map[Source]string{
		SourceBypass:       "bypass",
		SourceRule:         "rule",
		SourceClassifier:   "classifier",
		SourceHeuristic:    "heuristic",
		SourceUnclassified: "unclassified",
	}
	for src, want := range cases {
		if got := src.String(); got != want {
			t.Errorf("Source(%d).String() = %q, want %q", src, got, want)
		}
	}
}
