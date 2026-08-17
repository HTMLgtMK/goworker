package sandbox

import (
	"reflect"
	"testing"
)

func TestRiskLevel_StringParseRoundTrip(t *testing.T) {
	for i := 0; i <= 7; i++ {
		lv := RiskLevel(i)
		want := "R" + string(rune('0'+i))
		if got := lv.String(); got != want {
			t.Errorf("RiskLevel(%d).String() = %q, want %q", i, got, want)
		}
		parsed, err := ParseRiskLevel(want)
		if err != nil {
			t.Errorf("ParseRiskLevel(%q) err = %v", want, err)
			continue
		}
		if parsed != lv {
			t.Errorf("ParseRiskLevel(%q) = %v, want %v", want, parsed, lv)
		}
	}
}

func TestRiskLevel_ParseInvalid(t *testing.T) {
	bad := []string{"", "R", "R8", "r1", "R10", "4", "RiskR4"}
	for _, s := range bad {
		if _, err := ParseRiskLevel(s); err == nil {
			t.Errorf("ParseRiskLevel(%q) = nil, want error", s)
		}
	}
}

func TestRiskLevel_MarshalText(t *testing.T) {
	if b, err := RiskR4.MarshalText(); err != nil || string(b) != "R4" {
		t.Errorf("MarshalText(R4) = %q, %v", b, err)
	}
	var lv RiskLevel
	if err := lv.UnmarshalText([]byte("R6")); err != nil {
		t.Fatalf("UnmarshalText err = %v", err)
	}
	if lv != RiskR6 {
		t.Errorf("UnmarshalText(R6) = %v", lv)
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
