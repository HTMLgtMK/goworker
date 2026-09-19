package version

import (
	"strings"
	"testing"
)

// setInjected 覆写 ldflags 注入位，测试结束自动还原。
// 这三个是包级变量，测试里改它们必须清理，否则污染同包其它用例。
func setInjected(t *testing.T, v, c, d string) {
	t.Helper()
	ov, oc, od := version, commit, date
	t.Cleanup(func() { version, commit, date = ov, oc, od })
	version, commit, date = v, c, d
}

func TestGetInjectedWins(t *testing.T) {
	setInjected(t, "v1.2.3", "abcdef1234567890", "2026-09-18T10:00:00Z")

	got := Get()

	if got.Version != "v1.2.3" {
		t.Errorf("Version = %q, 期望注入值 v1.2.3", got.Version)
	}
	if got.Commit != "abcdef1234567890" {
		t.Errorf("Commit = %q, 期望注入值", got.Commit)
	}
	if got.Date != "2026-09-18T10:00:00Z" {
		t.Errorf("Date = %q, 期望注入值", got.Date)
	}
}

// 不注入时也不能出现空串——调用方打印版本号不该先判空。
func TestGetNeverEmpty(t *testing.T) {
	setInjected(t, "", "", "")

	got := Get()

	for name, val := range map[string]string{
		"Version": got.Version,
		"Commit":  got.Commit,
		"Date":    got.Date,
		"GoVer":   got.GoVer,
		"OS":      got.OS,
		"Arch":    got.Arch,
	} {
		if val == "" {
			t.Errorf("%s 为空，normalize 没兜住", name)
		}
	}
}

func TestShortCommit(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"长 sha 截 7 位", "abcdef1234567890", "abcdef1"},
		{"正好 7 位", "abcdef1", "abcdef1"},
		{"短于 7 位不 panic", "abc", "abc"},
		{"占位符原样返回", "unknown", "unknown"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := Info{Commit: tc.in}.ShortCommit()
			if got != tc.want {
				t.Errorf("ShortCommit(%q) = %q, 期望 %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestString(t *testing.T) {
	info := Info{
		Version: "v1.2.3", Commit: "abcdef1234567890", Date: "2026-09-18T10:00:00Z",
		GoVer: "go1.26.1", OS: "linux", Arch: "amd64",
	}

	got := info.String()

	for _, want := range []string{"v1.2.3", "abcdef1", "linux/amd64", "go1.26.1"} {
		if !strings.Contains(got, want) {
			t.Errorf("String() = %q, 缺少 %q", got, want)
		}
	}
	if strings.Contains(got, "未提交改动") {
		t.Errorf("干净工作区不该带脏标记: %q", got)
	}
}

// 脏工作区必须显式提示——否则 commit 看着有值，实际对不上二进制。
func TestStringFlagsModified(t *testing.T) {
	got := Info{Version: "v1", Commit: "abc", Date: "d", Modified: true}.String()

	if !strings.Contains(got, "未提交改动") {
		t.Errorf("Modified=true 时应提示工作区脏: %q", got)
	}
}
