package memory

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

// load 是测试助记：拼好三段文件后加载 InstructionSet。
func load(t *testing.T, user, global, project string) *InstructionSet {
	t.Helper()
	dir := t.TempDir()
	if user != "" {
		writeFile(t, filepath.Join(dir, "USER.md"), user)
	}
	if global != "" {
		writeFile(t, filepath.Join(dir, "AGENTS.md"), global)
	}
	projDir := filepath.Join(dir, "proj")
	if project != "" {
		writeFile(t, filepath.Join(projDir, "AGENTS.md"), project)
	}
	inst, err := LoadInstructions(dir, projDir, Cap{UserMaxChars: 1500, AgentsMaxChars: 4096})
	if err != nil {
		t.Fatal(err)
	}
	return inst
}

func TestLoadMissingFiles(t *testing.T) {
	inst := load(t, "", "", "")
	if snap := inst.Snapshot(); snap != "" {
		t.Fatalf("expect empty snapshot, got %q", snap)
	}
}

func TestRenderSectionsAndOrder(t *testing.T) {
	inst := load(t, "user prefers 中文", "global rule", "project rule")
	snap := inst.Snapshot()

	// 三段都进快照
	for _, want := range []string{"[Instructions]", "[User]", "global rule", "project rule", "user prefers 中文"} {
		if !strings.Contains(snap, want) {
			t.Errorf("snapshot missing %q:\n%s", want, snap)
		}
	}
	// 顺序：指令在前、用户在后（冲突时用户优先级最高）
	if i, j := strings.Index(snap, "[Instructions]"), strings.Index(snap, "[User]"); i == -1 || j == -1 || i > j {
		t.Errorf("expect [Instructions] before [User], got idx %d/%d", i, j)
	}
	// 指令段内全局在前、项目在后
	if i, j := strings.Index(snap, "global rule"), strings.Index(snap, "project rule"); i == -1 || j == -1 || i > j {
		t.Errorf("expect global before project, got idx %d/%d", i, j)
	}
}

func TestFrozenSnapshot(t *testing.T) {
	dir := t.TempDir()
	projDir := filepath.Join(dir, "proj")
	load := func() *InstructionSet {
		inst, err := LoadInstructions(dir, projDir, Cap{UserMaxChars: 1500, AgentsMaxChars: 4096})
		if err != nil {
			t.Fatal(err)
		}
		return inst
	}

	inst := load()
	snap := inst.Snapshot()
	if snap != "" {
		t.Fatalf("expect empty snapshot")
	}
	// 写入画像：磁盘与内存更新，但注入快照保持冻结（Hermes 语义）
	if _, err := inst.Profile().AddEntry("prefers short replies"); err != nil {
		t.Fatal(err)
	}
	if snap != inst.Snapshot() {
		t.Fatal("snapshot must stay frozen after profile write within session")
	}
	if !strings.Contains(inst.Profile().Content(), "prefers short replies") {
		t.Fatal("profile content should update on write")
	}
	// /new 重新构造后快照刷新
	if inst2 := load(); !strings.Contains(inst2.Snapshot(), "prefers short replies") {
		t.Fatal("snapshot should refresh after reload")
	}
}

func TestAgentsTruncatedAtCap(t *testing.T) {
	dir := t.TempDir()
	global := strings.Repeat("x", 100) // x 不出现在渲染头部，计数不受干扰
	writeFile(t, filepath.Join(dir, "AGENTS.md"), global)
	inst, err := LoadInstructions(dir, dir, Cap{AgentsMaxChars: 50})
	if err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(inst.Snapshot(), "x"); n > 50 {
		t.Fatalf("agents section not truncated to cap: %d x's, snapshot %q", n, inst.Snapshot())
	}
	if !strings.Contains(inst.Snapshot(), "…") {
		t.Fatalf("truncation marker missing: %q", inst.Snapshot())
	}
}

func TestFindUpward(t *testing.T) {
	dir := t.TempDir()
	deep := filepath.Join(dir, "a", "b", "c")
	writeFile(t, filepath.Join(dir, "AGENTS.md"), "root")
	writeFile(t, filepath.Join(dir, "a", "b", "AGENTS.md"), "mid")

	cases := []struct {
		start string
		want  string
	}{
		{deep, filepath.Join(dir, "a", "b", "AGENTS.md")},          // 最近的
		{filepath.Join(dir, "a"), filepath.Join(dir, "AGENTS.md")}, // a 下无 AGENTS.md，向上命中根目录
		{dir, filepath.Join(dir, "AGENTS.md")},
	}
	for _, c := range cases {
		if got := findUpward(c.start, "AGENTS.md"); got != c.want {
			t.Errorf("findUpward(%s) = %q, want %q", c.start, got, c.want)
		}
	}
	// 找不到返回空
	if got := findUpward(t.TempDir(), "AGENTS.md"); got != "" {
		t.Errorf("findUpward(empty dir) = %q, want \"\"", got)
	}
}

func TestMergeAgentsPrefersProject(t *testing.T) {
	global := strings.Repeat("g", 100)
	project := strings.Repeat("p", 50)
	got := mergeAgents(global, project, 80)
	// 项目优先：项目指令完整保留，从全局（头部）开始丢
	if strings.Count(got, "p") != 50 {
		t.Fatalf("project directives dropped: %q", got)
	}
	if n := strings.Count(got, "g"); n >= 100 {
		t.Fatalf("global not truncated: %d g's", n)
	}
	if !strings.Contains(got, "…") {
		t.Fatalf("truncation marker missing: %q", got)
	}
}

func TestRenderTruncatesUser(t *testing.T) {
	got := render(strings.Repeat("x", 2000), "", 1500)
	if n := strings.Count(got, "x"); n != 1500 {
		t.Fatalf("user not truncated to cap: %d x's", n)
	}
	if !strings.Contains(got, "(used 2000/1500 chars)") {
		t.Fatalf("header should show true usage: %q", got)
	}
}
