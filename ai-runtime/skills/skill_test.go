package skills

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeSKILL 在 root/name 下写一个 SKILL.md，供测试造目录结构。
func writeSKILL(t *testing.T, root, name, body string) {
	t.Helper()
	dir := filepath.Join(root, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, SKILLMD), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestParseSKILLMD_Valid(t *testing.T) {
	dir := t.TempDir()
	writeSKILL(t, dir, "gofmt", "---\nname: gofmt\ndescription: 格式化 Go 代码\n---\n用 gofmt 清理代码。\n")

	skill, ok, err := parseSKILLMD(filepath.Join(dir, "gofmt", SKILLMD))
	if err != nil || !ok {
		t.Fatalf("parse = ok=%v, err=%v", ok, err)
	}
	if skill.Name != "gofmt" || skill.Description != "格式化 Go 代码" {
		t.Errorf("skill = %+v", skill)
	}
	if !strings.Contains(skill.Content, "gofmt") {
		t.Errorf("content lost: %q", skill.Content)
	}
	if skill.Source != filepath.Join(dir, "gofmt") {
		t.Errorf("source = %q", skill.Source)
	}
}

func TestParseSKILLMD_NoFrontmatter(t *testing.T) {
	dir := t.TempDir()
	writeSKILL(t, dir, "plain", "# 标题\n这不是 skill 定义\n")

	_, ok, err := parseSKILLMD(filepath.Join(dir, "plain", SKILLMD))
	if err != nil || ok {
		t.Fatalf("ok=%v err=%v, want skip", ok, err)
	}
}

func TestParseSKILLMD_MissingName(t *testing.T) {
	dir := t.TempDir()
	writeSKILL(t, dir, "anon", "---\ndescription: 没有名字\n---\n正文\n")

	_, _, err := parseSKILLMD(filepath.Join(dir, "anon", SKILLMD))
	if err == nil || !strings.Contains(err.Error(), "name") {
		t.Fatalf("want missing-name error, got %v", err)
	}
}

func TestParseSKILLMD_BadName(t *testing.T) {
	dir := t.TempDir()
	writeSKILL(t, dir, "bad", "---\nname: bad name\n---\n正文\n")

	_, _, err := parseSKILLMD(filepath.Join(dir, "bad", SKILLMD))
	if err == nil {
		t.Fatal("want error for name with space")
	}
}

func TestParseSKILLMD_CRLFAndBOM(t *testing.T) {
	dir := t.TempDir()
	// BOM + CRLF 换行，模拟 Windows 编辑器产物
	writeSKILL(t, dir, "bom", "\ufeff---\r\nname: bom\r\ndescription: 容忍 CRLF 和 BOM\r\n---\r\n正文\r\n")

	skill, ok, err := parseSKILLMD(filepath.Join(dir, "bom", SKILLMD))
	if err != nil || !ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
	if skill.Name != "bom" || skill.Description != "容忍 CRLF 和 BOM" {
		t.Errorf("skill = %+v", skill)
	}
}

func TestDiscover_MultiDirOverride(t *testing.T) {
	userDir := t.TempDir()
	projDir := t.TempDir()

	writeSKILL(t, userDir, "greet", "---\nname: greet\ndescription: 用户级问候\n---\n用户级正文\n")
	writeSKILL(t, userDir, "db", "---\nname: db\ndescription: 数据库\n---\n数据库正文\n")
	// 项目级同名 greet 应覆盖用户级（Discover 目录参数后者优先）
	writeSKILL(t, projDir, "greet", "---\nname: greet\ndescription: 项目级问候\n---\n项目级正文\n")

	skills, errs := Discover(userDir, projDir)
	if len(errs) != 0 {
		t.Fatalf("Discover errors: %v", errs)
	}
	if len(skills) != 2 {
		t.Fatalf("got %d skills, want 2: %+v", len(skills), skills)
	}
	// 不依赖返回顺序（目录扫描按字典序），按 name 建索引再断言
	byName := make(map[string]Skill, len(skills))
	for _, s := range skills {
		byName[s.Name] = s
	}
	if got := byName["greet"]; got.Description != "项目级问候" || got.Content != "项目级正文" {
		t.Errorf("greet should be overridden by project: %+v", got)
	}
	if _, ok := byName["db"]; !ok {
		t.Errorf("db missing: %+v", byName)
	}
}

func TestDiscover_SkipsNonDirAndHidden(t *testing.T) {
	dir := t.TempDir()
	writeSKILL(t, dir, "real", "---\nname: real\ndescription: 真 skill\n---\n正文\n")
	// 普通文件
	os.WriteFile(filepath.Join(dir, "note.md"), []byte("不是目录"), 0o644)
	// 隐藏目录
	writeSKILL(t, dir, ".hidden", "---\nname: hidden\ndescription: 应被跳过\n---\n正文\n")
	// 目录下没有 SKILL.md
	os.MkdirAll(filepath.Join(dir, "nofile"), 0o755)

	skills, errs := Discover(dir)
	if len(errs) != 0 {
		t.Fatalf("Discover errors: %v", errs)
	}
	if len(skills) != 1 || skills[0].Name != "real" {
		t.Errorf("got %+v, want only real", skills)
	}
}

func TestDiscover_MissingDir(t *testing.T) {
	skills, errs := Discover(filepath.Join(t.TempDir(), "does-not-exist"))
	if len(errs) != 0 {
		t.Fatalf("Discover on missing dir: %v", errs)
	}
	if len(skills) != 0 {
		t.Errorf("want empty, got %+v", skills)
	}
}

func TestDiscover_NoDirs(t *testing.T) {
	skills, errs := Discover()
	if len(errs) != 0 || len(skills) != 0 {
		t.Fatalf("Discover() = %v, errs=%v", skills, errs)
	}
}

func TestDiscover_BadSkillSkippedAndReported(t *testing.T) {
	dir := t.TempDir()
	writeSKILL(t, dir, "good", "---\nname: good\ndescription: ok\n---\n正文\n")
	writeSKILL(t, dir, "broken", "---\ndescription: 缺 name\n---\n正文\n")

	skills, errs := Discover(dir)
	if len(errs) != 1 {
		t.Fatalf("errs = %v, want 1", errs)
	}
	if len(skills) != 1 || skills[0].Name != "good" {
		t.Errorf("good skill should survive a bad sibling: %+v", skills)
	}
}
