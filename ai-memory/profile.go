package memory

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// Profile 是用户画像 USER.md 的读写模型：每行一条画像条目，容量受限。
// 人类可直接编辑文件，agent 经 profile 工具调用 Add/Replace/Remove。
// 写入即原子落盘（tmp + rename），同时更新内存副本 —— 但注入快照由
// InstructionSet 冻结，本次会话不刷新（Hermes 语义，稳前缀缓存）。
type Profile struct {
	mu       sync.Mutex
	path     string
	maxChars int
	content  string
}

const defaultUserMaxChars = 1500

// ErrProfileFull 在新增/替换超容量时返回，agent 需先合并/删减再试（Hermes 式容量管理）。
var ErrProfileFull = errors.New("profile: capacity full, consolidate entries first")

func newProfile(path string, maxChars int) *Profile {
	if maxChars <= 0 {
		maxChars = defaultUserMaxChars
	}
	return &Profile{path: path, maxChars: maxChars}
}

func (p *Profile) reload() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.sync()
}

// sync 以磁盘为真相重载内容。冻结快照只影响注入文本，USER.md 文件本身可能被
// 会话内手改 —— 写操作前先对齐磁盘，避免基于陈旧内存整文件覆盖丢掉手改。
func (p *Profile) sync() error {
	data, err := os.ReadFile(p.path)
	if os.IsNotExist(err) {
		p.content = ""
		return nil
	}
	if err != nil {
		return fmt.Errorf("profile: read %s: %w", p.path, err)
	}
	p.content = string(data)
	return nil
}

// Content 返回画像原文（只读）。
func (p *Profile) Content() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.content
}

// Capacity 返回已用/上限字符数（rune）。
func (p *Profile) Capacity() (used, max int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return runeLen(p.content), p.maxChars
}

// AddEntry 追加一条画像条目（多行归一成一行）。重复条目自动跳过；超容量返回 ErrProfileFull。
func (p *Profile) AddEntry(content string) (string, error) {
	entry := normalizeEntry(content)
	if entry == "" {
		return "", errors.New("profile: empty entry")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.sync(); err != nil {
		return "", err
	}
	for _, l := range nonEmptyLines(p.content) {
		if l == entry {
			return "duplicate, skipped", nil
		}
	}
	next := strings.TrimSpace(p.content)
	if next != "" {
		next += "\n"
	}
	next += entry
	if runeLen(next) > p.maxChars {
		return "", ErrProfileFull
	}
	if err := atomicWrite(p.path, next); err != nil {
		return "", err
	}
	p.content = next
	return "added", nil
}

// ReplaceEntry 把唯一匹配 oldText 的整条画像条目替换为 content。
// 0 命中或 >1 命中都报错（Hermes 式唯一子串匹配，防误伤）。
func (p *Profile) ReplaceEntry(oldText, content string) (string, error) {
	oldText = strings.TrimSpace(oldText)
	repl := normalizeEntry(content)
	if oldText == "" {
		return "", errors.New("profile: empty old_text")
	}
	if repl == "" {
		return "", errors.New("profile: empty content")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.sync(); err != nil {
		return "", err
	}
	lines := nonEmptyLines(p.content)
	idx, err := uniqueHit(lines, oldText)
	if err != nil {
		return "", err
	}
	lines[idx] = repl
	next := strings.Join(lines, "\n")
	if runeLen(next) > p.maxChars {
		return "", ErrProfileFull
	}
	if err := atomicWrite(p.path, next); err != nil {
		return "", err
	}
	p.content = next
	return "replaced", nil
}

// RemoveEntry 删除唯一匹配 oldText 的画像条目。
func (p *Profile) RemoveEntry(oldText string) (string, error) {
	oldText = strings.TrimSpace(oldText)
	if oldText == "" {
		return "", errors.New("profile: empty old_text")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.sync(); err != nil {
		return "", err
	}
	lines := nonEmptyLines(p.content)
	idx, err := uniqueHit(lines, oldText)
	if err != nil {
		return "", err
	}
	lines = append(lines[:idx], lines[idx+1:]...)
	next := strings.Join(lines, "\n")
	if err := atomicWrite(p.path, next); err != nil {
		return "", err
	}
	p.content = next
	return "removed", nil
}

// uniqueHit 返回唯一包含 needle 的行下标；0 命中或 >1 命中返回错误。
func uniqueHit(lines []string, needle string) (int, error) {
	hit := -1
	for i, l := range lines {
		if strings.Contains(l, needle) {
			if hit >= 0 {
				return -1, fmt.Errorf("profile: %q matches multiple entries, be more specific", needle)
			}
			hit = i
		}
	}
	if hit < 0 {
		return -1, fmt.Errorf("profile: %q not found", needle)
	}
	return hit, nil
}

// ---- 小工具 ----

func nonEmptyLines(s string) []string {
	var out []string
	for _, l := range strings.Split(s, "\n") {
		if t := strings.TrimSpace(l); t != "" {
			out = append(out, t)
		}
	}
	return out
}

// normalizeEntry 把任意多行输入压成单行画像条目。
func normalizeEntry(s string) string {
	return strings.TrimSpace(strings.Join(strings.Fields(s), " "))
}

// atomicWrite 写 tmp 再 rename（照抄 FileStore.writeJSONL 的原子写模式）。
// 错误统一带 profile 前缀，agent 工具层能看出是哪次画像写在哪个文件挂了。
func atomicWrite(path, content string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("profile: mkdir %s: %w", filepath.Dir(path), err)
	}
	tmp := filepath.Join(filepath.Dir(path), ".profile.tmp")
	if err := os.WriteFile(tmp, []byte(content), 0o600); err != nil {
		return fmt.Errorf("profile: write %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("profile: rename %s: %w", tmp, err)
	}
	return nil
}
