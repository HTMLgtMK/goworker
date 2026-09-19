package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// 把 README 的 Configuration 示例喂给真实的 Load，防止文档漂移。
// 直接读 README 本身而不是在测试里抄一份——抄的那份测不出漂移，
// README 改了测试照样绿，防了个寂寞。
func TestReadmeSampleConfigParses(t *testing.T) {
	readme, err := os.ReadFile(filepath.Join("..", "..", "..", "..", "README.md"))
	if err != nil {
		t.Fatalf("读 README 失败: %v", err)
	}

	sample := extractConfigSample(string(readme))
	if sample == "" {
		t.Fatal("README 里没找到 Configuration 的 yaml 示例——围栏标记或章节标题变了？")
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(sample), 0o600); err != nil {
		t.Fatalf("写临时配置失败: %v", err)
	}

	cfg := Load(path)
	if cfg == nil {
		t.Fatal("Load 返回 nil —— README 的配置示例已与 loader 脱节，修 README 或修 loader")
	}

	p, ok := cfg.LLM.Providers["openai"]
	if !ok {
		t.Fatalf("provider openai 未解析出来: %v", cfg.LLM.Providers)
	}
	if p.Endpoint == "" || p.Model == "" {
		t.Errorf("endpoint/model 未解析: %+v", p)
	}
	if cfg.LLM.DefaultProvider != "openai" {
		t.Errorf("default_provider = %q", cfg.LLM.DefaultProvider)
	}
}

// extractConfigSample 抽 README「## Configuration」标题后的第一个 yaml 围栏。
func extractConfigSample(readme string) string {
	const section = "## Configuration"
	start := strings.Index(readme, section)
	if start < 0 {
		return ""
	}
	const fence = "```yaml"
	fs := strings.Index(readme[start:], fence)
	if fs < 0 {
		return ""
	}
	rest := readme[start+fs+len(fence):]
	fe := strings.Index(rest, "```")
	if fe < 0 {
		return ""
	}
	return rest[:fe]
}
