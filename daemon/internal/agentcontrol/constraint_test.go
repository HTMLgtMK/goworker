package agentcontrol

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestPackageImportConstraint ensures agentcontrol imports neither ACP/service/model
// packages nor any other daemon-internal package. It scans imports in all Go files,
// including tests, to enforce the dependency direction at source level.
func TestPackageImportConstraint(t *testing.T) {
	// 显式点名 forbidden 三包(方案 §十:禁止 agentcontrol 反向 import
	// plugins/agent 或 core/service/acp;core/model 的协议类型同样不得回流),
	// 断言失败信息直接指向越界文件,便于修复定位。
	forbidden := map[string]bool{
		"github.com/tinguo/goworker/daemon/internal/plugins/agent":    true,
		"github.com/tinguo/goworker/daemon/internal/core/service/acp": true,
		"github.com/tinguo/goworker/daemon/internal/core/model":       true,
	}

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}
	fset := token.NewFileSet()
	scanned := 0
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".go" {
			continue
		}
		// parser.ImportsOnly:只解析 import 声明,不触及函数体,快且足够。
		f, err := parser.ParseFile(fset, entry.Name(), nil, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("parse %s: %v", entry.Name(), err)
		}
		scanned++
		for _, imp := range f.Imports {
			path := strings.Trim(imp.Path.Value, `"`)
			if forbidden[path] {
				t.Errorf("%s imports forbidden package %q (依赖方向越界:agentcontrol 不得反向 import plugins/agent、core/service/acp、core/model)", entry.Name(), path)
				continue
			}
			// 依赖方向白名单:goworker 自有依赖只允许 ai-*(ai-core / ai-runtime
			// 及其子包);daemon/internal 的一切兄弟包都在禁止面内。
			if strings.HasPrefix(path, "github.com/tinguo/goworker/") && !strings.HasPrefix(path, "github.com/tinguo/goworker/ai-") {
				t.Errorf("%s imports %q (依赖方向越界:agentcontrol 的 goworker 自有依赖只允许 github.com/tinguo/goworker/ai-*)", entry.Name(), path)
			}
		}
	}
	if scanned == 0 {
		t.Fatal("no .go files scanned (test must run inside the agentcontrol package directory)")
	}
}
