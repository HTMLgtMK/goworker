package agentcontrol

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestPackageImportConstraint(总方案 §三/§十「包依赖约束检查」):agentcontrol
// 不得反向 import plugins/agent、core/service/acp、core/model——更一般地,不得
// import 任何 daemon/internal 兄弟包(依赖方向锚定 ACP / AgentPlugin →
// agentcontrol → ai-runtime → ai-core,goworker 自有依赖只允许 ai-* 前缀)。
// 用 go/parser 断言本包全部 .go 文件(含测试文件)的 import 集合,比靠 Code
// Review 记住规则可靠;与门禁的 go list -deps grep 双保险(那个覆盖传递闭包,
// 这个覆盖「源码里写明」的直接依赖)。
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
