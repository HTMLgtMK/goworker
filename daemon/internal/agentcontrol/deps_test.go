package agentcontrol

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestTransitiveDependencyClosure(A1,第四轮评审):constraint_test.go 的
// TestPackageImportConstraint 是源码级直接依赖检查(go/parser 扫 import 声明),
// 只能证明「agentcontrol 没有直接 import 越界包」;本测试是 CI 级传递闭包检查
// ——go list -deps -json ./internal/agentcontrol 解析完整编译依赖闭包,断言
// 闭包内不含 daemon/internal/plugins、daemon/internal/core/service、
// daemon/internal/core/model 的任何包。两者互补:源码级=直接依赖,go list 级
// =传递闭包——ai-runtime 等下层包未来若引入对 daemon/internal 的依赖(或生成
// 文件、build tag 门控的 import 绕过源码扫描),直接依赖检查不可见,本测试拦截。
//
// build tags 两形态各跑一次(A1 明确要求):无 tag(默认形态)与
// -tags goworker_no_dispatcher(mobile 裁剪形态,dispatcher/register.go、
// builtin/dispatcher.go 的 //go:build 门控面,见 mobile/doc.go)——不同形态下
// 实际编译图可能不同,约束对两形态都必须成立。CI 的既有 go list 门禁
// (.github/workflows/ci.yml)只对 ./cmd/goworker 闭包在 dispatcher/minimal
// 变体下 grep plugins/agent,本测试把同一性质钉到 agentcontrol 自身闭包、
// 常规 go test 即可执行,不依赖 CI 脚本存活。
//
// 命令形态与验收/CI 逐字一致:cmd.Dir 定到 daemon 模块根(go.mod 所在),模式
// 串即 ./internal/agentcontrol。
func TestTransitiveDependencyClosure(t *testing.T) {
	// forbidden 是闭包黑名单前缀(总方案 §三依赖方向的 go list 面复核):
	// agentcontrol 的 goworker 自有依赖只允许 ai-* 前缀,daemon/internal 兄弟包
	// 一律不得出现在传递闭包内。前缀匹配带路径边界(精确相等或后随 /),避免
	// 误伤同前缀的合法包名。
	forbidden := []string{
		"github.com/tinguo/goworker/daemon/internal/plugins",
		"github.com/tinguo/goworker/daemon/internal/core/service",
		"github.com/tinguo/goworker/daemon/internal/core/model",
	}
	self := "github.com/tinguo/goworker/daemon/internal/agentcontrol"

	root := moduleRoot(t)

	for _, tc := range []struct {
		name string
		tags []string
	}{
		{name: "default"},
		{name: "goworker_no_dispatcher", tags: []string{"goworker_no_dispatcher"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := []string{"list", "-deps", "-json"}
			if len(tc.tags) > 0 {
				args = append(args, "-tags", strings.Join(tc.tags, ","))
			}
			args = append(args, "./internal/agentcontrol")

			cmd := exec.Command("go", args...)
			cmd.Dir = root
			cmd.Env = os.Environ()
			var stderr bytes.Buffer
			cmd.Stderr = &stderr
			out, err := cmd.Output()
			if err != nil {
				t.Fatalf("go %s: %v\nstderr: %s", strings.Join(args, " "), err, stderr.String())
			}

			// go list -json 输出是连续的 JSON 对象流(每包一个),逐个解码。
			dec := json.NewDecoder(bytes.NewReader(out))
			seen := 0
			sawSelf := false
			for dec.More() {
				var pkg struct {
					ImportPath string
					DepOnly    bool
				}
				if err := dec.Decode(&pkg); err != nil {
					t.Fatalf("decode go list output: %v", err)
				}
				seen++
				if pkg.ImportPath == self {
					sawSelf = true
					if pkg.DepOnly {
						t.Errorf("%s 在闭包内却标记 DepOnly=true(命名包应 DepOnly=false,解析面与预期不符)", self)
					}
				}
				for _, f := range forbidden {
					if pkg.ImportPath == f || strings.HasPrefix(pkg.ImportPath, f+"/") {
						t.Errorf("agentcontrol 依赖闭包含 %q (DepOnly=%v;依赖方向越界:agentcontrol → ai-runtime → ai-core,daemon/internal 的 plugins、core/service、core/model 不得出现在传递闭包内——源码级约束见 TestPackageImportConstraint,本处管传递闭包)", pkg.ImportPath, pkg.DepOnly)
					}
				}
			}
			// 完整性守卫:闭包非空且含 agentcontrol 自身,防止 go list 静默空输出
			// (如模式解析失败被吞)让本测试假通过。
			if seen == 0 {
				t.Fatal("go list 输出为空(闭包检查未执行,模式或命令行失效)")
			}
			if !sawSelf {
				t.Fatalf("闭包 %d 包中未见 %s(输出不含命名包本身,检查命令形态)", seen, self)
			}
		})
	}
}

// moduleRoot 返回 daemon 模块根(go.mod 所在目录):go test 的测试二进制 cwd 恒
// 为被测包目录(daemon/internal/agentcontrol),模块根在其上两级;向上逐级找
// go.mod 而非硬编码层级,目录迁移时仍成立。go.work(仓库根)不影响解析——
// cmd.Dir 定在 daemon 模块根,./internal/agentcontrol 相对它展开,与验收命令
// go test -C daemon 的工作目录语义一致。
func moduleRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for i := 0; i < 8; i++ {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	t.Fatalf("daemon module root (go.mod) not found upward from %s", dir)
	return ""
}
