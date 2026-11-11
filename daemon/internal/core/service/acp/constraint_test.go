package acp

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/tinguo/goworker/daemon/internal/core/model"
)

// TestACPHandlerSurfaceImportConstraint keeps the ACP production layer independent of
// ai-runtime/agent: concrete EventHub types stay behind agentcontrol's subscription contract.
// The parser-based check scans production imports only; test fixtures may construct real hubs.
// Stable execution types and run-control operations should cross through agentcontrol contracts.
func TestACPHandlerSurfaceImportConstraint(t *testing.T) {
	// forbidden 是 ai-runtime/agent(EventHub 的家包):生产面 import 它即意味
	// EventHub 具体类型可被提名(§七 收敛前的越界形态:RunControlSource.Attach
	// 直接返回 *runtimeagent.EventHub)。
	forbidden := "github.com/tinguo/goworker/ai-runtime/agent"

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
		// 测试文件豁免(_test.go 后缀):fixture 需要真 hub 构造订阅。
		if strings.HasSuffix(entry.Name(), "_test.go") {
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
			if path == forbidden {
				t.Errorf("%s imports %q (依赖方向越界:ACP 生产面不得触碰 ai-runtime/agent——EventHub 具体类型必须留在 agentcontrol 订阅契约之后,handler 只消费 AttachResult/EventSubscription)", entry.Name(), path)
			}
		}
	}
	if scanned == 0 {
		t.Fatal("no non-test .go files scanned (test must run inside the acp package directory)")
	}
}

// TestRunControlSourceExcludesSyncEntryPoints keeps host-only StartRunSync/CancelRunSync
// off the ACP interface, preventing transport callers from bypassing lease fencing. Reflection
// verifies the negative constraint (Go has no negative interface assertion); compile-time
// assertions cover the trusted InternalRunner implementations.
func TestRunControlSourceExcludesSyncEntryPoints(t *testing.T) {
	iface := reflect.TypeOf((*model.RunControlSource)(nil)).Elem()
	// 负向:同步宿主入口的两个方法不得进入 ACP 消费面。
	for _, m := range []string{"StartRunSync", "CancelRunSync"} {
		if _, ok := iface.MethodByName(m); ok {
			t.Errorf("model.RunControlSource 含 %s(两面的方法集分离被破坏:同步宿主入口是 agentcontrol.InternalRunner 受限面,不得回流 ACP 消费面——ACP 面将结构性绕过 fencing,见 capability.go 方法集纪律)", m)
		}
	}
	// 正向锚点:fencing 面必须仍在,防止接口被改空后本守卫静默通过。
	for _, m := range []string{"StartRun", "CancelRun"} {
		if _, ok := iface.MethodByName(m); !ok {
			t.Errorf("model.RunControlSource 缺 %s(fencing 面被误删,协议面失去唯一合法的 Run 提交/取消入口)", m)
		}
	}
}
