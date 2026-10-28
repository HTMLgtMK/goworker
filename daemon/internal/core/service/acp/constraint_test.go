package acp

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestACPHandlerSurfaceImportConstraint(总方案 §七/§十 PR3 完成标准):ACP 的
// 生产面(handler.go/xgw.go/xproto.go 等非测试文件)不得 import
// ai-runtime/agent——EventHub 的具体类型(*runtimeagent.EventHub)必须留在
// agentcontrol 订阅契约之后(§二不变量 7「EventHub 不向协议层暴露具体实例:
// 通过订阅契约传递事件」),ACP 只消费 agentcontrol.AttachResult/EventSubscription
// 与 session.SessionEvent 通道;不 import 即不可提名,比逐文件 grep 类型字面量
// 可靠(git grep 会漏掉别名/点导入形态)。测试文件豁免:fixture 需要真
// EventHub 构造订阅(fakeRunControl/xgwLiveControl mirror 生产的编排),约束
// 面是 handler 的生产编译面。用 go/parser 断言(ImportOnly 快扫,agentcontrol
// 包的 TestPackageImportConstraint 先例),CI 上钉死依赖方向,不靠 Code Review
// 记住规则。
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
