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
//
// 约束的意图(A2,第四轮评审文档化):本约束保护的是 ACP 与执行实现的隔离
// ——EventHub 具体类型(执行侧的订阅/回放机制)不得回流协议层,并非断言
// ai-runtime/agent 的全部类型都不适合 ACP 使用。若未来确有稳定领域类型(如
// RunState)需要被 ACP 直接消费,正确路径是经 agentcontrol 的扁平契约暴露
// (agentcontrol 自身就是稳定的中间契约层,已承载 AttachResult/
// EventSubscription 等跨面类型),而不是解开本约束让 ACP 直接 import 执行包;
// 同理,新的 Run 控制业务应首选归属 agentcontrol(§二不变量 8:控制平面是
// 高层业务操作的唯一入口),而非继续往 AgentPlugin 加状态——AgentPlugin 只做
// 装配与委托(总方案 §三依赖方向)。本注释即设计共识的落点:改约束前先改
// 归属,不改归属就不改约束。
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

// TestRunControlSourceExcludesSyncEntryPoints(R3,第四轮评审)钉死两个面的方法
// 集分离:StartRunSync/CancelRunSync 只存在于 agentcontrol.InternalRunner(受限
// 内部提交面,仅供宿主组装面注入可信本地命令 Adapter,agentcontrol/runs.go),
// 绝不回流 model.RunControlSource——handler.go 的 h.runs 静态类型是本接口,Sync
// 方法不在方法集内 ⇒ ACP/x-goworker 面结构性无法绕过 fencing(run/start|cancel
// 一律走 StartRun/CancelRun 的 Connection Lease 临界区)。Go 无编译期负断言
// (接口类型无法声明「不得含某方法」),反射守卫 + CI 是负向事实的可落地最强
// 钉法;正向分离由编译期断言承担(agentcontrol.runs.go 的 var _ InternalRunner
// = (*Service)(nil)、runcontrol.go 的 var _ agentcontrol.InternalRunner =
// (*AgentPlugin)(nil))。若有人往 RunControlSource 加 Sync 方法,本测试与
// capability.go 的方法集纪律注释两处都会拦。
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
