package version

// Info 是一次解析后的版本快照。
//
// 所有字段都在构建期就定死，运行期不再变化——所以是值类型，按值传递，
// 不存在被下游改坏的风险。
type Info struct {
	Version  string // 语义化版本号（v1.2.3）。CI 用 ldflags 注入；本地构建回落为 "(devel)"
	Commit   string // 完整 commit sha。ldflags 注入，或 buildinfo 的 vcs.revision
	Date     string // RFC3339 时间。注意这是**提交时间**，不是构建时间
	Modified bool   // 构建时工作区有未提交改动 —— 此时 Commit 指向的代码并非二进制真实内容
	GoVer    string // go 工具链版本
	OS       string
	Arch     string
}
