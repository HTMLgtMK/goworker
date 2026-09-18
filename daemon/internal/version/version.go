// Package version 汇总两路版本信息来源：构建期注入 + Go 工具链自带。
//
// 分工是这样的：
//
//   - 语义化版本号（v1.2.3）**只能**靠 CI 用 -ldflags -X 注入。本地 go build
//     没有 tag 可依据，buildinfo 里主模块版本恒为 "(devel)"。
//   - commit / 提交时间 / 工作区是否脏，Go 1.18+ 由 -buildvcs（默认开启）自动
//     嵌进二进制，debug.ReadBuildInfo() 直接读，不需要注入。这意味着本地
//     go run 出来的也能报出真实 commit，而不是一片 unknown。
//
// 所以别把 commit/date 也塞进 ldflags —— 那是重复劳动，而且一旦 CI 忘了传就
// 退化成 unknown，白白丢掉工具链本来免费给的信息。
package version

import (
	"fmt"
	"runtime"
	"runtime/debug"
)

// 以下三个由构建期注入：
//
//	go build -ldflags "-X github.com/tinguo/goworker/daemon/internal/version.version=v1.0.0"
//
// 刻意保持未导出：注入点私有，读取一律走 Get()。外部包拿不到原始变量，
// 也就绕不过回落逻辑去用空串。
var (
	version string
	commit  string
	date    string
)

// Get 解析版本信息。
//
// 优先级：ldflags 注入 > Go 工具链内置 VCS 元数据 > 占位符。
func Get() Info {
	info := Info{
		Version: version,
		Commit:  commit,
		Date:    date,
		GoVer:   runtime.Version(),
		OS:      runtime.GOOS,
		Arch:    runtime.GOARCH,
	}

	bi, ok := debug.ReadBuildInfo()
	if !ok {
		// 极端情况：-buildvcs=false 且链接方式特殊，拿不到 buildinfo。
		// 注入了多少用多少，缺的交给 normalize 填占位符。
		return info.normalize()
	}

	// 主模块从工作区构建时 Main.Version 恒为 "(devel)"；只有 go install
	// pkg@vX.Y.Z 装出来的二进制才带真版本号。所以这一路基本是兜底，不是主力。
	if info.Version == "" {
		info.Version = bi.Main.Version
	}

	for _, s := range bi.Settings {
		switch s.Key {
		case "vcs.revision":
			if info.Commit == "" {
				info.Commit = s.Value
			}
		case "vcs.time":
			if info.Date == "" {
				info.Date = s.Value
			}
		case "vcs.modified":
			info.Modified = s.Value == "true"
		}
	}

	return info.normalize()
}

// normalize 把空字段填成占位符——调用方不该为了打印一行版本号去判空。
// 值接收器，返回副本，不改原值。
func (i Info) normalize() Info {
	if i.Version == "" {
		i.Version = "(devel)"
	}
	if i.Commit == "" {
		i.Commit = "unknown"
	}
	if i.Date == "" {
		i.Date = "unknown"
	}
	return i
}

// ShortCommit 返回 7 位缩略 commit，短于 7 位时原样返回。
func (i Info) ShortCommit() string {
	const n = 7
	if len(i.Commit) <= n {
		return i.Commit
	}
	return i.Commit[:n]
}

// String 输出单行版本摘要。-version 标志与 /version 命令共用同一份格式，
// 免得两处各写各的、迟早对不上。
func (i Info) String() string {
	s := fmt.Sprintf("goworker %s (%s, %s) %s/%s %s",
		i.Version, i.ShortCommit(), i.Date, i.OS, i.Arch, i.GoVer)
	if i.Modified {
		s += " [工作区有未提交改动，commit 不精确]"
	}
	return s
}
