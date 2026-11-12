// Package version combines build-time version injection with Go's VCS metadata.
package version

import (
	"fmt"
	"runtime"
	"runtime/debug"
)

// Build-time overrides are optional; Get fills missing fields from VCS metadata.
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
