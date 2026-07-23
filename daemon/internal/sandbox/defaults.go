package sandbox

import (
	"fmt"
	"regexp"
)

// DefaultDeniedPatterns 是默认拒绝的命令模式。
// 匹配的命令会被直接拦截，不经过用户确认。
var DefaultDeniedPatterns = []string{
	`rm\s+(-rf?\s+)?/\s*$`,         // rm -rf /（根目录删除）
	`:\(\)\s*\{`,                    // fork bomb
	`dd\s+if=`,                      // dd 读盘
	`mkfs\.`,                        // 格式化文件系统
	`>\s*/dev/`,                     // 写设备文件
	`chmod\s+777\s+/`,              // 改根目录权限
	`wget\s+.*\|\s*(bash|sh)`,      // 远程下载并执行
	`curl\s+.*\|\s*(bash|sh)`,
	`eval\s*\$\(`,                   // eval 远程内容
	`\|\s*sh\s*$`,                  // pipe to sh
}

// DefaultRiskyPatterns 是默认高风险命令模式。
// 匹配的命令在 normal 模式下需要用户确认，在 strict 模式下直接拒绝。
var DefaultRiskyPatterns = []string{
	`rm\s+`,      // 删除
	`mv\s+`,      // 移动
	`dd\s+`,      // 磁盘操作
	`>`,          // 重定向写
	`>>`,         // 追加重定向
	`\|`,         // 管道（可能链式执行）
	`sudo\s+`,    // 提权
	`chmod\s+`,   // 改权限
	`chown\s+`,   // 改所有者
	`kill\s+`,    // 杀进程
	`shutdown`,   // 关机
	`reboot`,     // 重启
	`init\s+0`,   // 关机（init）
	`init\s+6`,   // 重启（init）
}

// CompilePatterns 编译字符串模式列表为正则表达式列表。
func CompilePatterns(patterns []string) ([]*regexp.Regexp, error) {
	compiled := make([]*regexp.Regexp, 0, len(patterns))
	for _, p := range patterns {
		re, err := regexp.Compile(p)
		if err != nil {
			return nil, fmt.Errorf("compile pattern %q: %w", p, err)
		}
		compiled = append(compiled, re)
	}
	return compiled, nil
}

// MustCompile 等价于 CompilePatterns，编译失败时 panic。
func MustCompile(patterns []string) []*regexp.Regexp {
	compiled, err := CompilePatterns(patterns)
	if err != nil {
		panic(fmt.Sprintf("sandbox: %v", err))
	}
	return compiled
}
