// Package dispatch 提供 ACP（Agent Client Protocol）的双角色实现：
// Client 驱动 worker agent 子进程执行任务，Server 对外暴露任务入口。
package dispatch

import "crypto/rand"

// newHexID 生成 <prefix>_<16hex> 形式的短 ID。
func newHexID(prefix string) string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("dispatch: rand: " + err.Error())
	}
	return prefix + "_" + hexEncode(b[:])
}
