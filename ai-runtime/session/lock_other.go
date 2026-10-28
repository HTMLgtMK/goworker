//go:build !unix

// lock_other.go 是非 unix 平台的无锁桩。仓库 CI（ubuntu）与目标平台
// （android/darwin/linux）均为 unix 族，本文件仅为跨平台编译兜底：非 unix 上
// 不提供跨进程互斥（acquireDirLock 恒成功，Store.lock 恒 nil），
// ErrStoreLocked 不会由本层返回。
package session

import "os"

// acquireDirLock 非 unix 无锁桩：不做任何加锁，恒成功。
func acquireDirLock(dir string) (*os.File, error) {
	return nil, nil
}

// releaseDirLock 非 unix 无锁桩：no-op。
func releaseDirLock(f *os.File) error {
	return nil
}
