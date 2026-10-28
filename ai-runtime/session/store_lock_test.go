//go:build unix

// store_lock_test.go 验证跨进程文件锁（P1-2）。flock 语义为 unix 族专属
// （非 unix 平台是无锁桩，acquireDirLock 恒成功、永不返回 ErrStoreLocked），
// 故本文件仅在 unix 构建。
package session

import (
	"errors"
	"testing"
)

// TestStoreDoubleOpenRejected 验证同目录第二次 Open 被拒绝（ErrStoreLocked，
// 宁拒勿双写），Close 释放锁后可再次打开且身份不变。这是 P1-1 进程内实例唯一
// （Repository 缓存，TestConcurrentStoreOpen）的跨进程互补防线：flock 以
// open file description 为界，同进程对同一目录的第二个句柄同样抢不到锁，
// 直接双开也走拒绝路径。
func TestStoreDoubleOpenRejected(t *testing.T) {
	dir := tempDir(t)

	first, err := Open(dir)
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	t.Cleanup(func() { _ = first.Close() })

	second, err := Open(dir)
	if second != nil {
		_ = second.Close()
	}
	if !errors.Is(err, ErrStoreLocked) {
		t.Fatalf("second Open: err = %v, want ErrStoreLocked", err)
	}

	// Close 释放锁 → 同目录可再次打开，会话身份保持。
	if err := first.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	third, err := Open(dir)
	if err != nil {
		t.Fatalf("Open after Close: %v", err)
	}
	t.Cleanup(func() { _ = third.Close() })
	if got := third.SessionID(); got != first.SessionID() {
		t.Errorf("SessionID after relock = %q, want %q", got, first.SessionID())
	}
}
