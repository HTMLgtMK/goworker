//go:build unix

// lock_unix.go uses a permanent 0600 lock file and nonblocking exclusive flock per
// session directory. Contention or unsupported/degraded flock semantics fail closed with
// ErrStoreLocked; flock also rejects a second same-process descriptor. Never unlink the
// lock file: another process could lock a different inode and defeat mutual exclusion.
package session

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// acquireDirLock 对会话目录 dir 加排它文件锁。成功返回锁句柄（由 Store 持有，
// Close 时经 releaseDirLock 释放）；失败（已被占用 / flock 不可用）返回包裹
// ErrStoreLocked 的错误。
func acquireDirLock(dir string) (*os.File, error) {
	path := filepath.Join(dir, "lock")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("session: open lock file %s: %w", path, err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("session: lock %s: %w: %w", path, ErrStoreLocked, err)
	}
	return f, nil
}

// releaseDirLock 解锁并关闭锁句柄。锁文件本身保留在目录中（见文件头 unlink 竞态）。
func releaseDirLock(f *os.File) error {
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_UN); err != nil {
		f.Close()
		return fmt.Errorf("session: unlock: %w", err)
	}
	return f.Close()
}
