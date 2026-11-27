//go:build unix

// lock_unix.go uses a permanent 0600 lock file and nonblocking exclusive flock per
// session directory. Contention or unsupported/degraded flock semantics fail closed with
// ErrStoreLocked; flock also rejects a second same-process descriptor. Never unlink the
// lock file: another process could lock a different inode and defeat mutual exclusion.
package session

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

func requireLockSupport() error { return nil }

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

// acquireDirLockContext serializes repository-level publication and lookup while honoring
// cancellation. Nonblocking flock attempts let ctx.Done interrupt contention without a helper
// goroutine; a successful lock is rechecked against ctx before it is returned.
func acquireDirLockContext(ctx context.Context, dir string) (*os.File, error) {
	if ctx == nil {
		return nil, errors.New("session: nil lock context")
	}
	path := filepath.Join(dir, "lock")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("session: open blocking lock file %s: %w", path, err)
	}

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, errors.Join(ctxErr, releaseDirLock(f))
			}
			return f, nil
		}
		if err == syscall.EINTR {
			continue
		}
		if err != syscall.EWOULDBLOCK && err != syscall.EAGAIN {
			_ = f.Close()
			return nil, fmt.Errorf("session: blocking lock %s: %w", path, err)
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, errors.Join(ctxErr, f.Close())
		}
		select {
		case <-ctx.Done():
			return nil, errors.Join(ctx.Err(), f.Close())
		case <-ticker.C:
		}
	}
}

// releaseDirLock 解锁并关闭锁句柄。
func releaseDirLock(f *os.File) error {
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_UN); err != nil {
		f.Close()
		return fmt.Errorf("session: unlock: %w", err)
	}
	return f.Close()
}
