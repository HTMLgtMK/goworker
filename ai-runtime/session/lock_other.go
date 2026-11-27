//go:build !unix

// lock_other.go fails closed on platforms without the repository's verified flock
// implementation. Adding an unverified lock backend would make shared session storage unsafe.
package session

import (
	"context"
	"fmt"
	"os"
)

func requireLockSupport() error { return ErrLockUnavailable }

// acquireDirLock refuses to open a Store when mutual exclusion cannot be guaranteed.
func acquireDirLock(dir string) (*os.File, error) {
	return nil, fmt.Errorf("session: lock store %s: %w", dir, ErrLockUnavailable)
}

// acquireDirLockContext refuses repository operations without mutual exclusion.
func acquireDirLockContext(_ context.Context, dir string) (*os.File, error) {
	return nil, fmt.Errorf("session: lock repository path %s: %w", dir, ErrLockUnavailable)
}

// releaseDirLock is a no-op because unsupported platforms never return a lock handle.
func releaseDirLock(f *os.File) error {
	return nil
}
