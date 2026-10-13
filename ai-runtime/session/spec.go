package session

import "time"

// SessionStatus 是持久化会话的生命周期状态。
type SessionStatus string

const (
	SessionStatusLive     SessionStatus = "live"
	SessionStatusArchived SessionStatus = "archived"
)

// SessionMeta 是写入单个会话日志的生命周期快照。
type SessionMeta struct {
	ID        string
	Status    SessionStatus
	CreatedAt time.Time
	UpdatedAt time.Time
}
