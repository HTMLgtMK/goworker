package model

import "github.com/tinguo/goworker/ai-dispatch/protocol"

const SessionSourceCapability = "session.source"

// SessionSource is the consumer-facing capability exposed by the agent plugin.
type SessionSource interface {
	ListSessions() []protocol.SessionInfo
	CurrentSessionID() string
	SetSession(sessionID, cwd string)
	ReloadCurrentSession() error
	CurrentSessionHistory() []protocol.SessionUpdateBody
	ArchivedSessionHistory(sessionID string) ([]protocol.SessionUpdateBody, error)
	SessionModes() *protocol.SessionModeState
}
