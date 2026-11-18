// Package protocol contains the public x-goworker wire contract.
package protocol

import (
	"encoding/json"

	acpprotocol "github.com/tinguo/goworker/ai-acp/protocol"
)

const (
	MethodCapabilities         = "x-goworker/capabilities"
	MethodRunStart             = "x-goworker/run/start"
	MethodRunGet               = "x-goworker/run/get"
	MethodRunCancel            = "x-goworker/run/cancel"
	MethodApprovalRespond      = "x-goworker/approval/respond"
	MethodSessionAttach        = "x-goworker/session/attach"
	MethodSessionDetach        = "x-goworker/session/detach"
	MethodSessionArchive       = "x-goworker/session/archive"
	MethodSessionTitleGet      = "x-goworker/session/title/get"
	MethodSessionTitleSet      = "x-goworker/session/title/set"
	MethodSessionTitleGenerate = "x-goworker/session/title/generate"
	MethodSessionModes         = "x-goworker/session/modes"
	NotificationSessionEvents  = "x-goworker/session/events"

	ProviderMethodList       = "x-goworker/providers"
	ProviderMethodDetail     = "x-goworker/provider/detail"
	ProviderMethodCurrentGet = "x-goworker/provider/current/get"
	ProviderMethodCurrentSet = "x-goworker/provider/current/set"
	ProviderMethodUpsert     = "x-goworker/provider/upsert"
	ProviderMethodRemove     = "x-goworker/provider/remove"

	AttachModeObserve = "observe"
	AttachModeControl = "control"
)

// Version is the version advertised by the x-goworker initialize capability.
const Version = 1

// XGWorkerCapability advertises the supported x-goworker protocol version.
type XGWorkerCapability struct{}

var _ acpprotocol.Capability = XGWorkerCapability{}

func (XGWorkerCapability) Key() string { return "x-goworker" }

func (XGWorkerCapability) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Version int `json:"version"`
	}{Version: Version})
}

// CapabilitiesResult describes the extension surface available on one connection.
type CapabilitiesResult struct {
	Methods       []string `json:"methods"`
	Notifications []string `json:"notifications"`
}

type XRunStartParams struct {
	SessionID string `json:"sessionId"`
	Input     string `json:"input"`
	RequestID string `json:"requestId"`
}

type XRunStartResult struct {
	RunID string `json:"runId"`
	State string `json:"state"`
}

type XRunGetParams struct {
	SessionID string `json:"sessionId"`
	RunID     string `json:"runId"`
}

type XRunGetResult struct {
	State           string `json:"state"`
	Result          string `json:"result,omitempty"`
	LatestSeq       uint64 `json:"latestSeq"`
	ReplayIntegrity string `json:"replayIntegrity"`
}

type XRunCancelParams struct {
	SessionID string `json:"sessionId"`
	RunID     string `json:"runId"`
}

type XRunCancelResult struct {
	Accepted bool `json:"accepted"`
}

type XApprovalDecision struct {
	InterruptID string `json:"interrupt_id"`
	Type        string `json:"type"`
	Command     string `json:"command,omitempty"`
	Message     string `json:"message,omitempty"`
}

type XApprovalRespondParams struct {
	SessionID string            `json:"sessionId"`
	RequestID string            `json:"requestId"`
	Decision  XApprovalDecision `json:"decision"`
}

type XApprovalRespondResult struct {
	Accepted bool `json:"accepted"`
}

type XSessionAttachParams struct {
	SessionID string `json:"sessionId"`
	AfterSeq  uint64 `json:"afterSeq"`
	Mode      string `json:"mode,omitempty"`
}

type XSessionAttachRun struct {
	RunID string `json:"runId"`
	State string `json:"state"`
}

type XSessionAttachResult struct {
	CurrentRun      *XSessionAttachRun `json:"currentRun"`
	LatestSeq       uint64             `json:"latestSeq"`
	ReplayIntegrity string             `json:"replayIntegrity"`
	TookOver        bool               `json:"tookOver,omitempty"`
}

type XSessionDetachParams struct {
	SessionID string `json:"sessionId"`
	Mode      string `json:"mode,omitempty"`
}

type XSessionDetachResult struct {
	Detached bool `json:"detached"`
}

type XSessionArchiveParams struct {
	SessionID string `json:"sessionId"`
}

type XSessionArchiveResult struct {
	Archived bool `json:"archived"`
}

type XSessionTitleGetParams struct {
	SessionID string `json:"sessionId"`
}

type XSessionTitleGetResult struct {
	Title        string `json:"title"`
	TitleVersion uint64 `json:"titleVersion"`
}

type XSessionTitleSetParams struct {
	SessionID string `json:"sessionId"`
	Title     string `json:"title"`
}

type XSessionTitleSetResult struct {
	Updated bool `json:"updated"`
}

// XSessionTitleGenerateParams asks the daemon to create a title on demand for a session.
type XSessionTitleGenerateParams struct {
	SessionID string `json:"sessionId"`
}

// XSessionTitleGenerateResult carries the persisted title; source is always "manual".
type XSessionTitleGenerateResult struct {
	Title  string `json:"title"`
	Source string `json:"source"`
}

type XSessionModesResult struct {
	CurrentModeID  string              `json:"currentModeId"`
	AvailableModes []SessionModeChoice `json:"availableModes"`
}

type SessionModeChoice struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
}

// Methods returns a fresh list of all request names in this protocol version.
// The list is a directory; a connection advertises only methods with handlers.
func Methods() []string {
	return []string{
		MethodCapabilities,
		MethodRunStart,
		MethodRunGet,
		MethodRunCancel,
		MethodApprovalRespond,
		MethodSessionAttach,
		MethodSessionDetach,
		MethodSessionArchive,
		MethodSessionTitleGet,
		MethodSessionTitleSet,
		MethodSessionTitleGenerate,
		MethodSessionModes,
		ProviderMethodList,
		ProviderMethodDetail,
		ProviderMethodCurrentGet,
		ProviderMethodCurrentSet,
		ProviderMethodUpsert,
		ProviderMethodRemove,
	}
}

// Notifications returns a fresh list of notification names in this protocol version.
func Notifications() []string {
	return []string{NotificationSessionEvents}
}

// ProviderMethods returns the reserved provider request names. They are not
// necessarily available; clients must use x-goworker/capabilities to discover handlers.
func ProviderMethods() []string {
	return []string{
		ProviderMethodList,
		ProviderMethodDetail,
		ProviderMethodCurrentGet,
		ProviderMethodCurrentSet,
		ProviderMethodUpsert,
		ProviderMethodRemove,
	}
}
