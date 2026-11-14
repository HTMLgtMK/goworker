package acp

import (
	"fmt"

	"github.com/tinguo/goworker/ai-acp/protocol"
	"github.com/tinguo/goworker/ai-runtime/hitl"
	xgwprotocol "github.com/tinguo/goworker/daemon/xgoworker/protocol"
)

const (
	XMethodCapabilities    = xgwprotocol.MethodCapabilities
	XMethodRunStart        = xgwprotocol.MethodRunStart
	XMethodRunGet          = xgwprotocol.MethodRunGet
	XMethodRunCancel       = xgwprotocol.MethodRunCancel
	XMethodApprovalRespond = xgwprotocol.MethodApprovalRespond
	XMethodSessionAttach   = xgwprotocol.MethodSessionAttach
	XMethodSessionDetach   = xgwprotocol.MethodSessionDetach
	XMethodSessionEvents   = xgwprotocol.NotificationSessionEvents
	XMethodSessionArchive  = xgwprotocol.MethodSessionArchive
	XMethodSessionModes    = xgwprotocol.MethodSessionModes

	XProviderMethodList   = xgwprotocol.ProviderMethodList
	XProviderMethodDetail = xgwprotocol.ProviderMethodDetail
	XProviderMethodUpsert = xgwprotocol.ProviderMethodUpsert
	XProviderMethodRemove = xgwprotocol.ProviderMethodRemove

	XAttachModeObserve = xgwprotocol.AttachModeObserve
	XAttachModeControl = xgwprotocol.AttachModeControl

	xgwErrorCode = -32000
)

type XRunStartParams = xgwprotocol.XRunStartParams
type XRunStartResult = xgwprotocol.XRunStartResult
type XRunGetParams = xgwprotocol.XRunGetParams
type XRunGetResult = xgwprotocol.XRunGetResult
type XRunCancelParams = xgwprotocol.XRunCancelParams
type XRunCancelResult = xgwprotocol.XRunCancelResult
type XApprovalDecision = xgwprotocol.XApprovalDecision
type XApprovalRespondParams = xgwprotocol.XApprovalRespondParams
type XApprovalRespondResult = xgwprotocol.XApprovalRespondResult
type XSessionAttachParams = xgwprotocol.XSessionAttachParams
type XSessionAttachRun = xgwprotocol.XSessionAttachRun
type XSessionAttachResult = xgwprotocol.XSessionAttachResult
type XSessionDetachParams = xgwprotocol.XSessionDetachParams
type XSessionDetachResult = xgwprotocol.XSessionDetachResult
type XSessionArchiveParams = xgwprotocol.XSessionArchiveParams
type XSessionArchiveResult = xgwprotocol.XSessionArchiveResult
type XSessionModesResult = xgwprotocol.XSessionModesResult
type SessionModeChoice = xgwprotocol.SessionModeChoice

func XProviderMethods() []string { return xgwprotocol.ProviderMethods() }

func xgwError(format string, args ...any) *protocol.RPCError {
	return &protocol.RPCError{Code: xgwErrorCode, Message: fmt.Sprintf(format, args...)}
}

func xgwNotAvailableError(method string) *protocol.RPCError {
	return xgwError("x-goworker: %s not supported: backend has no run control capability", method)
}

func xgwDecisionFromWire(d XApprovalDecision) (hitl.Decision, error) {
	t := hitl.DecisionType(d.Type)
	switch t {
	case hitl.DecisionApprove,
		hitl.DecisionApproveAlways,
		hitl.DecisionReject,
		hitl.DecisionEdit,
		hitl.DecisionRespond:
		return hitl.Decision{
			InterruptID: d.InterruptID,
			Type:        t,
			Command:     d.Command,
			Message:     d.Message,
		}, nil
	}
	return hitl.Decision{}, xgwError(
		"x-goworker: unknown decision type %q (want approve, approve_always, reject, edit or respond)", d.Type)
}

func xgwAttachMode(mode string) (string, error) {
	switch mode {
	case "", XAttachModeObserve, XAttachModeControl:
		return mode, nil
	}
	return "", xgwError("x-goworker: unknown attach mode %q (want %q or %q)", mode, XAttachModeObserve, XAttachModeControl)
}
