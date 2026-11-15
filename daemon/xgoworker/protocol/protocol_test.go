package protocol

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestXGWorkerCapabilityWire(t *testing.T) {
	data, err := json.Marshal(XGWorkerCapability{})
	if err != nil {
		t.Fatalf("MarshalJSON: %v", err)
	}
	if string(data) != `{"version":1}` {
		t.Fatalf("capability = %s, want {\"version\":1}", data)
	}
}

func TestMethodDirectoryIncludesReservedProviderMethods(t *testing.T) {
	methods := Methods()
	for _, want := range []string{
		MethodCapabilities,
		MethodRunStart,
		MethodRunGet,
		MethodRunCancel,
		MethodApprovalRespond,
		MethodSessionAttach,
		MethodSessionDetach,
		MethodSessionArchive,
		MethodSessionModes,
		ProviderMethodList,
		ProviderMethodDetail,
		ProviderMethodCurrentGet,
		ProviderMethodCurrentSet,
		ProviderMethodUpsert,
		ProviderMethodRemove,
	} {
		if !contains(methods, want) {
			t.Errorf("method directory lacks %q", want)
		}
	}
	methods[0] = "corrupted"
	if reflect.DeepEqual(methods, Methods()) {
		t.Fatal("Methods must return an independent slice")
	}
	if got := ProviderMethods(); !reflect.DeepEqual(got, []string{
		ProviderMethodList,
		ProviderMethodDetail,
		ProviderMethodCurrentGet,
		ProviderMethodCurrentSet,
		ProviderMethodUpsert,
		ProviderMethodRemove,
	}) {
		t.Fatalf("ProviderMethods() = %v", got)
	}
}

func TestNotificationDirectory(t *testing.T) {
	if got := Notifications(); !reflect.DeepEqual(got, []string{NotificationSessionEvents}) {
		t.Fatalf("Notifications() = %v", got)
	}
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
