package mobile

import (
	"reflect"
	"testing"

	xgwprotocol "github.com/tinguo/goworker/daemon/xgoworker/protocol"
)

func TestXGWorkerMethodDirectory(t *testing.T) {
	methods := XGWorkerMethods()
	if !reflect.DeepEqual(methods, xgwprotocol.Methods()) {
		t.Fatalf("XGWorkerMethods() = %v, want protocol directory %v", methods, xgwprotocol.Methods())
	}
	methods[0] = "corrupted"
	if reflect.DeepEqual(methods, XGWorkerMethods()) {
		t.Fatal("XGWorkerMethods must return an independent slice")
	}
	if got := XGWorkerNotifications(); !reflect.DeepEqual(got, xgwprotocol.Notifications()) {
		t.Fatalf("XGWorkerNotifications() = %v, want protocol directory %v", got, xgwprotocol.Notifications())
	}
}
