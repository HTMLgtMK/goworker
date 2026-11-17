package acp

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"slices"
	"testing"

	aiacp "github.com/tinguo/goworker/ai-acp"
	xgwprotocol "github.com/tinguo/goworker/daemon/xgoworker/protocol"

	"github.com/tinguo/goworker/daemon/internal/core/model"
)

type titleReaderStub struct{}

func (titleReaderStub) GetSessionTitle(id string) (model.SessionTitleSnapshot, error) {
	if id != "older-session" {
		return model.SessionTitleSnapshot{}, errors.New("not found")
	}
	return model.SessionTitleSnapshot{Title: "persisted", TitleVersion: 42}, nil
}

func TestTitleGetCapabilityDiscovery(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		client, conn := net.Pipe()
		h := &FrontendHandler{}
		if enabled {
			h.titleReader = titleReaderStub{}
		}
		server, err := aiacp.NewServer(conn, h)
		if err != nil {
			t.Fatal(err)
		}
		registerXGWMethods(server, h)
		result, err := h.xgwCapabilities(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		methods := result.(xgwprotocol.CapabilitiesResult).Methods
		if slices.Contains(methods, XMethodSessionTitleGet) != enabled {
			t.Fatalf("enabled=%t methods=%v", enabled, methods)
		}
		client.Close()
		conn.Close()
	}
}

func TestTitleGetSnapshot(t *testing.T) {
	h := &FrontendHandler{titleReader: titleReaderStub{}}
	got, err := h.xgwSessionTitleGet(context.Background(), json.RawMessage(`{"sessionId":"older-session"}`))
	if err != nil {
		t.Fatal(err)
	}
	if got != (XSessionTitleGetResult{Title: "persisted", TitleVersion: 42}) {
		t.Fatalf("result = %+v", got)
	}
	for _, params := range []string{`{}`, `{"sessionId":"missing"}`, `{`} {
		if _, err := h.xgwSessionTitleGet(context.Background(), json.RawMessage(params)); err == nil {
			t.Fatalf("params %s unexpectedly accepted", params)
		}
	}
}
