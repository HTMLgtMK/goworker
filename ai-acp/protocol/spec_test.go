package protocol

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestSessionUpdateBodyPreservesUnknownFields(t *testing.T) {
	input := []byte(`{"sessionUpdate":"tool_call","toolCallId":"call-1","custom":{"answer":42}}`)
	var body SessionUpdateBody
	if err := json.Unmarshal(input, &body); err != nil {
		t.Fatalf("UnmarshalJSON: %v", err)
	}

	body.Title = "renamed"
	output, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("MarshalJSON: %v", err)
	}

	var got, want map[string]any
	if err := json.Unmarshal(output, &got); err != nil {
		t.Fatalf("decode output: %v", err)
	}
	if err := json.Unmarshal(input, &want); err != nil {
		t.Fatalf("decode input: %v", err)
	}
	want["title"] = "renamed"
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("round-trip = %#v, want %#v", got, want)
	}
}

func TestSessionUpdateBodyManualRawPreservesWire(t *testing.T) {
	body := SessionUpdateBody{Raw: json.RawMessage(`{"sessionUpdate":"custom_update","extension":{"enabled":true}}`)}
	output, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("MarshalJSON: %v", err)
	}

	var got, want map[string]any
	if err := json.Unmarshal(output, &got); err != nil {
		t.Fatalf("decode output: %v", err)
	}
	if err := json.Unmarshal(body.Raw, &want); err != nil {
		t.Fatalf("decode input: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("raw output = %#v, want %#v", got, want)
	}
}

func TestCapabilitiesMarshalConcreteValues(t *testing.T) {
	caps, err := (Capabilities{}).With(LoadSessionCapability{})
	if err != nil {
		t.Fatalf("With load session: %v", err)
	}
	caps, err = caps.With(FileSystemCapability{ReadTextFile: true})
	if err != nil {
		t.Fatalf("With filesystem: %v", err)
	}

	data, err := json.Marshal(caps)
	if err != nil {
		t.Fatalf("Marshal capabilities: %v", err)
	}
	var got map[string]json.RawMessage
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("decode capabilities: %v", err)
	}
	if string(got["loadSession"]) != "true" {
		t.Fatalf("loadSession = %s, want true", got["loadSession"])
	}
	if string(got["fs"]) != `{"readTextFile":true,"writeTextFile":false}` {
		t.Fatalf("fs = %s", got["fs"])
	}
}

func TestCapabilitiesRejectDuplicateKeys(t *testing.T) {
	caps, err := (Capabilities{}).With(LoadSessionCapability{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := caps.With(LoadSessionCapability{}); err == nil {
		t.Fatal("duplicate capability key was accepted")
	}
}

func TestCapabilitiesRoundTripUnknownCapability(t *testing.T) {
	var caps AgentCapabilities
	if err := json.Unmarshal([]byte(`{"future":{"enabled":true}}`), &caps); err != nil {
		t.Fatalf("Unmarshal capabilities: %v", err)
	}
	if !caps.Has("future") {
		t.Fatal("unknown capability was not retained")
	}
	data, err := json.Marshal(caps)
	if err != nil {
		t.Fatalf("Marshal capabilities: %v", err)
	}
	if string(data) != `{"future":{"enabled":true}}` {
		t.Fatalf("round-trip = %s", data)
	}
}

func TestCapabilitiesRejectInvalidCapabilityKey(t *testing.T) {
	_, err := (Capabilities{}).With(rawCapability{key: " ", value: json.RawMessage("true")})
	if err == nil {
		t.Fatal("empty capability key was accepted")
	}
}
