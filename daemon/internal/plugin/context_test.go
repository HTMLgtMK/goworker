package plugin

import (
	"context"
	"reflect"
	"testing"

	"github.com/tinguo/goworker/ai-runtime/hitl"
)

func TestNewContextPreservesInputs(t *testing.T) {
	base := context.WithValue(context.Background(), testContextKey{}, "value")
	args := []string{"alpha", "beta"}
	var wrote string
	writer := func(s string) { wrote = s }
	decide := func(req *hitl.InterruptRequest) hitl.Decision {
		return hitl.Decision{InterruptID: req.ID, Type: hitl.DecisionApprove}
	}

	ctx := NewContext(base, writer, decide, args)

	if ctx.Ctx != base {
		t.Fatalf("Ctx mismatch")
	}
	if !reflect.DeepEqual(ctx.Args, args) {
		t.Fatalf("Args = %#v, want %#v", ctx.Args, args)
	}
	ctx.Writer("hello")
	if wrote != "hello" {
		t.Fatalf("writer captured %q, want hello", wrote)
	}
	decision := ctx.Decide(&hitl.InterruptRequest{ID: "interrupt-1"})
	if decision.InterruptID != "interrupt-1" || decision.Type != hitl.DecisionApprove {
		t.Fatalf("decision = %#v", decision)
	}
	if ctx.Values == nil {
		t.Fatalf("Values is nil")
	}
	ctx.Values["k"] = "v"
	if ctx.Values["k"] != "v" {
		t.Fatalf("Values is not writable")
	}
}

func TestSessionIsValid(t *testing.T) {
	if (Session{}).IsValid() {
		t.Fatalf("empty session should be invalid")
	}
	if !(Session{UserID: "u1"}).IsValid() {
		t.Fatalf("session with user id should be valid")
	}
}

func TestRenderKindValues(t *testing.T) {
	cases := map[RenderKind]string{
		KindText:       "text",
		KindToolCall:   "tool_call",
		KindToolResult: "tool_result",
	}
	for got, want := range cases {
		if string(got) != want {
			t.Fatalf("RenderKind %q = %q, want %q", got, string(got), want)
		}
	}
}

type testContextKey struct{}
