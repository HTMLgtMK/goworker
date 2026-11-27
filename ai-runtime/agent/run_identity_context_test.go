package agent

import (
	"context"
	"testing"
)

func TestRunIdentityFromContext(t *testing.T) {
	tests := []struct {
		name      string
		ctx       context.Context
		want      RunIdentity
		wantValid bool
	}{
		{
			name: "nil context",
		},
		{
			name: "space-only session ID",
			ctx:  withRunIdentity(context.Background(), " ", "run-1"),
		},
		{
			name: "tab-only session ID",
			ctx:  withRunIdentity(context.Background(), "\t", "run-1"),
		},
		{
			name: "space-only run ID",
			ctx:  withRunIdentity(context.Background(), "session-1", " "),
		},
		{
			name: "tab-only run ID",
			ctx:  withRunIdentity(context.Background(), "session-1", "\t"),
		},
		{
			name:      "valid IDs are preserved",
			ctx:       withRunIdentity(context.Background(), "session-1", "run-1"),
			want:      RunIdentity{SessionID: "session-1", RunID: "run-1"},
			wantValid: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := RunIdentityFromContext(tt.ctx)
			if ok != tt.wantValid {
				t.Fatalf("RunIdentityFromContext() valid = %t, want %t", ok, tt.wantValid)
			}
			if got != tt.want {
				t.Errorf("RunIdentityFromContext() identity = %+v, want %+v", got, tt.want)
			}
		})
	}
}
