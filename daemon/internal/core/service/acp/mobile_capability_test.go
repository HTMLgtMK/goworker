package acp

import "testing"

func TestMobileCapability(t *testing.T) {
	tests := []struct {
		name string
		caps string
		want bool
	}{
		{name: "enabled", caps: `{"mobile":true}`, want: true},
		{name: "disabled", caps: `{"mobile":false}`, want: false},
		{name: "missing", caps: `{}`, want: false},
		{name: "null", caps: `{"mobile":null}`, want: false},
		{name: "object", caps: `{"mobile":{}}`, want: false},
		{name: "string", caps: `{"mobile":"true"}`, want: false},
		{name: "other capability", caps: `{"x-device":{"tools":true}}`, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isMobileClient(decodeCaps(t, tt.caps)); got != tt.want {
				t.Fatalf("isMobileClient(%s) = %v, want %v", tt.caps, got, tt.want)
			}
		})
	}
}
