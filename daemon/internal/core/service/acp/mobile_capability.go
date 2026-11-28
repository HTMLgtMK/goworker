package acp

import (
	"encoding/json"

	"github.com/tinguo/goworker/ai-acp/protocol"
)

func isMobileClient(capabilities protocol.ClientCapabilities) bool {
	capability, ok := capabilities.Get("mobile")
	if !ok {
		return false
	}
	data, err := json.Marshal(capability)
	if err != nil {
		return false
	}
	var mobile bool
	return json.Unmarshal(data, &mobile) == nil && mobile
}
