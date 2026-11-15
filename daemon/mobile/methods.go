package mobile

import xgwprotocol "github.com/tinguo/goworker/daemon/xgoworker/protocol"

// XGWorkerMethods returns the protocol method directory for mobile clients.
// A connection's available subset is negotiated through x-goworker/capabilities.
func XGWorkerMethods() []string {
	return xgwprotocol.Methods()
}

// XGWorkerNotifications returns the protocol notification directory for mobile clients.
func XGWorkerNotifications() []string {
	return xgwprotocol.Notifications()
}
