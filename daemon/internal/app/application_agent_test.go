//go:build !goworker_no_agent

package app

func wantAgentPlugin() bool { return true }
