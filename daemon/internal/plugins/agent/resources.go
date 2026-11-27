package service

import (
	"log/slog"
	"strconv"

	"github.com/tinguo/goworker/ai-core/core"
	"github.com/tinguo/goworker/ai-memory"
	"github.com/tinguo/goworker/ai-runtime/mcp"
	"github.com/tinguo/goworker/ai-runtime/skills"
)

const resourceSnapshotMetadataKey = "goworker.agent.resource_snapshot"

type sessionResources struct {
	id          string
	skills      []skills.Skill
	instruction *memory.InstructionSet
	clients     map[string]mcp.Client
	mcpTools    []core.Tool
}

func (p *AgentPlugin) refreshResources() *sessionResources {
	resources := &sessionResources{clients: make(map[string]mcp.Client)}
	discovered, errs := skills.Discover(p.paths.SkillsUser, p.paths.SkillsProject)
	for _, err := range errs {
		slog.Warn("skill load error, skipping", "err", err)
	}
	resources.skills = append([]skills.Skill(nil), discovered...)
	if p.cfg.Memory.Enabled {
		resources.instruction = p.loadInstructions()
	}
	p.loadMCPInto(resources)
	p.publishResources(resources)
	return resources
}

func (p *AgentPlugin) publishResources(resources *sessionResources) {
	if resources == nil {
		return
	}
	p.resourceMu.Lock()
	if p.resourcesByID == nil {
		p.resourcesByID = make(map[string]*sessionResources)
	}
	if resources.id == "" {
		p.resourceSeq++
		resources.id = resourceSnapshotID(p.resourceSeq)
	}
	p.resourcesByID[resources.id] = resources
	p.currentResource = resources
	// Preserve the old diagnostic fields for existing in-package integrations. Runtime
	// construction and command reads use the immutable snapshot, never these mirrors.
	p.skills = append([]skills.Skill(nil), resources.skills...)
	p.mcpClients = cloneMCPClients(resources.clients)
	p.mcpTools = append([]core.Tool(nil), resources.mcpTools...)
	p.resourceMu.Unlock()
}

func resourceSnapshotID(seq uint64) string {
	return "resource-" + strconv.FormatUint(seq, 10)
}

func (p *AgentPlugin) currentResources() *sessionResources {
	p.resourceMu.RLock()
	resources := p.currentResource
	if resources != nil {
		p.resourceMu.RUnlock()
		return resources
	}
	// Keep direct unit-test/plugin construction compatible before Init publishes a generation.
	fallback := &sessionResources{
		skills:      append([]skills.Skill(nil), p.skills...),
		instruction: p.deps.Instruction,
		clients:     cloneMCPClients(p.mcpClients),
		mcpTools:    append([]core.Tool(nil), p.mcpTools...),
	}
	p.resourceMu.RUnlock()
	return fallback
}

func (p *AgentPlugin) associateResources(sessionID string, resources *sessionResources) {
	if sessionID == "" || resources == nil {
		return
	}
	p.resourceMu.Lock()
	if p.resourceBySession == nil {
		p.resourceBySession = make(map[string]*sessionResources)
	}
	p.resourceBySession[sessionID] = resources
	p.resourceMu.Unlock()
}

func (p *AgentPlugin) resourcesForSession(sessionID string) *sessionResources {
	p.resourceMu.RLock()
	resources := p.resourceBySession[sessionID]
	p.resourceMu.RUnlock()
	if resources != nil {
		return resources
	}
	return p.currentResources()
}

func (p *AgentPlugin) resourcesForID(id string) *sessionResources {
	p.resourceMu.RLock()
	resources := p.resourcesByID[id]
	if resources == nil {
		resources = p.currentResource
	}
	p.resourceMu.RUnlock()
	return resources
}

func cloneMCPClients(clients map[string]mcp.Client) map[string]mcp.Client {
	if len(clients) == 0 {
		return map[string]mcp.Client{}
	}
	cloned := make(map[string]mcp.Client, len(clients))
	for name, client := range clients {
		cloned[name] = client
	}
	return cloned
}

// Close is deliberately delayed until manager.Close has drained runs. It also ensures
// direct tool executions already holding a resource snapshot have finished in normal
// runtime paths before the client processes are reaped.
func (p *AgentPlugin) closeResourceClients() {
	p.resourceMu.Lock()
	clients := make([]mcp.Client, 0)
	if len(p.resourcesByID) == 0 {
		for _, client := range p.mcpClients {
			if client != nil {
				clients = append(clients, client)
			}
		}
	} else {
		for _, resources := range p.resourcesByID {
			for _, client := range resources.clients {
				if client != nil {
					clients = append(clients, client)
				}
			}
		}
	}
	p.resourcesByID = nil
	p.resourceBySession = nil
	p.currentResource = nil
	p.mcpClients = make(map[string]mcp.Client)
	p.mcpTools = nil
	p.skills = nil
	p.resourceMu.Unlock()
	for _, client := range clients {
		if err := client.Close(); err != nil {
			slog.Warn("mcp close error", "err", err)
		}
	}
}
