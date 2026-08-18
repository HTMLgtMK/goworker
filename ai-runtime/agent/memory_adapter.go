package agent

import (
	"context"

	"github.com/tinguo/goworker/ai-core/middlewares"
	"github.com/tinguo/goworker/ai-memory"
)

// memoryClientAdapter 把 ai-memory 的 *Client 适配成 ai-core/middlewares.MemoryClient。
// ai-core 零 memory 依赖，具体实现由 ai-runtime 注入。
type memoryClientAdapter struct {
	client *memory.Client
}

func (a *memoryClientAdapter) Search(ctx context.Context, query string, taskTopK, factTopK int) ([]middlewares.MemoryResult, error) {
	results, err := a.client.Search(ctx, query, taskTopK, factTopK)
	if err != nil {
		return nil, err
	}
	out := make([]middlewares.MemoryResult, 0, len(results))
	for _, r := range results {
		mr := middlewares.MemoryResult{Score: r.Score}
		switch {
		case r.Task != nil:
			mr.Task = &middlewares.MemoryTask{
				ID:        r.Task.ID,
				Title:     r.Task.Title,
				Summary:   r.Task.Summary,
				NextSteps: r.Task.NextSteps,
			}
		case r.Fact != nil:
			mr.Fact = &middlewares.MemoryFact{
				Topic:   r.Fact.Topic,
				Content: r.Fact.Content,
			}
		}
		out = append(out, mr)
	}
	return out, nil
}
