package memory

import (
	"context"
	"slices"
	"testing"
)

// stubProvider 记录请求并返回固定响应，供抽取/检查点测试复用。实现本地 LLM 接口。
type stubProvider struct {
	lastReq *ChatRequest
	resp    string
	err     error
}

func (p *stubProvider) Model() string { return "stub-model" }
func (p *stubProvider) Chat(_ context.Context, req *ChatRequest) (*ChatResponse, error) {
	p.lastReq = req
	if p.err != nil {
		return nil, p.err
	}
	return &ChatResponse{Choices: []ResponseChoice{{Message: Message{Role: "assistant", Content: p.resp}}}}, nil
}

func TestApplyDecisions_FullFlow(t *testing.T) {
	s, _ := NewFileStore(t.TempDir(), 0)
	defer s.Close()
	// 已有两条，供 update/delete 引用
	s.AddFact(&Fact{ID: "f_old", Content: "old fact", Topic: "t", Source: "checkpoint:cp-0"})
	s.AddFact(&Fact{ID: "f_gone", Content: "to be deleted", Topic: "t", Source: "checkpoint:cp-0"})
	current, _ := s.ListFacts(10)

	decs := []Decision{
		{Action: DecisionAdd, Content: "new fact", Topic: "n"},
		{Action: DecisionAdd, Content: "new fact", Topic: "n"}, // 同批重复 → 跳过（store 静默去重，明细不虚报）
		{Action: DecisionAdd, Content: "old fact", Topic: "t"}, // 与已有重复 → 跳过
		{Action: DecisionAdd, Content: "   ", Topic: "t"},      // 空内容 → 跳过
		{Action: DecisionUpdate, ID: "f_old", Content: "refined fact", Topic: "t2"},
		{Action: DecisionUpdate, ID: "f_nonexistent", Content: "ghost"}, // 未知 id → 忽略
		{Action: DecisionDelete, ID: "f_gone"},
		{Action: DecisionNoop, Content: "nothing"},
	}
	applied, appliedFacts, deletedFacts, err := ApplyDecisions(s, decs, current, "checkpoint:cp-1")
	if err != nil {
		t.Fatalf("ApplyDecisions: %v", err)
	}
	if applied != 3 { // add + update + delete（同批重复 add 被跳过）
		t.Errorf("applied = %d, want 3", applied)
	}
	// 明细：add/update 只含真正落库的，delete 单独列出；noop/重复/空/未知 id 不出现
	if !slices.Equal(appliedFacts, []string{"n: new fact", "t2: refined fact"}) {
		t.Errorf("appliedFacts = %v, want [n: new fact t2: refined fact]", appliedFacts)
	}
	if !slices.Equal(deletedFacts, []string{"t: to be deleted"}) {
		t.Errorf("deletedFacts = %v, want [t: to be deleted]", deletedFacts)
	}
	facts, _ := s.ListFacts(10)
	if len(facts) != 2 { // f_old 更新后仍在，f_gone 删了，new fact 加入
		t.Fatalf("facts = %d, want 2 (%+v)", len(facts), facts)
	}
	var updated *Fact
	for i := range facts {
		if facts[i].ID == "f_old" {
			updated = &facts[i]
		}
	}
	if updated == nil || updated.Content != "refined fact" || updated.Topic != "t2" {
		t.Errorf("f_old not updated: %+v", facts)
	}
	// 新加的事实 Source 带上检查点前缀
	for _, f := range facts {
		if f.Content == "new fact" && f.Source != "checkpoint:cp-1" {
			t.Errorf("new fact Source = %q, want checkpoint:cp-1", f.Source)
		}
	}
}

// 复刻 wttr.in 重复案例的文本形态：short 是 long 的真子集（信息更少）。
const (
	overlapLong  = "wttr.in 支持中文（lang=zh）、JSON格式（format=j1）、指定天数（days=N）以及自定义格式化输出（如 format='%l:+%c+%t'）。城市名直接用拼音/英文（Changsha、Beijing）即可。中文支持时好时坏：2026-08-07 实测返回英文描述；2026-08-13 实测 lang=zh 正常返回中文。建议每次输出前核对语言。示例：curl -s 'wttr.in/Changsha?lang=zh'"
	overlapShort = "wttr.in 支持中文（lang=zh）、JSON格式（format=j1）、指定天数（days=N）以及自定义格式化输出（如 format='%l:+%c+%t'）。城市名直接用拼音/英文（Changsha、Beijing）即可。示例：curl -s 'wttr.in/Changsha?lang=zh'"
)

func TestFindOverlapFact(t *testing.T) {
	shortFacts := []Fact{{ID: "f_short", Content: overlapShort}}
	// 超集：new=long 覆盖 old=short（long 更全）→ newCovers=true
	hit, old, newCovers := findOverlapFact(shortFacts, overlapLong)
	if !hit || old == nil || old.ID != "f_short" {
		t.Fatalf("superset: hit=%v old=%v, want hit f_short", hit, old)
	}
	if !newCovers {
		t.Errorf("long as new should cover short, got newCovers=false")
	}
	// 子集：new=short 被 old=long 覆盖 → newCovers=false
	longFacts := []Fact{{ID: "f_long", Content: overlapLong}}
	hit, _, newCovers = findOverlapFact(longFacts, overlapShort)
	if !hit {
		t.Fatalf("subset: want hit")
	}
	if newCovers {
		t.Errorf("short as new should NOT cover long, got newCovers=true")
	}
	// 不同主题：无交集 → 不命中
	if hit, _, _ := findOverlapFact(longFacts, "字节面试准备清单位于桌面，包含 4 周冲刺计划。"); hit {
		t.Errorf("unrelated content should not hit, got hit")
	}
	// 等长互有独有 token：cov>=0.8 命中，但互不为超集 → newCovers=false（保守跳过，
	// 覆盖会永久丢掉旧 fact 的"长沙/阴天"独有信息）。回归：此前 len 比较在等长时恒 true。
	equalFacts := []Fact{{ID: "f_equal", Content: "wttr.in 支持 lang=zh 城市用拼音 示例 长沙 阴天"}}
	hit, _, newCovers = findOverlapFact(equalFacts, "wttr.in 支持 lang=zh 城市用拼音 示例 北京 晴天")
	if !hit {
		t.Fatalf("equal-length overlap: want hit")
	}
	if newCovers {
		t.Errorf("equal-length partial overlap should NOT overwrite (would drop 长沙/阴天), got newCovers=true")
	}
	// 空 newSet → 不命中
	if hit, _, _ := findOverlapFact(longFacts, "   "); hit {
		t.Errorf("empty content should not hit, got hit")
	}
}

func TestApplyDecisions_OverlapFallback(t *testing.T) {
	s, _ := NewFileStore(t.TempDir(), 0)
	defer s.Close()
	// 已有 fact 是 short（较短版本）
	s.AddFact(&Fact{ID: "f_weather", Content: overlapShort, Topic: "天气接口", Source: "checkpoint:cp-0"})
	current, _ := s.ListFacts(10)

	decs := []Decision{
		// 模型漏判：给了 add 而非 update。内容与 f_weather 高度重叠但更完整
		// （超集，逐字不同所以 duplicateContent 拦不住）→ 兜底转 update 旧 fact
		{Action: DecisionAdd, Content: overlapLong, Topic: "天气接口"},
		// 同批重复超集 add → added map 去重，不再二次 update（applied 不虚增）
		{Action: DecisionAdd, Content: overlapLong, Topic: "天气接口"},
		// 与 f_weather 逐字相同 → duplicateContent 跳过（原有逻辑，不走到兜底）
		{Action: DecisionAdd, Content: overlapShort, Topic: "天气接口"},
		// 完全不重叠 → 正常 add
		{Action: DecisionAdd, Content: "字节面试准备清单位于桌面，包含 4 周冲刺计划。", Topic: "面试"},
	}
	applied, appliedFacts, _, err := ApplyDecisions(s, decs, current, "checkpoint:cp-1")
	if err != nil {
		t.Fatalf("ApplyDecisions: %v", err)
	}
	if applied != 2 { // 1 次兜底 update + 1 次正常 add（逐字重复 + 同批重复超集都被去重）
		t.Errorf("applied = %d, want 2 (update + add)", applied)
	}
	if len(appliedFacts) != 2 {
		t.Errorf("appliedFacts = %v, want exactly 2 lines (no duplicate)", appliedFacts)
	}
	facts, _ := s.ListFacts(10)
	if len(facts) != 2 {
		t.Fatalf("facts = %d, want 2 (%+v)", len(facts), facts)
	}
	// f_weather 更新成 long，没有新增重复条
	var w *Fact
	for i := range facts {
		if facts[i].ID == "f_weather" {
			w = &facts[i]
		}
	}
	if w == nil || w.Content != overlapLong {
		t.Errorf("f_weather should be updated to long content: %+v", facts)
	}
	if w != nil && w.UpdatedAt.IsZero() {
		t.Errorf("f_weather UpdatedAt not refreshed after fallback update")
	}
	if w != nil && w.Topic != "天气接口" {
		t.Errorf("f_weather Topic = %q, want 天气接口", w.Topic)
	}
}

// 模型矛盾输出：同批先 delete 一条又用 add 覆盖它 —— 兜底 update 应被 deleted 拦截，
// 不虚报 applied、不落库空转的"更新"。
func TestApplyDecisions_DeleteThenOverlapAdd(t *testing.T) {
	s, _ := NewFileStore(t.TempDir(), 0)
	defer s.Close()
	s.AddFact(&Fact{ID: "f_gone", Content: overlapShort, Topic: "天气接口", Source: "checkpoint:cp-0"})
	current, _ := s.ListFacts(10)

	decs := []Decision{
		{Action: DecisionDelete, ID: "f_gone"},
		// add 与 f_gone 高度重叠（超集）—— 本批已删，update 是空转，应跳过
		{Action: DecisionAdd, Content: overlapLong, Topic: "天气接口"},
	}
	applied, _, deletedFacts, err := ApplyDecisions(s, decs, current, "checkpoint:cp-1")
	if err != nil {
		t.Fatalf("ApplyDecisions: %v", err)
	}
	if applied != 1 {
		t.Errorf("applied = %d, want 1 (delete only; overlapping add skipped)", applied)
	}
	if len(deletedFacts) != 1 {
		t.Errorf("deletedFacts = %v, want 1 line", deletedFacts)
	}
	facts, _ := s.ListFacts(10)
	if len(facts) != 0 {
		t.Errorf("facts = %d, want 0 (deleted and not resurrected)", len(facts))
	}
}
