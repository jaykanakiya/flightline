package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/google/uuid"

	"github.com/jaykanakiya/flightline-observability/internal/span"
)

const (
	minSteps        = 3
	maxSteps        = 15
	toolFailureRate = 0.08 // split roughly 70% error / 30% timeout
	llmFailureRate  = 0.02
	loopChance      = 0.12 // chance a run includes a stuck-in-a-loop pattern
	retryChance     = 0.6  // chance a failed tool call is retried once
)

var (
	agentNames = []string{"research-agent", "coder-agent", "support-agent"}
	toolNames  = []string{"web_search", "code_exec", "file_read", "calculator", "database_query"}

	errorMessages = []string{
		"rate limited by provider",
		"connection refused",
		"invalid response format",
		"context length exceeded",
		"upstream 500",
	}

	models = []modelPricing{
		{name: "gpt-4o-mini", inputPer1K: 0.00015, outputPer1K: 0.0006},
		{name: "gpt-4o", inputPer1K: 0.0025, outputPer1K: 0.01},
		{name: "claude-3-haiku", inputPer1K: 0.00025, outputPer1K: 0.00125},
	}
)

type modelPricing struct {
	name        string
	inputPer1K  float64
	outputPer1K float64
}

// generateRun builds one simulated agent run: a root agent_step span
// followed by a mix of llm_call and tool_call spans, with occasional
// failures, retries, and "stuck in a loop" patterns (the same tool called
// with the same input_hash several times in a row).
func generateRun(rng *rand.Rand) []span.Span {
	runID := uuid.NewString()
	agentName := agentNames[rng.Intn(len(agentNames))]
	targetSteps := minSteps + rng.Intn(maxSteps-minSteps+1)

	t := time.Now()
	var parentID string

	spans := []span.Span{newSpan(runID, agentName, span.StepAgentStep, &parentID, &t, 10+rng.Intn(20))}

	for len(spans) < targetSteps+1 {
		if rng.Float64() < loopChance {
			tool := toolNames[rng.Intn(len(toolNames))]
			hash := fmt.Sprintf("%s-%d", tool, rng.Intn(5))
			repeats := 4 + rng.Intn(3) // 4-6
			for i := 0; i < repeats; i++ {
				spans = append(spans, toolCallSpan(runID, agentName, &parentID, &t, tool, hash, rng))
			}
			continue
		}

		if rng.Float64() < 0.5 {
			spans = append(spans, llmCallSpan(runID, agentName, &parentID, &t, rng))
			continue
		}

		tool := toolNames[rng.Intn(len(toolNames))]
		hash := fmt.Sprintf("%s-%d", tool, rng.Intn(5))
		sp := toolCallSpan(runID, agentName, &parentID, &t, tool, hash, rng)
		spans = append(spans, sp)

		if sp.Status != span.StatusOK && rng.Float64() < retryChance {
			spans = append(spans, toolCallSpan(runID, agentName, &parentID, &t, tool, hash, rng))
		}
	}

	return spans
}

// newSpan advances the synthetic clock by latencyMS and chains parentID, so
// each run reads as a plausible, strictly increasing sequence of steps -
// independent of how fast the generator actually sends them.
func newSpan(runID, agentName string, stepType span.StepType, parentID *string, t *time.Time, latencyMS int) span.Span {
	start := *t
	end := start.Add(time.Duration(latencyMS) * time.Millisecond)
	*t = end

	spanID := uuid.NewString()
	sp := span.Span{
		TraceID:      runID,
		SpanID:       spanID,
		ParentSpanID: *parentID,
		AgentName:    agentName,
		RunID:        runID,
		StepType:     stepType,
		Status:       span.StatusOK,
		LatencyMS:    uint32(latencyMS),
		StartTime:    start,
		EndTime:      end,
	}
	*parentID = spanID
	return sp
}

func llmCallSpan(runID, agentName string, parentID *string, t *time.Time, rng *rand.Rand) span.Span {
	m := models[rng.Intn(len(models))]
	inputTokens := 100 + rng.Intn(1500)
	outputTokens := 20 + rng.Intn(500)
	latencyMS := 200 + outputTokens*3 + rng.Intn(300)

	sp := newSpan(runID, agentName, span.StepLLMCall, parentID, t, latencyMS)
	sp.Model = m.name
	sp.InputTokens = uint32(inputTokens)
	sp.OutputTokens = uint32(outputTokens)
	sp.CostUSD = float64(inputTokens)/1000*m.inputPer1K + float64(outputTokens)/1000*m.outputPer1K

	if rng.Float64() < llmFailureRate {
		sp.Status = span.StatusError
		sp.ErrorMessage = errorMessages[rng.Intn(len(errorMessages))]
	}

	return sp
}

func toolCallSpan(runID, agentName string, parentID *string, t *time.Time, tool, inputHash string, rng *rand.Rand) span.Span {
	latencyMS := 50 + rng.Intn(1500)

	sp := newSpan(runID, agentName, span.StepToolCall, parentID, t, latencyMS)
	sp.ToolName = tool
	sp.InputHash = inputHash

	roll := rng.Float64()
	switch {
	case roll < toolFailureRate*0.3:
		sp.Status = span.StatusTimeout
		sp.ErrorMessage = fmt.Sprintf("request timed out after %dms", latencyMS)
	case roll < toolFailureRate:
		sp.Status = span.StatusError
		sp.ErrorMessage = errorMessages[rng.Intn(len(errorMessages))]
	}

	return sp
}
