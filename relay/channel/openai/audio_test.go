package openai

import (
	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/pkg/billingexpr"
	"github.com/QuantumNous/new-api/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestTranscriptionTokenModalities(t *testing.T) {
	for _, field := range []string{"input_token_details", "input_tokens_details", "prompt_tokens_details"} {
		t.Run(field, func(t *testing.T) {
			body := `{"text":"Hello","usage":{"input_tokens":100,"output_tokens":5,"total_tokens":105,"` + field + `":{"audio_tokens":80,"text_tokens":20}}}`
			ctx, _, resp, info := newImageTestContext(t, body, "application/json", false)
			info.SetEstimatePromptTokens(7)
			apiErr, usage := OpenaiSTTHandler(ctx, resp, info, "json")
			require.Nil(t, apiErr)
			assert.Equal(t, 100, usage.PromptTokens)
			assert.Equal(t, 5, usage.CompletionTokens)
			assert.Equal(t, 80, usage.PromptTokensDetails.AudioTokens)
			assert.Equal(t, 20, usage.PromptTokensDetails.TextTokens)
			assert.Equal(t, 5, usage.CompletionTokenDetails.TextTokens)
			expr := `tier("base",p * 2 + ai * 10 + c * 4)`
			params := service.BuildTieredTokenParams(usage, false, billingexpr.UsedVars(expr))
			cost, _, err := billingexpr.RunExpr(expr, params)
			require.NoError(t, err)
			assert.Equal(t, 860.0, cost)
			encoded, err := common.Marshal(usage)
			require.NoError(t, err)
			assert.Contains(t, string(encoded), `"audio_tokens":80`)
		})
	}
}
