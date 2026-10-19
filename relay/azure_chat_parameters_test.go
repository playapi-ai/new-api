package relay

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/relay/channel/openai"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	relayconstant "github.com/QuantumNous/new-api/relay/constant"
	"github.com/QuantumNous/new-api/relay/helper"
	"github.com/QuantumNous/new-api/relaykit/dto"
	"github.com/QuantumNous/new-api/relaykit/types"
	"github.com/QuantumNous/new-api/service"
	"github.com/QuantumNous/new-api/setting/model_setting"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAzureChatSamplingOutbound(t *testing.T) {
	settings := model_setting.GetGlobalSettings()
	previous := *settings
	settings.PassThroughRequestEnabled = false
	settings.ChatCompletionsToResponsesPolicy.Enabled = false
	t.Cleanup(func() { *settings = previous })
	service.InitHttpClient()

	const probabilities = `{"logprobs":true,"top_logprobs":2}`
	for _, tc := range []struct {
		name, model, effort, params, want, mapped, errorParam string
		channelType                                           int
		stream, passThrough, legacyRequest                    bool
		override                                              map[string]any
	}{
		{name: "5.5 probabilities", model: "gpt-5.5", effort: "none", params: probabilities},
		{name: "5.6 luna probabilities", model: "gpt-5.6-luna", effort: "none", params: probabilities},
		{name: "5.6 sol probabilities", model: "gpt-5.6-sol", effort: "none", params: probabilities},
		{name: "5.6 terra probabilities", model: "gpt-5.6-terra", effort: "none", params: probabilities},
		{name: "astra unsupported probabilities reach provider", model: "gpt-6-astra", effort: "low", params: probabilities, errorParam: "logprobs"},
		{name: "luna invalid temperature", model: "gpt-5.6-luna", effort: "none", params: `{"temperature":-0.1}`, errorParam: "temperature"},
		{name: "luna invalid top_p", model: "gpt-5.6-luna", effort: "none", params: `{"top_p":1.1}`, errorParam: "top_p"},
		{name: "luna invalid top_logprobs", model: "gpt-5.6-luna", effort: "none", params: `{"logprobs":true,"top_logprobs":21}`, errorParam: "top_logprobs"},
		{name: "astra invalid temperature", model: "gpt-6-astra", effort: "low", params: `{"temperature":-0.1}`, errorParam: "temperature"},
		{name: "astra invalid top_p", model: "gpt-6-astra", effort: "low", params: `{"top_p":1.1}`, errorParam: "top_p"},
		{name: "astra invalid top_logprobs", model: "gpt-6-astra", effort: "low", params: `{"logprobs":true,"top_logprobs":21}`, errorParam: "logprobs"},
		{name: "streaming rejection", model: "gpt-6-astra", effort: "low", params: probabilities, stream: true, errorParam: "logprobs"},
		{name: "explicit zero and false", model: "gpt-5.6-luna", effort: "none", params: `{"temperature":0,"top_p":0,"logprobs":false,"top_logprobs":0}`},
		{name: "upper boundaries", model: "gpt-5.6-luna", effort: "none", params: `{"temperature":2,"top_p":1,"logprobs":true,"top_logprobs":20}`},
		{name: "omitted fields and effort", model: "gpt-5.6-luna", params: `{}`},
		{name: "omitted effort is not inferred", model: "gpt-5.6-luna", params: probabilities},
		{name: "reasoning effort is not downgraded", model: "gpt-5.6-luna", effort: "high", params: probabilities},
		{name: "mapped into affected model", model: "deployment-alias", mapped: "gpt-5.6-sol", effort: "none", params: probabilities},
		{name: "mapped out of affected model", model: "gpt-5.6-sol", mapped: "gpt-5", effort: "none", params: probabilities, want: `{}`},
		{name: "administrator override remains last", model: "gpt-5.6-luna", effort: "none", params: probabilities, override: map[string]any{"top_logprobs": 1}, want: `{"logprobs":true,"top_logprobs":1}`},
		{name: "body pass through unchanged", model: "gpt-5.6-luna", effort: "none", params: probabilities, passThrough: true},
		{name: "legacy token and system role compatibility", model: "gpt-5.6-luna", effort: "none", params: probabilities, legacyRequest: true},
		{name: "openai channel retains compatibility filtering", channelType: constant.ChannelTypeOpenAI, model: "gpt-5.6-luna", effort: "none", params: probabilities, want: `{}`},
		{name: "openrouter channel retains compatibility filtering", channelType: constant.ChannelTypeOpenRouter, model: "gpt-5.6-luna", effort: "none", params: probabilities, want: `{}`},
		{name: "azure 5.4 reasoning retains filtering", model: "gpt-5.4", effort: "high", params: probabilities, want: `{}`},
		{name: "azure 6 luna remains supported", model: "gpt-6-luna", effort: "none", params: probabilities},
		{name: "unverified named variant retains filtering", model: "gpt-5.6-luna-pro", effort: "none", params: probabilities, want: `{}`},
		{name: "unverified snapshot retains filtering", model: "gpt-5.6-luna-2026-09-01", effort: "none", params: probabilities, want: `{}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var input map[string]any
			require.NoError(t, common.UnmarshalJsonStr(tc.params, &input))
			input["model"] = tc.model
			input["messages"] = []any{map[string]any{"role": "user", "content": "A"}}
			input["max_completion_tokens"] = 20
			if tc.legacyRequest {
				delete(input, "max_completion_tokens")
				input["max_tokens"] = 20
				input["messages"] = []any{map[string]any{"role": "system", "content": "A"}}
			}
			if tc.effort != "" {
				input["reasoning_effort"] = tc.effort
			}
			if tc.stream {
				input["stream"] = true
			}
			body, err := common.Marshal(input)
			require.NoError(t, err)
			// Always reject after capture to stop before settlement. This fixture
			// tests forwarding and error propagation, not provider capabilities.
			errorBody, err := common.Marshal(map[string]any{"error": types.OpenAIError{
				Message: "upstream rejected request", Type: "invalid_request_error", Param: tc.errorParam, Code: "invalid_parameter",
			}})
			require.NoError(t, err)
			received := make(chan []byte, 1)
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				captured, readErr := io.ReadAll(r.Body)
				if !assert.NoError(t, readErr) {
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				received <- captured
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write(errorBody)
			}))
			t.Cleanup(upstream.Close)
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
			c.Request.Header.Set("Content-Type", "application/json")
			channelType := tc.channelType
			if channelType == 0 {
				channelType = constant.ChannelTypeAzure
			}
			common.SetContextKey(c, constant.ContextKeyChannelType, channelType)
			common.SetContextKey(c, constant.ContextKeyChannelBaseUrl, upstream.URL)
			common.SetContextKey(c, constant.ContextKeyOriginalModel, tc.model)
			common.SetContextKey(c, constant.ContextKeyChannelSetting, dto.ChannelSettings{PassThroughBodyEnabled: tc.passThrough})
			common.SetContextKey(c, constant.ContextKeyChannelParamOverride, tc.override)
			if tc.mapped != "" {
				mapping, marshalErr := common.Marshal(map[string]string{tc.model: tc.mapped})
				require.NoError(t, marshalErr)
				c.Set("model_mapping", string(mapping))
			}
			request, err := helper.GetAndValidateTextRequest(c, relayconstant.RelayModeChatCompletions)
			require.NoError(t, err)
			info := &relaycommon.RelayInfo{Request: request, OriginModelName: tc.model, RelayFormat: types.RelayFormatOpenAI,
				RelayMode: relayconstant.RelayModeChatCompletions, RequestURLPath: c.Request.URL.Path, IsStream: tc.stream, DisablePing: true}
			apiErr := TextHelper(c, info)
			require.NotNil(t, apiErr)
			assert.Equal(t, http.StatusBadRequest, apiErr.StatusCode)
			assert.Equal(t, "invalid_request_error", apiErr.ToOpenAIError().Type)
			assert.Equal(t, tc.errorParam, apiErr.ToOpenAIError().Param)
			assert.Equal(t, "invalid_parameter", apiErr.ToOpenAIError().Code)
			require.Len(t, received, 1)
			var got map[string]any
			require.NoError(t, common.Unmarshal(<-received, &got))
			wantModel := tc.model
			if tc.mapped != "" {
				wantModel = tc.mapped
			}
			assert.Equal(t, wantModel, got["model"])
			assert.Equal(t, float64(20), got["max_completion_tokens"])
			assert.NotContains(t, got, "max_tokens")
			if tc.legacyRequest {
				assert.Equal(t, []any{map[string]any{"role": "developer", "content": "A"}}, got["messages"])
			}
			if tc.stream {
				assert.Equal(t, true, got["stream"])
			}
			if channelType == constant.ChannelTypeOpenRouter {
				assert.NotContains(t, got, "reasoning_effort")
				assert.Equal(t, map[string]any{"enabled": false}, got["reasoning"])
			} else if tc.effort == "" {
				assert.NotContains(t, got, "reasoning_effort")
			} else {
				assert.Equal(t, tc.effort, got["reasoning_effort"])
			}
			want := tc.want
			if want == "" {
				want = tc.params
			}
			var wantParams map[string]any
			require.NoError(t, common.UnmarshalJsonStr(want, &wantParams))
			for _, key := range []string{"temperature", "top_p", "logprobs", "top_logprobs"} {
				value, exists := wantParams[key]
				if exists {
					assert.Contains(t, got, key)
					assert.Equal(t, value, got[key], key)
				} else {
					assert.NotContains(t, got, key)
				}
			}
		})
	}
}

func TestAzureChatSamplingScope(t *testing.T) {
	for _, tc := range []struct {
		name   string
		format types.RelayFormat
		mode   int
		chain  []types.RelayFormat
	}{
		{name: "claude conversion", format: types.RelayFormatClaude, mode: relayconstant.RelayModeChatCompletions, chain: []types.RelayFormat{types.RelayFormatClaude, types.RelayFormatOpenAI}},
		{name: "gemini conversion", format: types.RelayFormatGemini, mode: relayconstant.RelayModeChatCompletions, chain: []types.RelayFormat{types.RelayFormatGemini, types.RelayFormatOpenAI}},
		{name: "responses conversion", format: types.RelayFormatOpenAIResponses, mode: relayconstant.RelayModeChatCompletions, chain: []types.RelayFormat{types.RelayFormatOpenAIResponses, types.RelayFormatOpenAI}},
		{name: "chat converted through responses", format: types.RelayFormatOpenAI, mode: relayconstant.RelayModeChatCompletions, chain: []types.RelayFormat{types.RelayFormatOpenAI, types.RelayFormatOpenAIResponses, types.RelayFormatOpenAI}},
		{name: "legacy completions", format: types.RelayFormatOpenAI, mode: relayconstant.RelayModeCompletions},
		{name: "unspecified source format", mode: relayconstant.RelayModeChatCompletions},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			info := &relaycommon.RelayInfo{OriginModelName: "gpt-5.6-luna", RelayFormat: tc.format, RelayMode: tc.mode,
				ChannelMeta:            &relaycommon.ChannelMeta{ChannelType: constant.ChannelTypeAzure, UpstreamModelName: "gpt-5.6-luna"},
				RequestConversionChain: tc.chain}
			request := &dto.GeneralOpenAIRequest{Model: "gpt-5.6-luna", ReasoningEffort: "none",
				Temperature: common.GetPointer(0.5), TopP: common.GetPointer(0.8), LogProbs: common.GetPointer(true), TopLogProbs: common.GetPointer(2)}
			converted, err := (&openai.Adaptor{}).ConvertOpenAIRequest(c, info, request)
			require.NoError(t, err)
			got, ok := converted.(*dto.GeneralOpenAIRequest)
			require.True(t, ok)
			assert.Nil(t, got.Temperature)
			assert.Nil(t, got.TopP)
			assert.Nil(t, got.LogProbs)
			assert.Nil(t, got.TopLogProbs)
		})
	}
}

func TestAzureChatLogprobsResponse(t *testing.T) {
	const body = `{"id":"chatcmpl-test","object":"chat.completion","model":"gpt-5.6-luna","choices":[{"index":0,"message":{"role":"assistant","content":"A"},"logprobs":{"content":[{"token":"A","logprob":-0.1,"bytes":[65],"top_logprobs":[{"token":"A","logprob":-0.1,"bytes":[65]}]}]},"finish_reason":"stop"}],"usage":{"prompt_tokens":2,"completion_tokens":1,"total_tokens":3}}`
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	info := &relaycommon.RelayInfo{RelayFormat: types.RelayFormatOpenAI,
		ChannelMeta: &relaycommon.ChannelMeta{ChannelType: constant.ChannelTypeAzure, UpstreamModelName: "gpt-5.6-luna"}}
	response := &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(body))}
	_, apiErr := openai.OpenaiHandler(c, info, response)
	require.Nil(t, apiErr)
	assert.Equal(t, http.StatusOK, recorder.Code)
	assert.JSONEq(t, body, recorder.Body.String())
}
