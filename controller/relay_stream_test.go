package controller

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/pkg/billingexpr"
	"github.com/QuantumNous/new-api/relay/channel"
	"github.com/QuantumNous/new-api/relay/channel/claude"
	"github.com/QuantumNous/new-api/relay/channel/openai"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/QuantumNous/new-api/relay/helper"
	"github.com/QuantumNous/new-api/relaykit/dto"
	"github.com/QuantumNous/new-api/relaykit/types"
	"github.com/QuantumNous/new-api/service"
	"github.com/QuantumNous/new-api/setting/operation_setting"
	"github.com/QuantumNous/new-api/setting/system_setting"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRelayKeepAliveDuringImagePreparation(t *testing.T) {
	settings := operation_setting.GetGeneralSetting()
	previousEnabled, previousInterval := settings.PingIntervalEnabled, settings.PingIntervalSeconds
	previousCount, previousMedia := constant.CountToken, constant.GetMediaToken
	fetchSettings := system_setting.GetFetchSetting()
	previousFetchSettings := *fetchSettings
	fetchSettings.AllowPrivateIp = true
	fetchSettings.AllowedPorts = []string{"1-65535"}
	settings.PingIntervalEnabled, settings.PingIntervalSeconds = true, 1
	constant.CountToken, constant.GetMediaToken = true, true
	t.Cleanup(func() {
		settings.PingIntervalEnabled, settings.PingIntervalSeconds = previousEnabled, previousInterval
		constant.CountToken, constant.GetMediaToken = previousCount, previousMedia
		*fetchSettings = previousFetchSettings
	})
	service.InitHttpClient()

	imageRequested := make(chan struct{})
	releaseImage := make(chan struct{})
	var releaseOnce sync.Once
	imageServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(imageRequested)
		<-releaseImage
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer imageServer.Close()
	engine := gin.New()
	engine.POST("/v1/messages", func(c *gin.Context) {
		common.SetContextKey(c, constant.ContextKeyOriginalModel, "claude-sonnet-4-20250514")
		Relay(c, types.RelayFormatClaude)
	})
	gateway := httptest.NewServer(engine)
	defer gateway.Close()
	defer releaseOnce.Do(func() { close(releaseImage) })

	body := `{"model":"claude-sonnet-4-20250514","max_tokens":16,"stream":true,"messages":[{"role":"user","content":[{"type":"image","source":{"type":"url","url":"` + imageServer.URL + `"}}]}]}`
	client := &http.Client{Timeout: 4 * time.Second}
	response, err := client.Post(gateway.URL+"/v1/messages", "application/json", strings.NewReader(body))
	require.NoError(t, err, "the gateway must send keepalive while its image download is blocked")
	defer response.Body.Close()
	require.Equal(t, http.StatusOK, response.StatusCode)
	require.Equal(t, "text/event-stream", response.Header.Get("Content-Type"))
	reader := bufio.NewReader(response.Body)
	for range 2 {
		line, readErr := reader.ReadString('\n')
		require.NoError(t, readErr)
		require.Equal(t, ": PING\n", line)
		line, readErr = reader.ReadString('\n')
		require.NoError(t, readErr)
		require.Equal(t, "\n", line)
	}
	select {
	case <-imageRequested:
	default:
		t.Fatal("the request never reached image preparation")
	}
	releaseOnce.Do(func() { close(releaseImage) })
	remaining, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Contains(t, string(remaining), "event: error\n")
	require.Contains(t, string(remaining), `"type":"error"`)
	require.Contains(t, string(remaining), "data: ")
}

func TestRelayKeepAliveAcrossUpstreamRetries(t *testing.T) {
	settings := operation_setting.GetGeneralSetting()
	previousEnabled, previousInterval := settings.PingIntervalEnabled, settings.PingIntervalSeconds
	settings.PingIntervalEnabled, settings.PingIntervalSeconds = true, 1
	t.Cleanup(func() {
		settings.PingIntervalEnabled, settings.PingIntervalSeconds = previousEnabled, previousInterval
	})
	service.InitHttpClient()
	var attempts atomic.Int32
	retryStarted, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		close(retryStarted)
		select {
		case <-release:
			_, _ = io.WriteString(w, "data: ok\n\n")
		case <-r.Context().Done():
		}
	}))
	defer upstream.Close()
	engine := gin.New()
	engine.POST("/v1/messages", func(c *gin.Context) {
		stop := helper.StartStreamKeepAlive(c, 10*time.Millisecond)
		defer stop()
		if !assert.NoError(t, http.NewResponseController(c.Writer).SetWriteDeadline(time.Now().Add(5*time.Second))) {
			return
		}
		info := &relaycommon.RelayInfo{IsStream: true, ChannelMeta: &relaycommon.ChannelMeta{}}
		for range 2 {
			request, err := http.NewRequestWithContext(c.Request.Context(), http.MethodPost, upstream.URL, strings.NewReader("{}"))
			if !assert.NoError(t, err) {
				return
			}
			response, err := channel.DoRequest(c, request, info)
			if !assert.NoError(t, err) {
				return
			}
			if response.StatusCode == http.StatusTooManyRequests {
				response.Body.Close()
				continue
			}
			_, _ = io.Copy(c.Writer, response.Body)
			response.Body.Close()
		}
	})
	gateway := httptest.NewServer(engine)
	defer gateway.Close()
	defer releaseOnce.Do(func() { close(release) })
	client := &http.Client{Timeout: 4 * time.Second}
	response, err := client.Post(gateway.URL+"/v1/messages", "application/json", strings.NewReader("{}"))
	require.NoError(t, err)
	defer response.Body.Close()
	select {
	case <-retryStarted:
	case <-time.After(time.Second):
		t.Fatal("the upstream retry did not start")
	}
	reader := bufio.NewReader(response.Body)
	for range 2 {
		line, readErr := reader.ReadString('\n')
		require.NoError(t, readErr)
		require.Equal(t, ": PING\n", line)
		line, readErr = reader.ReadString('\n')
		require.NoError(t, readErr)
		require.Equal(t, "\n", line)
	}
	releaseOnce.Do(func() { close(release) })
	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Contains(t, string(body), "data: ok\n\n")
	require.Equal(t, int32(2), attempts.Load())
}

func TestRelayKeepAlivePreservesValidationErrors(t *testing.T) {
	settings := operation_setting.GetGeneralSetting()
	previousEnabled := settings.PingIntervalEnabled
	settings.PingIntervalEnabled = true
	t.Cleanup(func() { settings.PingIntervalEnabled = previousEnabled })
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"model":"claude-sonnet-4-20250514","stream":true}`))
	c.Request.Header.Set("Content-Type", "application/json")
	Relay(c, types.RelayFormatClaude)
	require.Equal(t, http.StatusBadRequest, recorder.Code)
	require.Contains(t, recorder.Header().Get("Content-Type"), "application/json")
	require.NotContains(t, recorder.Body.String(), ": PING")
}

func TestStreamKeepAlivePreservesFastHTTPFailures(t *testing.T) {
	settings := operation_setting.GetGeneralSetting()
	previousEnabled, previousInterval := settings.PingIntervalEnabled, settings.PingIntervalSeconds
	settings.PingIntervalEnabled, settings.PingIntervalSeconds = true, 60
	t.Cleanup(func() {
		settings.PingIntervalEnabled, settings.PingIntervalSeconds = previousEnabled, previousInterval
	})
	service.InitHttpClient()
	for _, status := range []int{http.StatusUnauthorized, http.StatusPaymentRequired, http.StatusForbidden, http.StatusTooManyRequests} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(status)
				_, _ = io.WriteString(w, `{"error":"request rejected"}`)
			}))
			defer upstream.Close()
			engine := gin.New()
			engine.POST("/v1/messages", func(c *gin.Context) {
				stop := helper.StartStreamKeepAlive(c, time.Minute)
				defer stop()
				request, err := http.NewRequestWithContext(c.Request.Context(), http.MethodPost, upstream.URL, strings.NewReader("{}"))
				if !assert.NoError(t, err) {
					return
				}
				info := &relaycommon.RelayInfo{IsStream: true, ChannelMeta: &relaycommon.ChannelMeta{}}
				response, err := channel.DoRequest(c, request, info)
				if !assert.NoError(t, err) {
					return
				}
				defer response.Body.Close()
				body, err := io.ReadAll(response.Body)
				if assert.NoError(t, err) {
					c.Header("Content-Type", "application/json")
					c.Data(response.StatusCode, "application/json", body)
				}
			})
			gateway := httptest.NewServer(engine)
			defer gateway.Close()
			client := &http.Client{Timeout: 4 * time.Second}
			response, err := client.Post(gateway.URL+"/v1/messages", "application/json", strings.NewReader("{}"))
			require.NoError(t, err)
			defer response.Body.Close()
			body, err := io.ReadAll(response.Body)
			require.NoError(t, err)
			require.Equal(t, status, response.StatusCode)
			require.Equal(t, "application/json", response.Header.Get("Content-Type"))
			require.JSONEq(t, `{"error":"request rejected"}`, string(body))
		})
	}
}

func TestRelayKeepAliveHonorsStreamSettings(t *testing.T) {
	settings := operation_setting.GetGeneralSetting()
	previousEnabled := settings.PingIntervalEnabled
	previousCount, previousMedia, previousNonStreamMedia := constant.CountToken, constant.GetMediaToken, constant.GetMediaTokenNotStream
	constant.CountToken, constant.GetMediaToken, constant.GetMediaTokenNotStream = true, true, true
	t.Cleanup(func() {
		settings.PingIntervalEnabled = previousEnabled
		constant.CountToken, constant.GetMediaToken, constant.GetMediaTokenNotStream = previousCount, previousMedia, previousNonStreamMedia
	})
	for _, test := range []struct {
		name    string
		enabled bool
		stream  bool
	}{
		{name: "ping disabled", enabled: false, stream: true},
		{name: "non-streaming", enabled: true, stream: false},
		{name: "fast streaming failure", enabled: true, stream: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			settings.PingIntervalEnabled = test.enabled
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			body := `{"model":"claude-sonnet-4-20250514","max_tokens":16,"stream":` + strconv.FormatBool(test.stream) + `,"messages":[{"role":"user","content":[{"type":"image","source":{"type":"base64","media_type":"image/png","data":"!"}}]}]}`
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(body))
			c.Request.Header.Set("Content-Type", "application/json")
			common.SetContextKey(c, constant.ContextKeyOriginalModel, "claude-sonnet-4-20250514")
			Relay(c, types.RelayFormatClaude)
			require.Equal(t, http.StatusInternalServerError, recorder.Code)
			require.Contains(t, recorder.Header().Get("Content-Type"), "application/json")
			require.NotContains(t, recorder.Body.String(), ": PING")
			var response map[string]any
			require.NoError(t, common.Unmarshal(recorder.Body.Bytes(), &response))
			require.Equal(t, "error", response["type"])
		})
	}
}

func TestRelayStreamErrorAfterKeepAlive(t *testing.T) {
	for _, format := range []types.RelayFormat{types.RelayFormatClaude, types.RelayFormatOpenAI} {
		t.Run(string(format), func(t *testing.T) {
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodPost, "/stream", nil)
			helper.SetEventStreamHeaders(c)
			require.NoError(t, helper.PingData(c))
			common.SetContextKey(c, constant.ContextKeyChannelBaseUrl, "https://provider.example")
			common.SetContextKey(c, constant.ContextKeyUpstreamRequestOrigin, "https://provider.example")
			apiError := types.NewErrorWithStatusCode(errors.New("upstream rejected input at https://provider.example/v1/messages"), types.ErrorCodeBadResponseStatusCode, http.StatusBadRequest)
			writeRelayStreamError(c, format, apiError)
			require.Equal(t, http.StatusOK, recorder.Code)
			require.Equal(t, "text/event-stream", recorder.Header().Get("Content-Type"))
			require.Contains(t, recorder.Body.String(), "upstream rejected input")
			require.NotContains(t, recorder.Body.String(), "provider.example")
			status, ok := common.GetContextKeyType[*relaycommon.StreamStatus](c, constant.ContextKeyResponseStreamStatus)
			require.True(t, ok)
			require.True(t, status.ResponseFailed(), "stream errors must not consume the successful-request rate limit")
			require.Equal(t, http.StatusBadRequest, status.OutcomeSnapshot().ErrorStatus)
			if format == types.RelayFormatClaude {
				require.Contains(t, recorder.Body.String(), "event: error\n")
			} else {
				require.Contains(t, recorder.Body.String(), "data: [DONE]")
			}
		})
	}
}

func TestStreamKeepAliveCancellation(t *testing.T) {
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	requestContext, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.Request = httptest.NewRequest(http.MethodPost, "/stream", nil).WithContext(requestContext)
	stop := helper.StartStreamKeepAlive(c, time.Hour)
	cancel()
	stopped := make(chan struct{})
	go func() {
		stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("keepalive did not stop after client cancellation")
	}
	_, err := c.Writer.WriteString("data: done\n\n")
	require.NoError(t, err)
	require.True(t, strings.HasSuffix(recorder.Body.String(), "data: done\n\n"))
}

// Exercise a real upstream and HTTP client through reservation, streaming,
// settlement/refund, and log recording, including downstream cancellation.
func TestOpenAIStreamAccounting(t *testing.T) {
	const content = `{"id":"chat-test","model":"billing-test","choices":[{"index":0,"delta":{"content":"hello"}}]}`
	const finish = `{"id":"chat-test","model":"billing-test","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`
	const usage = `{"choices":[],"usage":{"prompt_tokens":1000,"completion_tokens":20,"total_tokens":1020,"prompt_tokens_details":{"cached_tokens":900}}}`
	const empty = `{"choices":[]}`
	frame := func(data string) string { return "data: " + data + "\n\n" }
	for _, tc := range []struct {
		name, body  string
		cut, cancel bool
		want        int
		tokens      bool
	}{
		{name: "empty"},
		{name: "done only", body: frame("[DONE]")},
		{name: "error only", body: frame(`{"error":{"message":"upstream failed","type":"server_error"}}`) + frame("[DONE]")},
		{name: "error after output", body: frame(content) + frame(`{"error":{"message":"upstream failed"}}`)},
		{name: "early usage", body: frame(content) + frame(usage) + frame(empty) + frame(finish) + frame("[DONE]"), want: 185, tokens: true},
		{name: "repeated usage", body: frame(content) + frame(usage) + frame(usage) + frame(finish) + frame("[DONE]"), want: 185, tokens: true},
		{name: "completed", body: frame(content) + frame(finish) + frame("[DONE]"), want: 1000},
		{name: "ordinary EOF", body: frame(content) + frame(finish), want: 1000},
		{name: "read failure", body: frame(content), cut: true},
		{name: "failure after completion", body: frame(content) + frame(finish), cut: true, want: 1000},
		{name: "client cancellation", body: frame(content) + frame(empty), cancel: true, want: 1000},
		{name: "refusal cancellation", body: frame(`{"choices":[{"index":0,"delta":{"refusal":"hello, denied"}}]}`) + frame(empty), cancel: true, want: 1000},
		{name: "legacy completion", body: frame(`{"choices":[{"index":0,"text":"hello","finish_reason":"stop"}]}`) + frame("[DONE]"), want: 1000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := modelManagementDB(t, "sqlite", "")
			require.NoError(t, db.AutoMigrate(&model.Token{}, &model.Log{}))
			oldLog, oldBatch := common.LogConsumeEnabled, common.BatchUpdateEnabled
			common.LogConsumeEnabled, common.BatchUpdateEnabled = true, false
			oldTimeout := constant.StreamingTimeout
			constant.StreamingTimeout = 5
			t.Cleanup(func() {
				common.LogConsumeEnabled, common.BatchUpdateEnabled = oldLog, oldBatch
				constant.StreamingTimeout = oldTimeout
			})
			user := model.User{Username: "stream-accounting", Quota: 10000}
			require.NoError(t, db.Create(&user).Error)
			token := model.Token{UserId: user.Id, Key: "stream-accounting-key", RemainQuota: 10000}
			require.NoError(t, db.Create(&token).Error)
			ch := model.Channel{Name: "stream-accounting", Key: "unused"}
			require.NoError(t, db.Create(&ch).Error)
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				if tc.cut {
					w.Header().Set("Content-Length", strconv.Itoa(len(tc.body)+100))
				}
				_, _ = io.WriteString(w, tc.body)
				w.(http.Flusher).Flush()
				if tc.cancel {
					<-r.Context().Done()
				}
			}))
			defer upstream.Close()
			done := make(chan struct{})
			engine := gin.New()
			engine.POST("/v1/chat/completions", func(c *gin.Context) {
				defer close(done)
				expr := `tier("request",fixed(0.002))`
				if tc.tokens {
					expr = `tier("token",p * 2 + c * 4 + cr * 0.1)`
				}
				info := &relaycommon.RelayInfo{UserId: user.Id, TokenId: token.Id, TokenKey: token.Key, OriginModelName: "billing-test", ChannelMeta: &relaycommon.ChannelMeta{UpstreamModelName: "billing-test", ChannelId: ch.Id}, StartTime: time.Now(), IsStream: true, ShouldIncludeUsage: true, RelayFormat: types.RelayFormatOpenAI, ForcePreConsume: true, UserSetting: dto.UserSetting{BillingPreference: "wallet_only"}, TieredBillingSnapshot: &billingexpr.BillingSnapshot{BillingMode: "tiered_expr", ExprString: expr, ExprHash: billingexpr.ExprHashString(expr), QuotaPerUnit: common.QuotaPerUnit, GroupRatio: 1, EstimatedQuotaAfterGroup: 2000}}
				info.SetEstimatePromptTokens(12)
				if apiErr := service.PreConsumeBilling(c, 2000, info); apiErr != nil {
					assert.Fail(t, apiErr.Error())
					return
				}
				request, err := http.NewRequestWithContext(c.Request.Context(), http.MethodGet, upstream.URL, nil)
				if !assert.NoError(t, err) {
					return
				}
				response, err := http.DefaultClient.Do(request)
				if !assert.NoError(t, err) {
					return
				}
				actual, apiErr := openai.OaiStreamHandler(c, info, response)
				if apiErr != nil {
					info.Billing.Refund(c)
					return
				}
				service.PostTextConsumeQuota(c, info, actual, nil)
			})
			gateway := httptest.NewServer(engine)
			defer gateway.Close()
			requestCtx, cancel := context.WithCancel(context.Background())
			defer cancel()
			request, err := http.NewRequestWithContext(requestCtx, http.MethodPost, gateway.URL+"/v1/chat/completions", nil)
			require.NoError(t, err)
			client := &http.Client{Timeout: 5 * time.Second}
			response, err := client.Do(request)
			require.NoError(t, err)
			defer response.Body.Close()
			var received []byte
			if tc.cancel {
				reader := bufio.NewReader(response.Body)
				line, err := reader.ReadString('\n')
				require.NoError(t, err)
				require.Contains(t, line, "hello")
				cancel()
			} else {
				received, err = io.ReadAll(response.Body)
				require.NoError(t, err)
			}
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("request did not finish")
			}
			require.NoError(t, db.First(&user, user.Id).Error)
			require.NoError(t, db.First(&token, token.Id).Error)
			assert.Equal(t, 10000-tc.want, user.Quota)
			assert.Equal(t, 10000-tc.want, token.RemainQuota)
			assert.Equal(t, tc.want, token.UsedQuota)
			var logs []model.Log
			require.NoError(t, db.Where("type = ? AND user_id = ?", model.LogTypeConsume, user.Id).Find(&logs).Error)
			if tc.want == 0 {
				assert.Empty(t, logs)
				assert.NotContains(t, string(received), "[DONE]")
			} else {
				require.Len(t, logs, 1)
				assert.Equal(t, tc.want, logs[0].Quota)
			}
			if tc.tokens {
				assert.Equal(t, 1000, logs[0].PromptTokens)
				assert.Equal(t, 20, logs[0].CompletionTokens)
				assert.Contains(t, string(received), `"cached_tokens":900`)
			}
		})
	}
}

func TestClaudeCacheAccounting(t *testing.T) {
	oldTimeout := constant.StreamingTimeout
	constant.StreamingTimeout = 5
	t.Cleanup(func() { constant.StreamingTimeout = oldTimeout })
	const expression = `tier("cache", p * 2 + cr * 0.1 + cc * 2.5 + cc1h * 4 + c * 4)`
	for _, tc := range []struct {
		name, usage, expr string
		want              int
	}{
		{"aggregate writes", `{"input_tokens":100,"cache_read_input_tokens":200,"cache_creation_input_tokens":300}`, expression, 485},
		{"partial TTL buckets", `{"input_tokens":100,"cache_read_input_tokens":200,"cache_creation_input_tokens":300,"cache_creation":{"ephemeral_1h_input_tokens":100}}`, expression, 560},
		{"split writes only", `{"cache_creation":{"ephemeral_5m_input_tokens":200,"ephemeral_1h_input_tokens":100}}`, expression, 450},
		{"cache read only", `{"cache_read_input_tokens":200}`, expression, 10},
		{"omitted cache variables", `{"input_tokens":100,"cache_read_input_tokens":200,"cache_creation_input_tokens":300}`, `tier("input", p * 2)`, 600},
	} {
		for _, stream := range []bool{false, true} {
			for _, format := range []types.RelayFormat{types.RelayFormatClaude, types.RelayFormatOpenAI} {
				t.Run(tc.name+"/"+strconv.FormatBool(stream)+"/"+string(format), func(t *testing.T) {
					db := modelManagementDB(t, "sqlite", "")
					require.NoError(t, db.AutoMigrate(&model.Token{}, &model.Log{}))
					oldLog, oldBatch := common.LogConsumeEnabled, common.BatchUpdateEnabled
					common.LogConsumeEnabled, common.BatchUpdateEnabled = true, false
					t.Cleanup(func() { common.LogConsumeEnabled, common.BatchUpdateEnabled = oldLog, oldBatch })
					user := model.User{Username: "claude-cache-accounting", Quota: 10000}
					require.NoError(t, db.Create(&user).Error)
					token := model.Token{UserId: user.Id, Key: "claude-cache-accounting-key", RemainQuota: 10000}
					require.NoError(t, db.Create(&token).Error)
					ch := model.Channel{Name: "claude-cache-accounting", Key: "unused"}
					require.NoError(t, db.Create(&ch).Error)
					body := `{"id":"msg-cache","model":"claude-cache-test","type":"message","content":[],"stop_reason":"end_turn","usage":` + tc.usage + `}`
					if stream {
						body = "data: " + `{"type":"message_start","message":` + body + "}\n\n" +
							"data: " + `{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":0}}` + "\n\n" +
							"data: " + `{"type":"message_stop"}` + "\n\n"
					}
					upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
						if stream {
							w.Header().Set("Content-Type", "text/event-stream")
						}
						_, _ = io.WriteString(w, body)
					}))
					defer upstream.Close()
					ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
					ctx.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
					info := &relaycommon.RelayInfo{
						UserId: user.Id, TokenId: token.Id, TokenKey: token.Key, OriginModelName: "claude-cache-test",
						ChannelMeta: &relaycommon.ChannelMeta{UpstreamModelName: "claude-cache-test", ChannelId: ch.Id},
						StartTime:   time.Now(), IsStream: stream, ShouldIncludeUsage: true, RelayFormat: format, ForcePreConsume: true,
						UserSetting: dto.UserSetting{BillingPreference: "wallet_only"},
						TieredBillingSnapshot: &billingexpr.BillingSnapshot{BillingMode: "tiered_expr", ExprString: tc.expr,
							ExprHash: billingexpr.ExprHashString(tc.expr), QuotaPerUnit: common.QuotaPerUnit, GroupRatio: 1, EstimatedQuotaAfterGroup: 2000},
					}
					require.Nil(t, service.PreConsumeBilling(ctx, 2000, info))
					response, err := http.Get(upstream.URL)
					require.NoError(t, err)
					var usage *dto.Usage
					var apiErr *types.NewAPIError
					if stream {
						usage, apiErr = claude.ClaudeStreamHandler(ctx, response, info)
					} else {
						usage, apiErr = claude.ClaudeHandler(ctx, response, info)
					}
					require.Nil(t, apiErr)
					require.NotNil(t, usage)
					service.PostTextConsumeQuota(ctx, info, usage, nil)
					require.NoError(t, db.First(&user, user.Id).Error)
					require.NoError(t, db.First(&token, token.Id).Error)
					assert.Equal(t, 10000-tc.want, user.Quota)
					assert.Equal(t, 10000-tc.want, token.RemainQuota)
					assert.Equal(t, tc.want, token.UsedQuota)
					var logs []model.Log
					require.NoError(t, db.Where("type = ? AND user_id = ?", model.LogTypeConsume, user.Id).Find(&logs).Error)
					require.Len(t, logs, 1)
					assert.Equal(t, tc.want, logs[0].Quota)
					var other map[string]any
					require.NoError(t, common.UnmarshalJsonStr(logs[0].Other, &other))
					assert.Equal(t, "anthropic", other["usage_semantic"])
				})
			}
		}
	}
}
