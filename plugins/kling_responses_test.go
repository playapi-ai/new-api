package plugins_test

import (
	"testing"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/pkg/jsplugin"
	builtinplugins "github.com/QuantumNous/new-api/plugins"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKlingResponsesProtocol(t *testing.T) {
	testVideoResponsesProtocol(t, videoResponsesTestCase{
		pluginKey: "kling",
		model:     "kling-v2-master",
		requestBody: map[string]any{
			"model": "kling-v2-master",
			"input": []any{map[string]any{"role": "user", "content": []any{
				map[string]any{"type": "input_text", "text": "camera orbit"},
				map[string]any{"type": "input_image", "image_url": "https://cdn.example/frame.png"},
			}}},
			"seconds": 10,
			"metadata": map[string]any{
				"mode": "pro",
			},
		},
		wantAction: "image_to_video",
		wantRequest: map[string]any{
			"model":    "kling-v2-master",
			"prompt":   "camera orbit",
			"image":    "https://cdn.example/frame.png",
			"duration": float64(10),
			"metadata": map[string]any{"mode": "pro"},
		},
		wantUsageKeys:  []string{"units"},
		wantVendorName: "kling",
	})
}

func TestKlingQueryAPIKey(t *testing.T) {
	source, err := builtinplugins.Source("kling")
	require.NoError(t, err)
	plugin, err := jsplugin.NewRegistry().RegisterFactory(source, jsplugin.Options{Key: "kling"})
	require.NoError(t, err)
	for _, tc := range []struct {
		name, key, want, wantError string
	}{
		{name: "single key", key: "vendor-test-key", want: "Bearer vendor-test-key"},
		{name: "trim whitespace", key: "  vendor-test-key \n", want: "Bearer vendor-test-key"},
		{name: "empty key", wantError: "api_key is empty"},
		{name: "whitespace key", key: " \t", wantError: "api_key is empty"},
		{name: "malformed pair", key: "access|secret|extra", wantError: "invalid api_key"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			value, err := plugin.Engine.Call(t.Context(), "buildQueryRequest", map[string]any{
				"apiKey": tc.key, "taskId": "query-task", "action": "text_to_video", "baseUrl": "https://kling.example",
			})
			if tc.wantError != "" {
				require.ErrorContains(t, err, tc.wantError)
				return
			}
			require.NoError(t, err)
			encoded, err := common.Marshal(value)
			require.NoError(t, err)
			var descriptor struct {
				Headers map[string]string
			}
			require.NoError(t, common.Unmarshal(encoded, &descriptor))
			assert.Equal(t, tc.want, descriptor.Headers["Authorization"])
		})
	}
}
