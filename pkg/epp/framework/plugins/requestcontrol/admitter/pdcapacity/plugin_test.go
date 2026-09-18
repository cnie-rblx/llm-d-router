/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package pdcapacity

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	errcommon "github.com/llm-d/llm-d-router/pkg/common/error"
	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	fwkrh "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requesthandling"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	attrmetrics "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/metrics"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/scheduling/filter/bylabel"
)

const (
	preallocKey = "sglang.decode_prealloc_queue_reqs"
	transferKey = "sglang.decode_transfer_queue_reqs"
)

func TestFactoryValidation(t *testing.T) {
	tests := []struct {
		name       string
		parameters string
		wantErr    string
	}{
		{name: "defaults", parameters: `{}`},
		{name: "bad staleness", parameters: `{"metricsStalenessThreshold":"bad"}`, wantErr: "metricsStalenessThreshold"},
		{name: "zero decode waiting", parameters: `{"decode":{"waitingQueueThreshold":0}}`, wantErr: "waitingQueueThreshold"},
		{name: "bad kv fraction", parameters: `{"decode":{"kvCacheUtilizationThreshold":1.1}}`, wantErr: "kvCacheUtilizationThreshold"},
		{name: "empty prealloc key", parameters: `{"decode":{"prealloc":{"attributeKey":""}}}`, wantErr: "prealloc.attributeKey"},
		{name: "legacy transfer config ignored", parameters: `{"decode":{"transfer":{"attributeKey":"sglang.decode_transfer_queue_reqs","threshold":12}}}`},
		{name: "legacy output reservation config ignored", parameters: `{"decode":{"defaultOutputTokens":9,"maxOutputTokens":8}}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plugin, err := Factory("test", fwkplugin.StrictDecoder(json.RawMessage(tt.parameters)), nil)
			if tt.wantErr == "" {
				require.NoError(t, err)
				assert.Equal(t, PluginType, plugin.TypedName().Type)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestConsumesDependencies(t *testing.T) {
	config := DefaultConfig()
	admitter, err := New("test", config)
	require.NoError(t, err)
	deps := admitter.Consumes()
	assert.Empty(t, deps.Required)
	assert.Contains(t, deps.Optional, fwkplugin.NewDataKey(attrmetrics.ScalarMetricSampleKey(preallocKey), ""))
	assert.NotContains(t, deps.Optional, fwkplugin.NewDataKey(transferKey, ""))
	assert.Contains(t, deps.Optional, fwkplugin.NewDataKey(attrmetrics.WaitingQueueSampleKey, ""))
	assert.Contains(t, deps.Optional, fwkplugin.NewDataKey(attrmetrics.KVCacheUtilizationSampleKey, ""))
	assert.Len(t, deps.Optional, 3)
}

func TestAdmit(t *testing.T) {
	baseRequest := request(1000, 1000, 0)

	tests := []struct {
		name      string
		configure func(*Config)
		request   *fwksched.InferenceRequest
		endpoints []fwksched.Endpoint
		wantCode  string
	}{
		{
			name: "one feasible endpoint per role admits",
			endpoints: []fwksched.Endpoint{
				endpoint("prefill", bylabel.RolePrefill, 0, 0.1, 100000, 0, 0, time.Now()),
				endpoint("decode", bylabel.RoleDecode, 0, 0.1, 100000, 0, 0, time.Now()),
			},
		},
		{
			name: "all decode prealloc queues full",
			endpoints: []fwksched.Endpoint{
				endpoint("prefill", bylabel.RolePrefill, 0, 0.1, 100000, 0, 0, time.Now()),
				endpoint("decode", bylabel.RoleDecode, 0, 0.1, 100000, 8, 0, time.Now()),
			},
			wantCode: errcommon.ResourceExhausted,
		},
		{
			name: "transfer queue depth does not gate admission",
			endpoints: []fwksched.Endpoint{
				endpoint("prefill", bylabel.RolePrefill, 0, 0.1, 100000, 0, 0, time.Now()),
				endpoint("decode", bylabel.RoleDecode, 0, 0.1, 100000, 0, 100, time.Now()),
			},
		},
		{
			name: "all decode ordinary queues full",
			endpoints: []fwksched.Endpoint{
				endpoint("prefill", bylabel.RolePrefill, 0, 0.1, 100000, 0, 0, time.Now()),
				endpoint("decode", bylabel.RoleDecode, 4, 0.1, 100000, 0, 0, time.Now()),
			},
			wantCode: errcommon.ResourceExhausted,
		},
		{
			name: "decode kv utilization over threshold",
			endpoints: []fwksched.Endpoint{
				endpoint("prefill", bylabel.RolePrefill, 0, 0.1, 100000, 0, 0, time.Now()),
				endpoint("decode", bylabel.RoleDecode, 0, 0.92, 100000, 0, 0, time.Now()),
			},
			wantCode: errcommon.ResourceExhausted,
		},
		{
			name:    "request size and capacity do not gate admission",
			request: request(9000, 2000, 0),
			endpoints: []fwksched.Endpoint{
				endpoint("prefill", bylabel.RolePrefill, 0, 0.1, 100000, 0, 0, time.Now()),
				endpoint("decode", bylabel.RoleDecode, 0, 0.0, 10000, 0, 0, time.Now()),
			},
		},
		{
			name:    "missing tokenized request does not gate admission",
			request: &fwksched.InferenceRequest{Objectives: fwksched.RequestObjectives{Priority: 0}},
			endpoints: []fwksched.Endpoint{
				endpoint("prefill", bylabel.RolePrefill, 0, 0.1, 100000, 0, 0, time.Now()),
				endpoint("decode", bylabel.RoleDecode, 0, 0.1, 0, 0, 0, time.Now()),
			},
		},
		{
			name: "healthy decode bypasses overloaded peer",
			endpoints: []fwksched.Endpoint{
				endpoint("prefill", bylabel.RolePrefill, 0, 0.1, 100000, 0, 0, time.Now()),
				endpoint("decode-full", bylabel.RoleDecode, 0, 0.1, 100000, 8, 0, time.Now()),
				endpoint("decode-free", bylabel.RoleDecode, 0, 0.1, 100000, 0, 0, time.Now()),
			},
		},
		{
			name: "all prefill ordinary queues full",
			endpoints: []fwksched.Endpoint{
				endpoint("prefill", bylabel.RolePrefill, 4, 0.1, 100000, 0, 0, time.Now()),
				endpoint("decode", bylabel.RoleDecode, 0, 0.1, 100000, 0, 0, time.Now()),
			},
			wantCode: errcommon.ResourceExhausted,
		},
		{
			name: "stale prefill metrics",
			endpoints: []fwksched.Endpoint{
				endpoint("prefill", bylabel.RolePrefill, 0, 0.1, 100000, 0, 0, time.Now().Add(-time.Minute)),
				endpoint("decode", bylabel.RoleDecode, 0, 0.1, 100000, 0, 0, time.Now()),
			},
			wantCode: errcommon.ResourceExhausted,
		},
		{
			name: "zero metrics timestamp",
			endpoints: []fwksched.Endpoint{
				endpoint("prefill", bylabel.RolePrefill, 0, 0.1, 100000, 0, 0, time.Time{}),
				endpoint("decode", bylabel.RoleDecode, 0, 0.1, 100000, 0, 0, time.Now()),
			},
			wantCode: errcommon.ResourceExhausted,
		},
		{
			name: "missing core metrics",
			endpoints: []fwksched.Endpoint{
				endpoint("prefill", bylabel.RolePrefill, 0, 0.1, 100000, 0, 0, time.Now()),
				endpointWithoutCoreMetrics("decode", bylabel.RoleDecode),
			},
			wantCode: errcommon.ResourceExhausted,
		},
		{
			name: "missing custom decode metric",
			endpoints: []fwksched.Endpoint{
				endpoint("prefill", bylabel.RolePrefill, 0, 0.1, 100000, 0, 0, time.Now()),
				endpointWithoutCustomMetrics("decode", bylabel.RoleDecode, time.Now()),
			},
			wantCode: errcommon.ResourceExhausted,
		},
		{
			name: "hybrid role satisfies both legs",
			endpoints: []fwksched.Endpoint{
				endpoint("hybrid", bylabel.RolePrefillDecode, 0, 0.1, 100000, 0, 0, time.Now()),
			},
		},
		{
			name: "unlabeled endpoint satisfies neither role",
			endpoints: []fwksched.Endpoint{
				endpoint("unlabeled", "", 0, 0.1, 100000, 0, 0, time.Now()),
			},
			wantCode: errcommon.ResourceExhausted,
		},
		{
			name: "configured priority bypass skips gate",
			configure: func(config *Config) {
				config.RejectAllPriorities = false
			},
			request:   request(1000, 1000, 1),
			endpoints: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := DefaultConfig()
			config.RejectAllPriorities = true
			if tt.configure != nil {
				tt.configure(&config)
			}
			admitter, err := New("test", config)
			require.NoError(t, err)

			req := tt.request
			if req == nil {
				req = baseRequest
			}
			err = admitter.Admit(context.Background(), req, tt.endpoints)
			if tt.wantCode == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			var typed errcommon.Error
			require.ErrorAs(t, err, &typed)
			assert.Equal(t, tt.wantCode, typed.Code)
		})
	}
}

func TestAdmitRejectsStaleCustomMetrics(t *testing.T) {
	config := DefaultConfig()
	admitter, err := New("test", config)
	require.NoError(t, err)

	prefill := endpoint("prefill", bylabel.RolePrefill, 0, 0.1, 100000, 0, 0, time.Now())
	decode := endpoint("decode", bylabel.RoleDecode, 0, 0.1, 100000, 0, 0, time.Now())
	decode.Put(attrmetrics.ScalarMetricSampleKey(preallocKey),
		attrmetrics.MetricSample{Value: 0, UpdatedAt: time.Now().Add(-time.Minute)})

	err = admitter.Admit(context.Background(), request(1000, 1000, 0), []fwksched.Endpoint{prefill, decode})
	require.Error(t, err)
	var typed errcommon.Error
	require.ErrorAs(t, err, &typed)
	assert.Equal(t, errcommon.ResourceExhausted, typed.Code)
}

func TestAdmitRejectsStaleCoreMetricDespiteFreshSharedTimestamp(t *testing.T) {
	admitter, err := New("test", DefaultConfig())
	require.NoError(t, err)

	prefill := endpoint("prefill", bylabel.RolePrefill, 0, 0.1, 100000, 0, 0, time.Now())
	decode := endpoint("decode", bylabel.RoleDecode, 0, 0.1, 100000, 0, 0, time.Now())
	decode.GetMetrics().UpdateTime = time.Now()
	decode.Put(attrmetrics.WaitingQueueSampleKey,
		attrmetrics.MetricSample{Value: 0, UpdatedAt: time.Now().Add(-time.Minute)})

	err = admitter.Admit(context.Background(), request(1000, 1000, 0), []fwksched.Endpoint{prefill, decode})
	require.Error(t, err)
	var typed errcommon.Error
	require.ErrorAs(t, err, &typed)
	assert.Equal(t, errcommon.ResourceExhausted, typed.Code)
}

func TestEndpointRoles(t *testing.T) {
	tests := []struct {
		role        string
		wantPrefill bool
		wantDecode  bool
	}{
		{role: bylabel.RolePrefill, wantPrefill: true},
		{role: bylabel.RoleDecode, wantDecode: true},
		{role: bylabel.RoleEncodePrefill, wantPrefill: true},
		{role: bylabel.RolePrefillDecode, wantPrefill: true, wantDecode: true},
		{role: legacyRoleBoth, wantPrefill: true, wantDecode: true},
		{role: bylabel.RoleEncodePrefillDecode, wantPrefill: true, wantDecode: true},
		{role: ""},
		{role: "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.role, func(t *testing.T) {
			prefill, decode := endpointRoles(endpoint("test", tt.role, 0, 0, 100000, 0, 0, time.Now()))
			assert.Equal(t, tt.wantPrefill, prefill)
			assert.Equal(t, tt.wantDecode, decode)
		})
	}
}

func request(promptTokens int, maxOutputTokens int64, priority int) *fwksched.InferenceRequest {
	return &fwksched.InferenceRequest{
		Body: &fwkrh.InferenceRequestBody{
			TokenizedPrompt: &fwkrh.TokenizedPrompt{PerPromptTokens: [][]uint32{make([]uint32, promptTokens)}},
			MaxOutputTokens: &maxOutputTokens,
		},
		Objectives: fwksched.RequestObjectives{Priority: priority},
	}
}

func endpoint(name, role string, waiting int, kvUsage float64, kvCapacity int, prealloc, transfer float64, updated time.Time) fwksched.Endpoint {
	attrs := fwkdl.NewAttributes()
	attrs.Put(attrmetrics.WaitingQueueSampleKey, attrmetrics.MetricSample{Value: float64(waiting), UpdatedAt: updated})
	attrs.Put(attrmetrics.KVCacheUtilizationSampleKey, attrmetrics.MetricSample{Value: kvUsage, UpdatedAt: updated})
	attrs.Put(attrmetrics.ScalarMetricSampleKey(preallocKey), attrmetrics.MetricSample{Value: prealloc, UpdatedAt: updated})
	attrs.Put(preallocKey, attrmetrics.ScalarMetricValue(prealloc))
	attrs.Put(transferKey, attrmetrics.ScalarMetricValue(transfer))
	labels := map[string]string{}
	if role != "" {
		labels[bylabel.RoleLabel] = role
	}
	return fwksched.NewEndpoint(
		&fwkdl.EndpointMetadata{Name: name, Labels: labels},
		&fwkdl.Metrics{
			WaitingQueueSize:        waiting,
			KVCacheUsagePercent:     kvUsage,
			KvCacheMaxTokenCapacity: kvCapacity,
			UpdateTime:              updated,
		},
		attrs,
	)
}

func endpointWithoutCustomMetrics(name, role string, updated time.Time) fwksched.Endpoint {
	attrs := fwkdl.NewAttributes()
	attrs.Put(attrmetrics.WaitingQueueSampleKey, attrmetrics.MetricSample{Value: 0, UpdatedAt: updated})
	attrs.Put(attrmetrics.KVCacheUtilizationSampleKey, attrmetrics.MetricSample{Value: 0, UpdatedAt: updated})
	return fwksched.NewEndpoint(
		&fwkdl.EndpointMetadata{Name: name, Labels: map[string]string{bylabel.RoleLabel: role}},
		&fwkdl.Metrics{KvCacheMaxTokenCapacity: 100000, UpdateTime: updated},
		attrs,
	)
}

func endpointWithoutCoreMetrics(name, role string) fwksched.Endpoint {
	attrs := fwkdl.NewAttributes()
	attrs.Put(preallocKey, attrmetrics.ScalarMetricValue(0))
	attrs.Put(transferKey, attrmetrics.ScalarMetricValue(0))
	return fwksched.NewEndpoint(
		&fwkdl.EndpointMetadata{Name: name, Labels: map[string]string{bylabel.RoleLabel: role}},
		nil,
		attrs,
	)
}

func TestDefaultConfigValues(t *testing.T) {
	config := DefaultConfig()
	assert.True(t, config.RejectAllPriorities)
	assert.Equal(t, "12s", config.MetricsStalenessThreshold)
	assert.Equal(t, 4, config.Decode.WaitingQueueThreshold)
	assert.Equal(t, 0.92, config.Decode.KVCacheUtilizationThreshold)
	assert.Equal(t, SignalConfig{AttributeKey: preallocKey, Threshold: 8, RecoveryThreshold: 4}, config.Decode.Prealloc)
	assert.Equal(t, 2, config.Decode.WaitingQueueRecoveryThreshold)
	assert.Equal(t, .85, config.Decode.KVCacheUtilizationRecoveryThreshold)
	assert.Equal(t, 2, config.Prefill.WaitingQueueRecoveryThreshold)
	assert.Equal(t, 4, config.Prefill.WaitingQueueThreshold)
}

func ExampleConfig() {
	config := DefaultConfig()
	fmt.Println(config.Decode.Prealloc.AttributeKey)
	// Output: sglang.decode_prealloc_queue_reqs
}
