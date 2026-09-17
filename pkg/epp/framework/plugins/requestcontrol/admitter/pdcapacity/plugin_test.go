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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	errcommon "github.com/llm-d/llm-d-router/pkg/common/error"
	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkrh "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requesthandling"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	attrconcurrency "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/concurrency"
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
		{name: "zero transfer threshold", parameters: `{"decode":{"transfer":{"threshold":0}}}`, wantErr: "transfer.threshold"},
		{name: "default output over maximum", parameters: `{"decode":{"defaultOutputTokens":9,"maxOutputTokens":8}}`, wantErr: "defaultOutputTokens"},
		{name: "bad predicted wait", parameters: `{"prefill":{"predictedWait":{"inFlightLoadProducerName":"inflight-load-producer","peakTokensPerSecond":8000,"maxWait":"bad"}}}`, wantErr: "maxWait"},
		{name: "zero predicted throughput", parameters: `{"prefill":{"predictedWait":{"inFlightLoadProducerName":"inflight-load-producer","peakTokensPerSecond":0,"maxWait":"3s"}}}`, wantErr: "peakTokensPerSecond"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plugin, err := Factory("test", json.NewDecoder(strings.NewReader(tt.parameters)), nil)
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

func TestConsumesPredictedWaitDependencies(t *testing.T) {
	config := DefaultConfig()
	without, err := New("test", config)
	require.NoError(t, err)
	deps := without.Consumes()
	assert.NotEmpty(t, deps.Required)
	assert.Len(t, deps.Optional, 2)

	config.Prefill.PredictedWait = &PredictedWaitConfig{
		InFlightLoadProducerName: "custom-inflight",
		PeakTokensPerSecond:      8000,
		MaxWait:                  "3s",
	}
	with, err := New("test", config)
	require.NoError(t, err)
	deps = with.Consumes()
	assert.Len(t, deps.Required, 3)
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
			name: "all decode transfer queues full",
			endpoints: []fwksched.Endpoint{
				endpoint("prefill", bylabel.RolePrefill, 0, 0.1, 100000, 0, 0, time.Now()),
				endpoint("decode", bylabel.RoleDecode, 0, 0.1, 100000, 0, 12, time.Now()),
			},
			wantCode: errcommon.ResourceExhausted,
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
			name:    "projected request kv does not fit",
			request: request(9000, 2000, 0),
			endpoints: []fwksched.Endpoint{
				endpoint("prefill", bylabel.RolePrefill, 0, 0.1, 100000, 0, 0, time.Now()),
				endpoint("decode", bylabel.RoleDecode, 0, 0.0, 10000, 0, 0, time.Now()),
			},
			wantCode: errcommon.ResourceExhausted,
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
			name: "protected priority bypasses gate by default",
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

func TestAdmitPredictedPrefillWait(t *testing.T) {
	config := DefaultConfig()
	config.RejectAllPriorities = true
	config.Prefill.PredictedWait = &PredictedWaitConfig{
		InFlightLoadProducerName: "inflight-load-producer",
		PeakTokensPerSecond:      8000,
		MaxWait:                  "3s",
	}
	admitter, err := New("test", config)
	require.NoError(t, err)

	prefill := endpoint("prefill", bylabel.RolePrefill, 0, 0.1, 100000, 0, 0, time.Now())
	prefill.Put(admitter.inFlightLoadDataKey.String(), &attrconcurrency.InFlightLoad{Tokens: 24000})
	prefill.Put(admitter.uncachedRequestTokensDataKey.String(), &attrconcurrency.UncachedRequestTokens{Tokens: 1})
	decode := endpoint("decode", bylabel.RoleDecode, 0, 0.1, 100000, 0, 0, time.Now())

	err = admitter.Admit(context.Background(), request(1000, 1000, 0), []fwksched.Endpoint{prefill, decode})
	require.Error(t, err)
	var typed errcommon.Error
	require.ErrorAs(t, err, &typed)
	assert.Equal(t, errcommon.ResourceExhausted, typed.Code)
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
	return fwksched.NewEndpoint(
		&fwkdl.EndpointMetadata{Name: name, Labels: map[string]string{bylabel.RoleLabel: role}},
		&fwkdl.Metrics{KvCacheMaxTokenCapacity: 100000, UpdateTime: updated},
		fwkdl.NewAttributes(),
	)
}

func TestDefaultConfigValues(t *testing.T) {
	config := DefaultConfig()
	assert.Equal(t, "12s", config.MetricsStalenessThreshold)
	assert.Equal(t, 4, config.Decode.WaitingQueueThreshold)
	assert.Equal(t, 0.92, config.Decode.KVCacheUtilizationThreshold)
	assert.Equal(t, int64(2048), config.Decode.DefaultOutputTokens)
	assert.Equal(t, int64(8192), config.Decode.MaxOutputTokens)
	assert.Equal(t, SignalConfig{AttributeKey: preallocKey, Threshold: 8}, config.Decode.Prealloc)
	assert.Equal(t, SignalConfig{AttributeKey: transferKey, Threshold: 12}, config.Decode.Transfer)
	assert.Equal(t, 4, config.Prefill.WaitingQueueThreshold)
	assert.Nil(t, config.Prefill.PredictedWait)
}

func ExampleConfig() {
	config := DefaultConfig()
	fmt.Println(config.Decode.Prealloc.AttributeKey)
	// Output: sglang.decode_prealloc_queue_reqs
}
