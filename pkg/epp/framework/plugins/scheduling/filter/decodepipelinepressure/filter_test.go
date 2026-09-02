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

package decodepipelinepressure

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	attrmetrics "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/metrics"
)

const (
	preallocKey = "sglang.decode_prealloc_queue_reqs"
	transferKey = "sglang.decode_transfer_queue_reqs"

	validConfigJSON = `{
		"threshold": 0.75,
		"ordinaryWaiting": {"weight": 2, "fixedRange": {"min": 0, "max": 4}},
		"prealloc": {"attributeKey": "sglang.decode_prealloc_queue_reqs", "weight": 2,
			"fixedRange": {"min": 1, "max": 8}},
		"transfer": {"attributeKey": "sglang.decode_transfer_queue_reqs", "weight": 1,
			"fixedRange": {"min": 2, "max": 12}}
	}`
)

func validConfig() Config {
	return Config{
		Threshold: 0.75,
		OrdinaryWaiting: SignalConfig{
			Weight:     2,
			FixedRange: FixedRange{Min: 0, Max: 4},
		},
		Prealloc: SignalConfig{
			AttributeKey: preallocKey,
			Weight:       2,
			FixedRange:   FixedRange{Min: 1, Max: 8},
		},
		Transfer: SignalConfig{
			AttributeKey: transferKey,
			Weight:       1,
			FixedRange:   FixedRange{Min: 2, Max: 12},
		},
	}
}

func TestFactory(t *testing.T) {
	tests := []struct {
		name       string
		parameters string
		wantErr    string
	}{
		{name: "valid", parameters: validConfigJSON},
		{
			name:       "negative threshold",
			parameters: strings.Replace(validConfigJSON, `"threshold": 0.75`, `"threshold": -0.1`, 1),
			wantErr:    "threshold",
		},
		{
			name:       "negative weight",
			parameters: strings.Replace(validConfigJSON, `"weight": 2`, `"weight": -1`, 1),
			wantErr:    "weight",
		},
		{
			name: "all weights zero",
			parameters: `{"threshold": 0.75,
				"ordinaryWaiting": {"weight": 0},
				"prealloc": {"weight": 0},
				"transfer": {"weight": 0}}`,
			wantErr: "positive",
		},
		{
			name: "enabled prealloc missing attribute key",
			parameters: `{"threshold": 0.75,
				"ordinaryWaiting": {"weight": 1, "fixedRange": {"min": 0, "max": 4}},
				"prealloc": {"weight": 1, "fixedRange": {"min": 1, "max": 8}},
				"transfer": {"weight": 0}}`,
			wantErr: "attributeKey",
		},
		{
			name: "enabled transfer missing attribute key",
			parameters: `{"threshold": 0.75,
				"ordinaryWaiting": {"weight": 1, "fixedRange": {"min": 0, "max": 4}},
				"prealloc": {"weight": 0},
				"transfer": {"weight": 1, "fixedRange": {"min": 2, "max": 12}}}`,
			wantErr: "attributeKey",
		},
		{
			name:       "invalid ordinary waiting range",
			parameters: strings.Replace(validConfigJSON, `"min": 0, "max": 4`, `"min": 4, "max": 4`, 1),
			wantErr:    "ordinaryWaiting.fixedRange",
		},
		{
			name:       "invalid prealloc range",
			parameters: strings.Replace(validConfigJSON, `"min": 1, "max": 8`, `"min": 8, "max": 1`, 1),
			wantErr:    "prealloc.fixedRange",
		},
		{
			name:       "invalid transfer range",
			parameters: strings.Replace(validConfigJSON, `"min": 2, "max": 12`, `"min": 12, "max": 12`, 1),
			wantErr:    "transfer.fixedRange",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			decoder := json.NewDecoder(strings.NewReader(test.parameters))
			got, err := Factory("test-filter", decoder, nil)
			if test.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.wantErr)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, fwkplugin.TypedName{Type: PluginType, Name: "test-filter"}, got.TypedName())
		})
	}
}

func TestNormalize(t *testing.T) {
	r := FixedRange{Min: 2, Max: 12}
	assert.Equal(t, 0.0, normalize(1, r))
	assert.Equal(t, 0.0, normalize(2, r))
	assert.Equal(t, 0.5, normalize(7, r))
	assert.Equal(t, 1.0, normalize(12, r))
	assert.Equal(t, 1.0, normalize(20, r))
}

func endpoint(waiting int, prealloc, transfer *float64) fwksched.Endpoint {
	attrs := fwkdl.NewAttributes()
	if prealloc != nil {
		attrs.Put(preallocKey, attrmetrics.ScalarMetricValue(*prealloc))
	}
	if transfer != nil {
		attrs.Put(transferKey, attrmetrics.ScalarMetricValue(*transfer))
	}
	return fwksched.NewEndpoint(
		&fwkdl.EndpointMetadata{},
		&fwkdl.Metrics{WaitingQueueSize: waiting},
		attrs,
	)
}

func value(v float64) *float64 {
	return &v
}

func TestFilter(t *testing.T) {
	tests := []struct {
		name      string
		config    Config
		endpoints []fwksched.Endpoint
		wantKept  []int
	}{
		{
			name: "keeps pressure difference equal to threshold",
			config: Config{
				Threshold:       0.5,
				OrdinaryWaiting: SignalConfig{Weight: 2, FixedRange: FixedRange{Min: 0, Max: 4}},
			},
			endpoints: []fwksched.Endpoint{endpoint(0, nil, nil), endpoint(1, nil, nil)},
			wantKept:  []int{0, 1},
		},
		{
			name: "filters pressure difference above threshold",
			config: Config{
				Threshold:       0.5,
				OrdinaryWaiting: SignalConfig{Weight: 2, FixedRange: FixedRange{Min: 0, Max: 4}},
			},
			endpoints: []fwksched.Endpoint{endpoint(0, nil, nil), endpoint(2, nil, nil)},
			wantKept:  []int{0},
		},
		{
			name: "combines moderate pressure signals",
			config: Config{
				Threshold:       0.75,
				OrdinaryWaiting: SignalConfig{Weight: 2, FixedRange: FixedRange{Min: 0, Max: 4}},
				Prealloc:        SignalConfig{AttributeKey: preallocKey, Weight: 2, FixedRange: FixedRange{Min: 1, Max: 8}},
				Transfer:        SignalConfig{AttributeKey: transferKey, Weight: 1, FixedRange: FixedRange{Min: 2, Max: 12}},
			},
			endpoints: []fwksched.Endpoint{
				endpoint(0, value(1), value(2)),
				endpoint(1, value(2), value(3)),
			},
			wantKept: []int{0},
		},
		{
			name:      "excludes incomplete endpoint when complete data exists",
			config:    validConfig(),
			endpoints: []fwksched.Endpoint{endpoint(0, value(1), value(2)), endpoint(0, value(1), nil)},
			wantKept:  []int{0},
		},
		{
			name:      "fails open when every endpoint is incomplete",
			config:    validConfig(),
			endpoints: []fwksched.Endpoint{endpoint(0, nil, nil), endpoint(1, value(2), nil)},
			wantKept:  []int{0, 1},
		},
		{
			name: "ignores zero weight signal and missing attribute",
			config: Config{
				Threshold:       0.5,
				OrdinaryWaiting: SignalConfig{Weight: 2, FixedRange: FixedRange{Min: 0, Max: 4}},
				Prealloc:        SignalConfig{Weight: 0},
				Transfer:        SignalConfig{Weight: 0},
			},
			endpoints: []fwksched.Endpoint{endpoint(0, nil, nil), endpoint(1, nil, nil)},
			wantKept:  []int{0, 1},
		},
		{
			name:      "returns empty input unchanged",
			config:    validConfig(),
			endpoints: nil,
			wantKept:  nil,
		},
		{
			name:      "returns singleton input unchanged",
			config:    validConfig(),
			endpoints: []fwksched.Endpoint{endpoint(0, nil, nil)},
			wantKept:  []int{0},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			filter, err := New("test-filter", test.config)
			require.NoError(t, err)

			got := filter.Filter(context.Background(), &fwksched.InferenceRequest{}, test.endpoints)
			var want []fwksched.Endpoint
			if test.wantKept != nil {
				want = make([]fwksched.Endpoint, 0, len(test.wantKept))
				for _, i := range test.wantKept {
					want = append(want, test.endpoints[i])
				}
			}
			assert.Equal(t, want, got)
		})
	}
}

func TestConsumesOnlyEnabledCustomSignals(t *testing.T) {
	config := validConfig()
	config.Transfer.Weight = 0
	config.Transfer.AttributeKey = ""
	config.Transfer.FixedRange = FixedRange{}
	filter, err := New("test-filter", config)
	require.NoError(t, err)

	dependencies := filter.Consumes()
	require.Len(t, dependencies.Optional, 1)
	for key := range dependencies.Optional {
		assert.Equal(t, fwkplugin.NewDataKey(preallocKey, "").String(), key.String())
	}
}
