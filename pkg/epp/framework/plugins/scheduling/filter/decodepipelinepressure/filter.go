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

// Package decodepipelinepressure filters decode endpoints by their combined
// ordinary waiting, preallocation, and transfer queue pressure.
package decodepipelinepressure

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"

	"sigs.k8s.io/controller-runtime/pkg/log"

	logutil "github.com/llm-d/llm-d-router/pkg/common/observability/logging"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	attrmetrics "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/metrics"
)

const PluginType = "decode-pipeline-pressure-filter"

type FixedRange struct {
	Min float64 `json:"min"`
	Max float64 `json:"max"`
}

type SignalConfig struct {
	AttributeKey string     `json:"attributeKey,omitempty"`
	Weight       float64    `json:"weight"`
	FixedRange   FixedRange `json:"fixedRange"`
}

type Config struct {
	Threshold       float64      `json:"threshold"`
	OrdinaryWaiting SignalConfig `json:"ordinaryWaiting"`
	Prealloc        SignalConfig `json:"prealloc"`
	Transfer        SignalConfig `json:"transfer"`
}

type endpointPressure struct {
	endpoint fwksched.Endpoint
	pressure float64
}

type Filter struct {
	typedName fwkplugin.TypedName
	config    Config
}

var (
	_ fwksched.Filter          = &Filter{}
	_ fwkplugin.ConsumerPlugin = &Filter{}
)

func Factory(name string, rawParameters *json.Decoder, _ fwkplugin.Handle) (fwkplugin.Plugin, error) {
	var config Config
	if rawParameters != nil {
		if err := rawParameters.Decode(&config); err != nil {
			return nil, fmt.Errorf("failed to decode %s parameters: %w", PluginType, err)
		}
	}
	return New(name, config)
}

func New(name string, config Config) (*Filter, error) {
	if name == "" {
		name = PluginType
	}
	if config.Threshold < 0 {
		return nil, fmt.Errorf("%s threshold must be non-negative, got %v", PluginType, config.Threshold)
	}

	signals := []struct {
		name              string
		config            SignalConfig
		requiresAttribute bool
	}{
		{name: "ordinaryWaiting", config: config.OrdinaryWaiting},
		{name: "prealloc", config: config.Prealloc, requiresAttribute: true},
		{name: "transfer", config: config.Transfer, requiresAttribute: true},
	}
	totalWeight := 0.0
	for _, signal := range signals {
		if signal.config.Weight < 0 {
			return nil, fmt.Errorf("%s %s.weight must be non-negative, got %v", PluginType, signal.name, signal.config.Weight)
		}
		totalWeight += signal.config.Weight
		if signal.config.Weight == 0 {
			continue
		}
		if signal.requiresAttribute && signal.config.AttributeKey == "" {
			return nil, fmt.Errorf("%s %s.attributeKey must be non-empty when weight is positive", PluginType, signal.name)
		}
		if signal.config.FixedRange.Min >= signal.config.FixedRange.Max {
			return nil, fmt.Errorf("%s %s.fixedRange requires min < max, got min %v, max %v",
				PluginType, signal.name, signal.config.FixedRange.Min, signal.config.FixedRange.Max)
		}
	}
	if totalWeight == 0 {
		return nil, errors.New("decode pipeline pressure filter requires at least one positive weight")
	}

	return &Filter{
		typedName: fwkplugin.TypedName{Type: PluginType, Name: name},
		config:    config,
	}, nil
}

func (f *Filter) TypedName() fwkplugin.TypedName {
	return f.typedName
}

func (f *Filter) Consumes() fwkplugin.DataDependencies {
	optional := make(map[fwkplugin.DataKey]any, 2)
	for _, signal := range []SignalConfig{f.config.Prealloc, f.config.Transfer} {
		if signal.Weight > 0 {
			optional[fwkplugin.NewDataKey(signal.AttributeKey, "")] = attrmetrics.ScalarMetricValue(0)
		}
	}
	return fwkplugin.DataDependencies{Optional: optional}
}

func (f *Filter) Filter(ctx context.Context, _ *fwksched.InferenceRequest, endpoints []fwksched.Endpoint) []fwksched.Endpoint {
	if len(endpoints) <= 1 {
		return endpoints
	}

	logger := log.FromContext(ctx)
	pressures := make([]endpointPressure, 0, len(endpoints))
	minPressure := math.Inf(1)
	for _, endpoint := range endpoints {
		waiting := float64(endpoint.GetMetrics().WaitingQueueSize)
		waitingNormalized := 0.0
		if f.config.OrdinaryWaiting.Weight > 0 {
			waitingNormalized = normalize(waiting, f.config.OrdinaryWaiting.FixedRange)
		}

		prealloc, preallocNormalized, ok := f.readSignal(endpoint, f.config.Prealloc)
		if !ok {
			continue
		}
		transfer, transferNormalized, ok := f.readSignal(endpoint, f.config.Transfer)
		if !ok {
			continue
		}

		pressure := f.config.OrdinaryWaiting.Weight*waitingNormalized +
			f.config.Prealloc.Weight*preallocNormalized +
			f.config.Transfer.Weight*transferNormalized
		pressures = append(pressures, endpointPressure{endpoint: endpoint, pressure: pressure})
		minPressure = math.Min(minPressure, pressure)

		logger.V(logutil.TRACE).Info("DecodePipelinePressureFilter: endpoint pressure",
			"endpoint", endpoint.GetMetadata().Name,
			"rank", endpoint.GetMetadata().RankIndex,
			"ordinaryWaiting", waiting,
			"ordinaryWaitingNormalized", waitingNormalized,
			"prealloc", prealloc,
			"preallocNormalized", preallocNormalized,
			"transfer", transfer,
			"transferNormalized", transferNormalized,
			"pressure", pressure)
	}

	if len(pressures) == 0 {
		logger.V(logutil.DEBUG).Info("DecodePipelinePressureFilter: no endpoints have complete metrics, keeping all",
			"total", len(endpoints))
		return endpoints
	}

	filtered := make([]fwksched.Endpoint, 0, len(pressures))
	maximumPressure := minPressure + f.config.Threshold
	for _, candidate := range pressures {
		if candidate.pressure <= maximumPressure {
			filtered = append(filtered, candidate.endpoint)
		}
	}

	logger.V(logutil.DEBUG).Info("DecodePipelinePressureFilter: filtered endpoints",
		"minimumPressure", minPressure,
		"threshold", f.config.Threshold,
		"total", len(endpoints),
		"retained", len(filtered))
	return filtered
}

func (f *Filter) readSignal(endpoint fwksched.Endpoint, signal SignalConfig) (float64, float64, bool) {
	if signal.Weight == 0 {
		return 0, 0, true
	}
	value, ok := attrmetrics.ReadScalarMetricValue(endpoint, signal.AttributeKey)
	if !ok {
		return 0, 0, false
	}
	raw := float64(value)
	return raw, normalize(raw, signal.FixedRange), true
}

func normalize(value float64, fixedRange FixedRange) float64 {
	normalized := (value - fixedRange.Min) / (fixedRange.Max - fixedRange.Min)
	return math.Max(0, math.Min(1, normalized))
}
