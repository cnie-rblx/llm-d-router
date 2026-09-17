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

// Package pdcapacity implements role-aware admission control for disaggregated
// prefill and decode deployments.
package pdcapacity

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/log"

	errcommon "github.com/llm-d/llm-d-router/pkg/common/error"
	logutil "github.com/llm-d/llm-d-router/pkg/common/observability/logging"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requestcontrol"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	attrmetrics "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/metrics"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/scheduling/filter/bylabel"
)

const (
	PluginType = "pd-capacity-admitter"

	defaultMetricsStalenessThreshold = "12s"
	defaultDecodeWaitingThreshold    = 4
	defaultDecodeKVThreshold         = 0.92
	defaultPreallocAttributeKey      = "sglang.decode_prealloc_queue_reqs"
	defaultPreallocThreshold         = 8.0
	defaultPrefillWaitingThreshold   = 4
	legacyRoleBoth                   = "both"
)

// SignalConfig identifies a scalar endpoint metric and its rejection threshold.
type SignalConfig struct {
	AttributeKey string  `json:"attributeKey"`
	Threshold    float64 `json:"threshold"`
}

// DecodeConfig configures decode capacity checks.
type DecodeConfig struct {
	WaitingQueueThreshold       int          `json:"waitingQueueThreshold"`
	KVCacheUtilizationThreshold float64      `json:"kvCacheUtilizationThreshold"`
	Prealloc                    SignalConfig `json:"prealloc"`
	// Deprecated: retained only so existing strict-decoded configurations remain valid.
	// Output-token reservations are not used for admission decisions.
	DefaultOutputTokens int64 `json:"defaultOutputTokens"`
	MaxOutputTokens     int64 `json:"maxOutputTokens"`
	// Deprecated: retained only so existing strict-decoded configurations remain valid.
	// Transfer-queue depth is not used for admission decisions.
	Transfer SignalConfig `json:"transfer"`
}

// PrefillConfig configures prefill capacity checks.
type PrefillConfig struct {
	WaitingQueueThreshold int `json:"waitingQueueThreshold"`
}

// Config configures the P/D capacity admitter.
type Config struct {
	RejectAllPriorities       bool          `json:"rejectAllPriorities"`
	MetricsStalenessThreshold string        `json:"metricsStalenessThreshold"`
	Decode                    DecodeConfig  `json:"decode"`
	Prefill                   PrefillConfig `json:"prefill"`
}

// DefaultConfig returns the default admission thresholds.
func DefaultConfig() Config {
	return Config{
		RejectAllPriorities:       true,
		MetricsStalenessThreshold: defaultMetricsStalenessThreshold,
		Decode: DecodeConfig{
			WaitingQueueThreshold:       defaultDecodeWaitingThreshold,
			KVCacheUtilizationThreshold: defaultDecodeKVThreshold,
			Prealloc: SignalConfig{
				AttributeKey: defaultPreallocAttributeKey,
				Threshold:    defaultPreallocThreshold,
			},
		},
		Prefill: PrefillConfig{WaitingQueueThreshold: defaultPrefillWaitingThreshold},
	}
}

var (
	_ requestcontrol.Admitter  = &Admitter{}
	_ fwkplugin.ConsumerPlugin = &Admitter{}
)

// Admitter rejects requests when no feasible endpoint remains for either P/D role.
type Admitter struct {
	typedName                 fwkplugin.TypedName
	config                    Config
	metricsStalenessThreshold time.Duration
}

// Factory creates a P/D capacity admitter from plugin configuration.
func Factory(name string, rawParameters *json.Decoder, _ fwkplugin.Handle) (fwkplugin.Plugin, error) {
	config := DefaultConfig()
	if rawParameters != nil {
		if err := rawParameters.Decode(&config); err != nil {
			return nil, fmt.Errorf("failed to decode %s parameters: %w", PluginType, err)
		}
	}
	return New(name, config)
}

// New validates config and creates a P/D capacity admitter.
func New(name string, config Config) (*Admitter, error) {
	staleness, err := time.ParseDuration(config.MetricsStalenessThreshold)
	if err != nil || staleness <= 0 {
		return nil, fmt.Errorf("%s metricsStalenessThreshold must be a positive duration, got %q", PluginType, config.MetricsStalenessThreshold)
	}
	if err := validateDecodeConfig(config.Decode); err != nil {
		return nil, err
	}
	if config.Prefill.WaitingQueueThreshold <= 0 {
		return nil, fmt.Errorf("%s prefill.waitingQueueThreshold must be positive, got %d", PluginType, config.Prefill.WaitingQueueThreshold)
	}

	if name == "" {
		name = PluginType
	}
	return &Admitter{
		typedName:                 fwkplugin.TypedName{Type: PluginType, Name: name},
		config:                    config,
		metricsStalenessThreshold: staleness,
	}, nil
}

func validateDecodeConfig(config DecodeConfig) error {
	if config.WaitingQueueThreshold <= 0 {
		return fmt.Errorf("%s decode.waitingQueueThreshold must be positive, got %d", PluginType, config.WaitingQueueThreshold)
	}
	if config.KVCacheUtilizationThreshold <= 0 || config.KVCacheUtilizationThreshold > 1 {
		return fmt.Errorf("%s decode.kvCacheUtilizationThreshold must be in (0, 1], got %v", PluginType, config.KVCacheUtilizationThreshold)
	}
	if config.Prealloc.AttributeKey == "" {
		return fmt.Errorf("%s decode.prealloc.attributeKey must be non-empty", PluginType)
	}
	if config.Prealloc.Threshold <= 0 {
		return fmt.Errorf("%s decode.prealloc.threshold must be positive, got %v", PluginType, config.Prealloc.Threshold)
	}
	return nil
}

// TypedName returns the plugin type and instance name.
func (a *Admitter) TypedName() fwkplugin.TypedName {
	return a.typedName
}

// Consumes declares endpoint data needed by admission checks.
func (a *Admitter) Consumes() fwkplugin.DataDependencies {
	optional := map[fwkplugin.DataKey]any{
		fwkplugin.NewDataKey(a.config.Decode.Prealloc.AttributeKey, ""):                                        attrmetrics.ScalarMetricValue(0),
		fwkplugin.NewDataKey(attrmetrics.ScalarMetricUpdateTimeKey(a.config.Decode.Prealloc.AttributeKey), ""): attrmetrics.ScalarMetricUpdateTime{},
		fwkplugin.NewDataKey(attrmetrics.WaitingQueueUpdateTimeKey, ""):                                        attrmetrics.CoreMetricUpdateTime{},
		fwkplugin.NewDataKey(attrmetrics.KVCacheUtilizationUpdateTimeKey, ""):                                  attrmetrics.CoreMetricUpdateTime{},
	}
	return fwkplugin.DataDependencies{
		Optional: optional,
	}
}

// Admit rejects when every endpoint for either required role is unavailable.
func (a *Admitter) Admit(ctx context.Context, request *fwksched.InferenceRequest, endpoints []fwksched.Endpoint) error {
	if request == nil || (!a.config.RejectAllPriorities && request.Objectives.Priority >= 0) {
		return nil
	}

	now := time.Now()
	prefillTotal, prefillAvailable := 0, 0
	decodeTotal, decodeAvailable := 0, 0
	logger := log.FromContext(ctx)
	for _, endpoint := range endpoints {
		prefillRole, decodeRole := endpointRoles(endpoint)
		if prefillRole {
			prefillTotal++
			if ok, reason := a.prefillFeasible(endpoint, now); ok {
				prefillAvailable++
			} else {
				logger.V(logutil.DEBUG).Info("P/D capacity admitter excluded prefill endpoint", "endpoint", endpointName(endpoint), "reason", reason)
			}
		}
		if decodeRole {
			decodeTotal++
			if ok, reason := a.decodeFeasible(endpoint, now); ok {
				decodeAvailable++
			} else {
				logger.V(logutil.DEBUG).Info("P/D capacity admitter excluded decode endpoint", "endpoint", endpointName(endpoint), "reason", reason)
			}
		}
	}

	if prefillAvailable > 0 && decodeAvailable > 0 {
		return nil
	}

	reason := "no feasible prefill endpoint"
	if prefillAvailable > 0 {
		reason = "no feasible decode endpoint"
	} else if decodeAvailable == 0 {
		reason = "no feasible prefill or decode endpoint"
	}
	logger.Info("P/D capacity admission rejected request",
		"reason", reason,
		"prefillAvailable", prefillAvailable,
		"prefillTotal", prefillTotal,
		"decodeAvailable", decodeAvailable,
		"decodeTotal", decodeTotal)
	return errcommon.Error{Code: errcommon.ResourceExhausted, Msg: PluginType + ": " + reason}
}

func (a *Admitter) prefillFeasible(endpoint fwksched.Endpoint, now time.Time) (bool, string) {
	metrics := endpoint.GetMetrics()
	if metrics == nil {
		return false, "missing metrics"
	}
	if !a.coreMetricFresh(endpoint, attrmetrics.WaitingQueueUpdateTimeKey, now) {
		return false, "missing or stale ordinary waiting queue metric"
	}
	if metrics.WaitingQueueSize >= a.config.Prefill.WaitingQueueThreshold {
		return false, "ordinary waiting queue threshold reached"
	}
	return true, ""
}

func (a *Admitter) decodeFeasible(endpoint fwksched.Endpoint, now time.Time) (bool, string) {
	metrics := endpoint.GetMetrics()
	if metrics == nil {
		return false, "missing metrics"
	}
	if !a.coreMetricFresh(endpoint, attrmetrics.WaitingQueueUpdateTimeKey, now) {
		return false, "missing or stale ordinary waiting queue metric"
	}
	if metrics.WaitingQueueSize >= a.config.Decode.WaitingQueueThreshold {
		return false, "ordinary waiting queue threshold reached"
	}
	if !a.coreMetricFresh(endpoint, attrmetrics.KVCacheUtilizationUpdateTimeKey, now) {
		return false, "missing or stale KV utilization metric"
	}
	if metrics.KVCacheUsagePercent >= a.config.Decode.KVCacheUtilizationThreshold {
		return false, "KV utilization threshold reached"
	}
	prealloc, ok := a.readFreshSignal(endpoint, a.config.Decode.Prealloc.AttributeKey, now)
	if !ok {
		return false, "missing or stale decode preallocation metric"
	}
	if float64(prealloc) >= a.config.Decode.Prealloc.Threshold {
		return false, "decode preallocation threshold reached"
	}
	return true, ""
}

func (a *Admitter) coreMetricFresh(endpoint fwksched.Endpoint, key string, now time.Time) bool {
	updatedAt, ok := attrmetrics.ReadCoreMetricUpdateTime(endpoint, key)
	return ok && !updatedAt.IsZero() && now.Sub(updatedAt) <= a.metricsStalenessThreshold
}

func (a *Admitter) readFreshSignal(endpoint fwksched.Endpoint, key string, now time.Time) (attrmetrics.ScalarMetricValue, bool) {
	value, ok := attrmetrics.ReadScalarMetricValue(endpoint, key)
	if !ok {
		return 0, false
	}
	updatedAt, ok := attrmetrics.ReadScalarMetricUpdateTime(endpoint, key)
	if !ok || updatedAt.IsZero() || now.Sub(updatedAt) > a.metricsStalenessThreshold {
		return 0, false
	}
	return value, true
}

func endpointRoles(endpoint fwksched.Endpoint) (bool, bool) {
	if endpoint == nil || endpoint.GetMetadata() == nil {
		return false, false
	}
	role, ok := endpoint.GetMetadata().Labels[bylabel.RoleLabel]
	if !ok {
		return false, false
	}
	switch role {
	case bylabel.RolePrefill:
		return true, false
	case bylabel.RoleDecode:
		return false, true
	case bylabel.RolePrefillDecode, legacyRoleBoth, bylabel.RoleEncodePrefillDecode:
		return true, true
	case bylabel.RoleEncodePrefill:
		return true, false
	default:
		return false, false
	}
}

func endpointName(endpoint fwksched.Endpoint) string {
	if endpoint == nil || endpoint.GetMetadata() == nil {
		return ""
	}
	return endpoint.GetMetadata().Name
}
