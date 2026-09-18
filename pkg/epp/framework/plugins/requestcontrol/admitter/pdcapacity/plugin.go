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
	"math"
	"sync"
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
	AttributeKey      string  `json:"attributeKey"`
	Threshold         float64 `json:"threshold"`
	RecoveryThreshold float64 `json:"recoveryThreshold"`
}

// DecodeConfig configures decode capacity checks.
type DecodeConfig struct {
	WaitingQueueThreshold               int          `json:"waitingQueueThreshold"`
	KVCacheUtilizationThreshold         float64      `json:"kvCacheUtilizationThreshold"`
	WaitingQueueRecoveryThreshold       int          `json:"waitingQueueRecoveryThreshold"`
	KVCacheUtilizationRecoveryThreshold float64      `json:"kvCacheUtilizationRecoveryThreshold"`
	Prealloc                            SignalConfig `json:"prealloc"`
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
	WaitingQueueThreshold         int `json:"waitingQueueThreshold"`
	WaitingQueueRecoveryThreshold int `json:"waitingQueueRecoveryThreshold"`
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
			WaitingQueueThreshold:               defaultDecodeWaitingThreshold,
			KVCacheUtilizationThreshold:         defaultDecodeKVThreshold,
			WaitingQueueRecoveryThreshold:       2,
			KVCacheUtilizationRecoveryThreshold: 0.85,
			Prealloc: SignalConfig{
				AttributeKey:      defaultPreallocAttributeKey,
				Threshold:         defaultPreallocThreshold,
				RecoveryThreshold: 4,
			},
		},
		Prefill: PrefillConfig{WaitingQueueThreshold: defaultPrefillWaitingThreshold, WaitingQueueRecoveryThreshold: 2},
	}
}

var (
	_ requestcontrol.Admitter           = &Admitter{}
	_ requestcontrol.PoolScopedAdmitter = &Admitter{}
	_ fwkplugin.ConsumerPlugin          = &Admitter{}
)

// Admitter rejects requests when no feasible endpoint remains for either P/D role.
type Admitter struct {
	typedName                 fwkplugin.TypedName
	config                    Config
	metricsStalenessThreshold time.Duration
	mu                        sync.Mutex
	open                      bool
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
	if err := validateRecovery("prefill.waitingQueueRecoveryThreshold", float64(config.Prefill.WaitingQueueRecoveryThreshold), float64(config.Prefill.WaitingQueueThreshold)); err != nil {
		return nil, err
	}

	if name == "" {
		name = PluginType
	}
	registerMetrics()
	breakerOpen.WithLabelValues(name).Set(0)
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
	if !finite(config.KVCacheUtilizationThreshold) || config.KVCacheUtilizationThreshold <= 0 || config.KVCacheUtilizationThreshold > 1 {
		return fmt.Errorf("%s decode.kvCacheUtilizationThreshold must be in (0, 1], got %v", PluginType, config.KVCacheUtilizationThreshold)
	}
	if config.Prealloc.AttributeKey == "" {
		return fmt.Errorf("%s decode.prealloc.attributeKey must be non-empty", PluginType)
	}
	if !finite(config.Prealloc.Threshold) || config.Prealloc.Threshold <= 0 {
		return fmt.Errorf("%s decode.prealloc.threshold must be positive, got %v", PluginType, config.Prealloc.Threshold)
	}
	for _, threshold := range []struct {
		name           string
		recovery, trip float64
	}{
		{"decode.waitingQueueRecoveryThreshold", float64(config.WaitingQueueRecoveryThreshold), float64(config.WaitingQueueThreshold)},
		{"decode.kvCacheUtilizationRecoveryThreshold", config.KVCacheUtilizationRecoveryThreshold, config.KVCacheUtilizationThreshold},
		{"decode.prealloc.recoveryThreshold", config.Prealloc.RecoveryThreshold, config.Prealloc.Threshold},
	} {
		if err := validateRecovery(threshold.name, threshold.recovery, threshold.trip); err != nil {
			return err
		}
	}
	return nil
}

func validateRecovery(name string, recovery, trip float64) error {
	if !finite(recovery) || recovery <= 0 || recovery >= trip {
		return fmt.Errorf("%s %s must be positive and less than its trip threshold, got %v", PluginType, name, recovery)
	}
	return nil
}

func finite(value float64) bool { return !math.IsNaN(value) && !math.IsInf(value, 0) }

// TypedName returns the plugin type and instance name.
func (a *Admitter) TypedName() fwkplugin.TypedName {
	return a.typedName
}

// Consumes declares endpoint data needed by admission checks.
func (a *Admitter) Consumes() fwkplugin.DataDependencies {
	optional := map[fwkplugin.DataKey]any{
		fwkplugin.NewDataKey(attrmetrics.ScalarMetricSampleKey(a.config.Decode.Prealloc.AttributeKey), ""): attrmetrics.MetricSample{},
		fwkplugin.NewDataKey(attrmetrics.WaitingQueueSampleKey, ""):                                        attrmetrics.MetricSample{},
		fwkplugin.NewDataKey(attrmetrics.KVCacheUtilizationSampleKey, ""):                                  attrmetrics.MetricSample{},
	}
	return fwkplugin.DataDependencies{
		Optional: optional,
	}
}

// Admit rejects when every endpoint for either required role is unavailable.
func (a *Admitter) Admit(ctx context.Context, request *fwksched.InferenceRequest, endpoints []fwksched.Endpoint) error {
	return a.AdmitPool(ctx, request, func() []fwksched.Endpoint { return endpoints })
}

// AdmitPool evaluates the full protected pool, independently of scheduling subsets.
func (a *Admitter) AdmitPool(ctx context.Context, request *fwksched.InferenceRequest, snapshot func() []fwksched.Endpoint) error {
	if request == nil || (!a.config.RejectAllPriorities && request.Objectives.Priority >= 0) {
		return nil
	}
	// Capture metrics under the state lock so queued callers do not replay older snapshots.
	a.mu.Lock()
	defer a.mu.Unlock()
	endpoints := snapshot()

	now := time.Now()
	prefillTotal, prefillAvailable := 0, 0
	decodeTotal, decodeAvailable := 0, 0
	logger := log.FromContext(ctx)
	for _, endpoint := range endpoints {
		prefillRole, decodeRole := endpointRoles(endpoint)
		if prefillRole {
			prefillTotal++
			if ok, reason := a.prefillFeasible(endpoint, now, a.open); ok {
				prefillAvailable++
			} else {
				logger.V(logutil.DEBUG).Info("P/D capacity admitter excluded prefill endpoint", "endpoint", endpointName(endpoint), "reason", reason)
			}
		}
		if decodeRole {
			decodeTotal++
			if ok, reason := a.decodeFeasible(endpoint, now, a.open); ok {
				decodeAvailable++
			} else {
				logger.V(logutil.DEBUG).Info("P/D capacity admitter excluded decode endpoint", "endpoint", endpointName(endpoint), "reason", reason)
			}
		}
	}

	if prefillAvailable > 0 && decodeAvailable > 0 {
		a.setOpen(ctx, false, "prefill and decode recovery thresholds satisfied")
		return nil
	}

	reason := "no feasible prefill endpoint"
	if prefillAvailable > 0 {
		reason = "no feasible decode endpoint"
	} else if decodeAvailable == 0 {
		reason = "no feasible prefill or decode endpoint"
	}
	if a.open {
		reason = "recovery pending: " + reason
	}
	a.setOpen(ctx, true, reason)
	logger.Info("P/D capacity admission rejected request",
		"reason", reason,
		"prefillAvailable", prefillAvailable,
		"prefillTotal", prefillTotal,
		"decodeAvailable", decodeAvailable,
		"decodeTotal", decodeTotal)
	return errcommon.Error{Code: errcommon.ResourceExhausted, Msg: PluginType + ": " + reason}
}

func (a *Admitter) prefillFeasible(endpoint fwksched.Endpoint, now time.Time, recovering bool) (bool, string) {
	waiting, ok := a.readFreshSample(endpoint, attrmetrics.WaitingQueueSampleKey, now)
	if !ok {
		return false, "missing, stale or invalid ordinary waiting queue metric"
	}
	threshold := a.config.Prefill.WaitingQueueThreshold
	if recovering {
		threshold = a.config.Prefill.WaitingQueueRecoveryThreshold
	}
	if waiting >= float64(threshold) {
		return false, "ordinary waiting queue threshold reached"
	}
	return true, ""
}

func (a *Admitter) decodeFeasible(endpoint fwksched.Endpoint, now time.Time, recovering bool) (bool, string) {
	waitingThreshold := a.config.Decode.WaitingQueueThreshold
	kvThreshold := a.config.Decode.KVCacheUtilizationThreshold
	preallocThreshold := a.config.Decode.Prealloc.Threshold
	if recovering {
		waitingThreshold = a.config.Decode.WaitingQueueRecoveryThreshold
		kvThreshold = a.config.Decode.KVCacheUtilizationRecoveryThreshold
		preallocThreshold = a.config.Decode.Prealloc.RecoveryThreshold
	}
	waiting, ok := a.readFreshSample(endpoint, attrmetrics.WaitingQueueSampleKey, now)
	if !ok {
		return false, "missing, stale or invalid ordinary waiting queue metric"
	}
	if waiting >= float64(waitingThreshold) {
		return false, "ordinary waiting queue threshold reached"
	}
	kv, ok := a.readFreshSample(endpoint, attrmetrics.KVCacheUtilizationSampleKey, now)
	if !ok || kv > 1 {
		return false, "missing, stale or invalid KV utilization metric"
	}
	if kv >= kvThreshold {
		return false, "KV utilization threshold reached"
	}
	prealloc, ok := a.readFreshSample(endpoint, attrmetrics.ScalarMetricSampleKey(a.config.Decode.Prealloc.AttributeKey), now)
	if !ok {
		return false, "missing, stale or invalid decode preallocation metric"
	}
	if prealloc >= preallocThreshold {
		return false, "decode preallocation threshold reached"
	}
	return true, ""
}

func (a *Admitter) readFreshSample(endpoint fwksched.Endpoint, key string, now time.Time) (float64, bool) {
	sample, ok := attrmetrics.ReadMetricSample(endpoint, key)
	return sample.Value, ok && finite(sample.Value) && sample.Value >= 0 && !sample.UpdatedAt.IsZero() &&
		!sample.UpdatedAt.After(now) && now.Sub(sample.UpdatedAt) <= a.metricsStalenessThreshold
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
