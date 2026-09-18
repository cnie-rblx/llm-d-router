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
	"math"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	attrmetrics "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/metrics"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/scheduling/filter/bylabel"
)

func breakerPool(prefillWaiting, decodeWaiting int, kv, prealloc float64) []fwksched.Endpoint {
	return []fwksched.Endpoint{
		endpoint("prefill", bylabel.RolePrefill, prefillWaiting, 0.1, 100000, 0, 0, time.Now()),
		endpoint("decode", bylabel.RoleDecode, decodeWaiting, kv, 100000, prealloc, 0, time.Now()),
	}
}

func TestDeploymentHysteresis(t *testing.T) {
	tests := []struct {
		name                              string
		trip, boundary, middle, recovered []fwksched.Endpoint
	}{
		{"prefill queue", breakerPool(4, 0, .1, 0), breakerPool(2, 0, .1, 0), breakerPool(3, 0, .1, 0), breakerPool(1, 0, .1, 0)},
		{"decode queue", breakerPool(0, 4, .1, 0), breakerPool(0, 2, .1, 0), breakerPool(0, 3, .1, 0), breakerPool(0, 1, .1, 0)},
		{"kv", breakerPool(0, 0, .92, 0), breakerPool(0, 0, .85, 0), breakerPool(0, 0, .89, 0), breakerPool(0, 0, .84, 0)},
		{"prealloc", breakerPool(0, 0, .1, 8), breakerPool(0, 0, .1, 4), breakerPool(0, 0, .1, 6), breakerPool(0, 0, .1, 3)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a, err := New(t.Name(), DefaultConfig())
			require.NoError(t, err)
			req := request(1, 1, 0)
			ctx := context.Background()
			require.NoError(t, a.Admit(ctx, req, tt.middle), "startup is closed and uses trip thresholds")
			require.Error(t, a.Admit(ctx, req, tt.trip))
			require.Error(t, a.Admit(ctx, req, tt.middle), "open state must survive the hysteresis band")
			require.Error(t, a.Admit(ctx, req, tt.boundary), "recovery requires strictly below the lower threshold")
			require.NoError(t, a.Admit(ctx, req, tt.recovered))
			require.NoError(t, a.Admit(ctx, req, tt.middle), "closed state must survive the hysteresis band")
		})
	}
}

func TestRecoveryRequiresBothRolesButNotEveryEndpoint(t *testing.T) {
	a, err := New(t.Name(), DefaultConfig())
	require.NoError(t, err)
	req := request(1, 1, 0)
	ctx := context.Background()
	require.Error(t, a.Admit(ctx, req, breakerPool(4, 4, .95, 9)))
	require.Error(t, a.Admit(ctx, req, breakerPool(3, 0, .1, 0)), "prefill has not recovered")
	require.Error(t, a.Admit(ctx, req, breakerPool(0, 3, .1, 0)), "decode has not recovered")
	pool := breakerPool(0, 0, .1, 0)
	pool = append(pool, endpoint("busy-decode", bylabel.RoleDecode, 9, .99, 100000, 99, 0, time.Now()))
	require.NoError(t, a.Admit(ctx, req, pool))
	require.Error(t, a.Admit(ctx, req, nil), "an empty pool opens the breaker")
	mixed := breakerPool(0, 0, .89, 0)
	mixed = append(mixed, endpoint("other-decode", bylabel.RoleDecode, 3, .1, 100000, 0, 0, time.Now()))
	require.Error(t, a.Admit(ctx, req, mixed), "one decode endpoint must satisfy all recovery checks")
	require.NoError(t, a.Admit(ctx, req, pool))
}

func TestInvalidMetricsDoNotAdmit(t *testing.T) {
	tests := []struct {
		name string
		pool []fwksched.Endpoint
	}{
		{"nan kv", breakerPool(0, 0, math.NaN(), 0)},
		{"nan prealloc", breakerPool(0, 0, .1, math.NaN())},
		{"negative kv", breakerPool(0, 0, -.1, 0)},
		{"negative prealloc", breakerPool(0, 0, .1, -1)},
		{"negative prefill queue", breakerPool(-1, 0, .1, 0)},
		{"negative decode queue", breakerPool(0, -1, .1, 0)},
		{"infinite kv", breakerPool(0, 0, math.Inf(-1), 0)},
		{"infinite prealloc", breakerPool(0, 0, .1, math.Inf(-1))},
		{"positive infinite kv", breakerPool(0, 0, math.Inf(1), 0)},
		{"positive infinite prealloc", breakerPool(0, 0, .1, math.Inf(1))},
		{"kv above one", breakerPool(0, 0, 1.1, 0)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a, err := New(t.Name(), DefaultConfig())
			require.NoError(t, err)
			require.Error(t, a.Admit(context.Background(), request(1, 1, 0), tt.pool))
		})
	}
}

func TestConfiguredRecoveryThresholds(t *testing.T) {
	config := DefaultConfig()
	config.Prefill.WaitingQueueRecoveryThreshold = 1
	config.Decode.WaitingQueueRecoveryThreshold = 1
	config.Decode.KVCacheUtilizationRecoveryThreshold = .7
	config.Decode.Prealloc.RecoveryThreshold = 2
	a, err := New(t.Name(), config)
	require.NoError(t, err)
	ctx, req := context.Background(), request(1, 1, 0)
	require.Error(t, a.Admit(ctx, req, breakerPool(0, 0, .95, 0)))
	for _, pool := range [][]fwksched.Endpoint{
		breakerPool(1, 0, .1, 0), breakerPool(0, 1, .1, 0),
		breakerPool(0, 0, .7, 0), breakerPool(0, 0, .1, 2),
	} {
		require.Error(t, a.Admit(ctx, req, pool))
	}
	require.NoError(t, a.Admit(ctx, req, breakerPool(0, 0, .69, 1)))
}

func TestRecoveryConfiguration(t *testing.T) {
	for _, parameters := range []string{
		`{"decode":{"waitingQueueRecoveryThreshold":0}}`,
		`{"decode":{"waitingQueueRecoveryThreshold":4}}`,
		`{"decode":{"kvCacheUtilizationRecoveryThreshold":0.92}}`,
		`{"decode":{"prealloc":{"recoveryThreshold":9}}}`,
		`{"prefill":{"waitingQueueRecoveryThreshold":-1}}`,
	} {
		_, err := Factory("test", fwkplugin.StrictDecoder(json.RawMessage(parameters)), nil)
		require.Error(t, err, parameters)
	}
	_, err := Factory("test", fwkplugin.StrictDecoder(json.RawMessage(`{
		"decode":{"waitingQueueRecoveryThreshold":1,"kvCacheUtilizationRecoveryThreshold":0.7,"prealloc":{"recoveryThreshold":2}},
		"prefill":{"waitingQueueRecoveryThreshold":1}}`)), nil)
	require.NoError(t, err)
	config := DefaultConfig()
	config.Decode.KVCacheUtilizationThreshold = math.NaN()
	_, err = New("test", config)
	require.Error(t, err)
	config = DefaultConfig()
	config.Decode.Prealloc.RecoveryThreshold = math.Inf(1)
	_, err = New("test", config)
	require.Error(t, err)
}

func TestRecoveryRequiresValidFreshSamples(t *testing.T) {
	for _, key := range []string{attrmetrics.WaitingQueueSampleKey, attrmetrics.KVCacheUtilizationSampleKey, attrmetrics.ScalarMetricSampleKey(preallocKey)} {
		for _, sample := range []attrmetrics.MetricSample{
			{Value: 0, UpdatedAt: time.Now().Add(-time.Minute)},
			{Value: 0, UpdatedAt: time.Time{}},
			{Value: 0, UpdatedAt: time.Now().Add(time.Minute)},
			{Value: math.NaN(), UpdatedAt: time.Now()},
		} {
			a, err := New(t.Name(), DefaultConfig())
			require.NoError(t, err)
			req := request(1, 1, 0)
			require.Error(t, a.Admit(context.Background(), req, breakerPool(0, 0, .95, 0)))
			pool := breakerPool(0, 0, .1, 0)
			pool[1].Put(key, sample)
			require.Error(t, a.Admit(context.Background(), req, pool))
			require.NoError(t, a.Admit(context.Background(), req, breakerPool(0, 0, .1, 0)))
		}
	}
}

func TestAdmissionUsesSamplesNotSeparatelyPublishedCoreValues(t *testing.T) {
	a, err := New(t.Name(), DefaultConfig())
	require.NoError(t, err)
	pool := breakerPool(0, 0, .1, 0)
	pool[1].Put(attrmetrics.KVCacheUtilizationSampleKey, attrmetrics.MetricSample{Value: .95, UpdatedAt: time.Now()})
	require.Error(t, a.Admit(context.Background(), request(1, 1, 0), pool), "old healthy core values must not override an overloaded coherent sample")
}

func TestPoolSnapshotAndStateAreSerialized(t *testing.T) {
	a, err := New(t.Name(), DefaultConfig())
	require.NoError(t, err)
	req := request(1, 1, 0)
	ctx := context.Background()
	require.Equal(t, float64(0), testutil.ToFloat64(breakerOpen.WithLabelValues(t.Name())))
	require.Error(t, a.Admit(ctx, req, breakerPool(0, 0, .95, 0)))
	require.Equal(t, float64(1), testutil.ToFloat64(breakerOpen.WithLabelValues(t.Name())))
	// Unsynchronized callback state makes -race verify that snapshots are taken under the same lock.
	calls := 0
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			err := a.AdmitPool(ctx, req, func() []fwksched.Endpoint {
				calls++
				return breakerPool(0, 0, .89, 0)
			})
			if err == nil {
				t.Error("concurrent requests must not close a breaker in the hysteresis band")
			}
		})
	}
	wg.Wait()
	require.Equal(t, 32, calls)
	require.NoError(t, a.AdmitPool(ctx, req, func() []fwksched.Endpoint {
		if a.mu.TryLock() {
			a.mu.Unlock()
			t.Error("the state lock must be held before capturing the pool snapshot")
		}
		return breakerPool(0, 0, .1, 0)
	}))
	require.Equal(t, float64(0), testutil.ToFloat64(breakerOpen.WithLabelValues(t.Name())))
}

func TestPriorityBypassDoesNotChangeBreakerState(t *testing.T) {
	config := DefaultConfig()
	config.RejectAllPriorities = false
	a, err := New(t.Name(), config)
	require.NoError(t, err)
	ctx := context.Background()
	require.Error(t, a.Admit(ctx, request(1, 1, -1), breakerPool(0, 0, .95, 0)))
	require.NoError(t, a.AdmitPool(ctx, request(1, 1, 0), func() []fwksched.Endpoint {
		t.Fatal("bypassed requests must not evaluate or mutate breaker state")
		return nil
	}))
	require.Error(t, a.Admit(ctx, request(1, 1, -1), breakerPool(0, 0, .89, 0)))
}
