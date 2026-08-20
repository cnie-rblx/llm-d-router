/*
Copyright 2026 The llm-d Authors.

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

package datalayer

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	attrconcurrency "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/concurrency"
)

const testEndpointID = "test-ns/worker-rank-0"

func sumInFlightLoads(values []any) any {
	total := &attrconcurrency.InFlightLoad{}
	for _, value := range values {
		load, ok := value.(*attrconcurrency.InFlightLoad)
		if !ok {
			continue
		}
		total.Requests += load.Requests
		total.Tokens += load.Tokens
	}
	return total
}

func TestKubernetesSyncerAggregatesTwoReplicas(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	now := time.Date(2026, 8, 20, 1, 2, 3, 0, time.UTC)

	first := newKubernetesSyncer("shared-load", "test-ns", "gate2c", "epp-0", client, time.Second, 3*time.Second)
	second := newKubernetesSyncer("shared-load", "test-ns", "gate2c", "epp-1", client, time.Second, 3*time.Second)
	first.now = func() time.Time { return now }
	second.now = func() time.Time { return now }

	require.NoError(t, first.Set(ctx, fwkdl.StateKey("inflight:test"), testEndpointID,
		&attrconcurrency.InFlightLoad{Requests: 1, Tokens: 100}))
	require.NoError(t, second.Set(ctx, fwkdl.StateKey("inflight:test"), testEndpointID,
		&attrconcurrency.InFlightLoad{Requests: 2, Tokens: 250}))
	require.NoError(t, first.flush(ctx))
	require.NoError(t, second.flush(ctx))
	require.NoError(t, first.refresh(ctx))

	value, found, err := first.Get(ctx, fwkdl.StateKey("inflight:test"), testEndpointID, sumInFlightLoads)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, &attrconcurrency.InFlightLoad{Requests: 3, Tokens: 350}, value)
}

func TestKubernetesSyncerExcludesStaleReplica(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	base := time.Date(2026, 8, 20, 1, 2, 3, 0, time.UTC)

	first := newKubernetesSyncer("shared-load", "test-ns", "gate2c", "epp-0", client, time.Second, 2*time.Second)
	second := newKubernetesSyncer("shared-load", "test-ns", "gate2c", "epp-1", client, time.Second, 2*time.Second)
	first.now = func() time.Time { return base }
	second.now = func() time.Time { return base }

	require.NoError(t, first.Set(ctx, fwkdl.StateKey("inflight:test"), testEndpointID,
		&attrconcurrency.InFlightLoad{Requests: 1, Tokens: 100}))
	require.NoError(t, second.Set(ctx, fwkdl.StateKey("inflight:test"), testEndpointID,
		&attrconcurrency.InFlightLoad{Requests: 9, Tokens: 900}))
	require.NoError(t, first.flush(ctx))
	require.NoError(t, second.flush(ctx))

	first.now = func() time.Time { return base.Add(3 * time.Second) }
	require.NoError(t, first.refresh(ctx))

	value, found, err := first.Get(ctx, fwkdl.StateKey("inflight:test"), testEndpointID, sumInFlightLoads)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, &attrconcurrency.InFlightLoad{Requests: 1, Tokens: 100}, value)
}

func TestKubernetesSyncerBatchesEndpointsInOneConfigMap(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	syncer := newKubernetesSyncer("shared-load", "test-ns", "gate2c", "epp-0", client, time.Second, 3*time.Second)

	require.NoError(t, syncer.Set(ctx, fwkdl.StateKey("inflight:test"), "test-ns/rank-0",
		&attrconcurrency.InFlightLoad{Requests: 1}))
	require.NoError(t, syncer.Set(ctx, fwkdl.StateKey("inflight:test"), "test-ns/rank-1",
		&attrconcurrency.InFlightLoad{Requests: 2}))
	require.NoError(t, syncer.flush(ctx))

	configMaps, err := client.CoreV1().ConfigMaps("test-ns").List(ctx, metav1.ListOptions{})
	require.NoError(t, err)
	require.Len(t, configMaps.Items, 1)
	assert.Len(t, client.Actions(), 2, "one create plus the verification list")
}
