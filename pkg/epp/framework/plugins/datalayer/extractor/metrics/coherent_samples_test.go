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

package metrics

import (
	"context"
	"math"
	"sync"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"k8s.io/utils/ptr"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	attrmetrics "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/metrics"
	sourcemetrics "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/source/metrics"
)

type sampleObservingEndpoint struct {
	fwkdl.Endpoint
	beforePublish func()
}

func (ep *sampleObservingEndpoint) UpdateMetrics(m *fwkdl.Metrics) {
	ep.beforePublish()
	ep.Endpoint.UpdateMetrics(m)
}

func TestExtractionPublishesCoherentSamples(t *testing.T) {
	ext := buildExtractor(t, &modelServerExtractorParams{EngineConfigs: []engineConfigParams{{Name: "vllm", QueuedRequestsSpec: "queue", KVUsageSpec: "kv", CustomMetrics: []customMetricConfigParams{{AttributeKey: "prealloc", MetricSpec: "prealloc"}}}}})
	base := fwkdl.NewEndpoint(&fwkdl.EndpointMetadata{Labels: map[string]string{DefaultEngineTypeLabelKey: "vllm"}}, fwkdl.NewMetrics())
	observed := false
	ep := &sampleObservingEndpoint{Endpoint: base, beforePublish: func() {
		observed = true
		snapshot := fwksched.NewEndpoint(base.GetMetadata(), base.GetMetrics(), base.GetAttributes())
		require.Zero(t, snapshot.GetMetrics().WaitingQueueSize, "legacy pointer has not yet been published")
		for key, want := range map[string]float64{attrmetrics.WaitingQueueSampleKey: 7, attrmetrics.KVCacheUtilizationSampleKey: .95, attrmetrics.ScalarMetricSampleKey("prealloc"): 9} {
			sample, ok := attrmetrics.ReadMetricSample(snapshot, key)
			require.True(t, ok)
			require.Equal(t, want, sample.Value)
			require.False(t, sample.UpdatedAt.IsZero())
		}
	}}
	gauge := func(v float64) *dto.MetricFamily {
		return &dto.MetricFamily{Type: dto.MetricType_GAUGE.Enum(), Metric: []*dto.Metric{{Gauge: &dto.Gauge{Value: ptr.To(v)}}}}
	}
	payload := sourcemetrics.PrometheusMetricMap{"queue": gauge(7), "kv": gauge(.95), "prealloc": gauge(9)}
	require.NoError(t, ext.Extract(context.Background(), fwkdl.PollInput[sourcemetrics.PrometheusMetricMap]{Endpoint: ep, Payload: payload}))
	require.True(t, observed)
	queueBefore, _ := attrmetrics.ReadMetricSample(base.GetAttributes(), attrmetrics.WaitingQueueSampleKey)
	delete(payload, "queue")
	payload["kv"] = gauge(math.NaN())
	payload["prealloc"] = gauge(-1)
	require.Error(t, ext.Extract(context.Background(), fwkdl.PollInput[sourcemetrics.PrometheusMetricMap]{Endpoint: base, Payload: payload}))
	queueAfter, ok := attrmetrics.ReadMetricSample(base.GetAttributes(), attrmetrics.WaitingQueueSampleKey)
	require.True(t, ok)
	require.Equal(t, queueBefore, queueAfter, "partial scrape must not refresh an absent signal")
	kv, _ := attrmetrics.ReadMetricSample(base.GetAttributes(), attrmetrics.KVCacheUtilizationSampleKey)
	require.True(t, math.IsNaN(kv.Value), "invalid samples must replace old healthy values so admission can reject them")
	prealloc, _ := attrmetrics.ReadMetricSample(base.GetAttributes(), attrmetrics.ScalarMetricSampleKey("prealloc"))
	require.Equal(t, float64(-1), prealloc.Value)
}

func TestConcurrentMetricSampleSnapshots(t *testing.T) {
	attrs := fwkdl.NewAttributes()
	const key = "test.sample"
	attrs.Put(key, attrmetrics.MetricSample{Value: 1, UpdatedAt: time.Unix(1, 0)})
	var wg sync.WaitGroup
	wg.Go(func() {
		for i := 1; i <= 1000; i++ {
			attrs.Put(key, attrmetrics.MetricSample{Value: float64(i), UpdatedAt: time.Unix(int64(i), 0)})
		}
	})
	for range 4 {
		wg.Go(func() {
			for range 1000 {
				sample, ok := attrmetrics.ReadMetricSample(attrs.Clone(), key)
				if !ok || sample.Value != float64(sample.UpdatedAt.Unix()) {
					t.Error("snapshot combined a value with another sample's timestamp")
				}
			}
		})
	}
	wg.Wait()
}
