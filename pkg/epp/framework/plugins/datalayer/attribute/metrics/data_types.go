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

package metrics

import (
	"time"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
)

const (
	WaitingQueueSampleKey       = "core-metrics.waiting-queue.sample"
	KVCacheUtilizationSampleKey = "core-metrics.kv-cache-utilization.sample"
)

// ScalarMetricValue is a numeric endpoint attribute extracted from a configured scalar metric.
type ScalarMetricValue float64

func (v ScalarMetricValue) Clone() fwkdl.Cloneable { return v }

func ReadScalarMetricValue(attrs fwkdl.AttributeMap, key string) (ScalarMetricValue, bool) {
	return fwkdl.ReadAttribute[ScalarMetricValue](attrs, key)
}

// MetricSample keeps one signal's value and observation time in a single immutable attribute.
type MetricSample struct {
	Value     float64
	UpdatedAt time.Time
}

func (s MetricSample) Clone() fwkdl.Cloneable { return s }

func ScalarMetricSampleKey(key string) string { return key + ".sample" }

func ReadMetricSample(attrs fwkdl.AttributeMap, key string) (MetricSample, bool) {
	return fwkdl.ReadAttribute[MetricSample](attrs, key)
}
