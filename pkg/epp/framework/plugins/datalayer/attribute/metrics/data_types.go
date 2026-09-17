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
	scalarMetricUpdateTimeSuffix = ".updateTime"
	// WaitingQueueUpdateTimeKey identifies the waiting-queue scrape timestamp.
	WaitingQueueUpdateTimeKey = "core-metrics.waiting-queue.updateTime"
	// KVCacheUtilizationUpdateTimeKey identifies the KV-utilization scrape timestamp.
	KVCacheUtilizationUpdateTimeKey = "core-metrics.kv-cache-utilization.updateTime"
)

// ScalarMetricValue is a numeric endpoint attribute extracted from a configured scalar metric.
type ScalarMetricValue float64

func (v ScalarMetricValue) Clone() fwkdl.Cloneable {
	return v
}

func ReadScalarMetricValue(attrs fwkdl.AttributeMap, key string) (ScalarMetricValue, bool) {
	return fwkdl.ReadAttribute[ScalarMetricValue](attrs, key)
}

// ScalarMetricUpdateTime records when a scalar endpoint metric was extracted.
type ScalarMetricUpdateTime time.Time

func (v ScalarMetricUpdateTime) Clone() fwkdl.Cloneable {
	return v
}

// ScalarMetricUpdateTimeKey returns the companion timestamp key for a scalar metric.
func ScalarMetricUpdateTimeKey(key string) string {
	return key + scalarMetricUpdateTimeSuffix
}

// ReadScalarMetricUpdateTime reads the companion timestamp for a scalar metric.
func ReadScalarMetricUpdateTime(attrs fwkdl.AttributeMap, key string) (time.Time, bool) {
	value, ok := fwkdl.ReadAttribute[ScalarMetricUpdateTime](attrs, ScalarMetricUpdateTimeKey(key))
	return time.Time(value), ok
}

// CoreMetricUpdateTime records when one core endpoint metric was extracted.
type CoreMetricUpdateTime time.Time

func (v CoreMetricUpdateTime) Clone() fwkdl.Cloneable {
	return v
}

// ReadCoreMetricUpdateTime reads a core endpoint metric timestamp.
func ReadCoreMetricUpdateTime(attrs fwkdl.AttributeMap, key string) (time.Time, bool) {
	value, ok := fwkdl.ReadAttribute[CoreMetricUpdateTime](attrs, key)
	return time.Time(value), ok
}
