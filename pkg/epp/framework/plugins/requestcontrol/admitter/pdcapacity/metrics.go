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
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	compbasemetrics "k8s.io/component-base/metrics"
	"sigs.k8s.io/controller-runtime/pkg/log"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	metricsutil "github.com/llm-d/llm-d-router/pkg/common/observability/metrics"
	eppmetrics "github.com/llm-d/llm-d-router/pkg/epp/metrics"
)

var breakerOpen = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Subsystem: eppmetrics.LLMDRouterEndpointPickerSubsystem,
	Name:      "pd_capacity_breaker_open",
	Help:      metricsutil.HelpMsgWithStability("P/D pool admission breaker state: 1 is open (rejecting), 0 is closed (admitting). Updated on evaluated requests.", compbasemetrics.ALPHA),
}, []string{"plugin_name"})

var registerMetricsOnce sync.Once

func registerMetrics() {
	registerMetricsOnce.Do(func() { ctrlmetrics.Registry.MustRegister(breakerOpen) })
}

// setOpen is called with mu held.
func (a *Admitter) setOpen(ctx context.Context, open bool, reason string) {
	if a.open == open {
		return
	}
	a.open = open
	value := float64(0)
	if open {
		value = 1
	}
	breakerOpen.WithLabelValues(a.typedName.Name).Set(value)
	log.FromContext(ctx).Info("P/D capacity breaker state changed", "plugin", a.typedName.Name, "open", open, "reason", reason)
}
