/*
Copyright 2025 The Kubernetes Authors.

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

package requestcontrol

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkrc "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requestcontrol"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
)

type poolScopeTestAdmitter struct {
	*mockAdmissionPlugin
	evaluate func(func() []fwksched.Endpoint) error
}

func (p *poolScopeTestAdmitter) AdmitPool(_ context.Context, _ *fwksched.InferenceRequest, snapshot func() []fwksched.Endpoint) error {
	return p.evaluate(snapshot)
}

type candidateScopeTestAdmitter struct {
	*mockAdmissionPlugin
	seen []fwksched.Endpoint
}

func (p *candidateScopeTestAdmitter) Admit(_ context.Context, _ *fwksched.InferenceRequest, endpoints []fwksched.Endpoint) error {
	p.seen = endpoints
	return nil
}

func TestPoolAdmissionUsesLazyFullPool(t *testing.T) {
	ep := func(name string) fwkdl.Endpoint {
		return fwkdl.NewEndpoint(&fwkdl.EndpointMetadata{Name: name}, fwkdl.NewMetrics())
	}
	ds := &mockDatastore{pods: []fwkdl.Endpoint{ep("prefill"), ep("decode")}}
	called := false
	pool := &poolScopeTestAdmitter{mockAdmissionPlugin: newMockAdmissionPlugin("pool", nil)}
	pool.evaluate = func(snapshot func() []fwksched.Endpoint) error {
		called = true
		ds.pods = append(ds.pods, ep("new-decode"))
		got := snapshot()
		require.Len(t, got, 3, "snapshot must be captured lazily, independent of the request subset")
		require.Equal(t, "new-decode", got[2].GetMetadata().Name)
		return nil
	}
	candidate := &candidateScopeTestAdmitter{mockAdmissionPlugin: newMockAdmissionPlugin("candidate", nil)}
	d := &Director{datastore: ds, requestControlPlugins: Config{admissionPlugins: []fwkrc.Admitter{pool, candidate}}}
	subset := d.toSchedulerEndpoints(ds.pods[:1])
	require.NoError(t, d.runAdmissionPlugins(context.Background(), &fwksched.InferenceRequest{}, subset))
	require.True(t, called)
	require.Equal(t, subset, candidate.seen, "ordinary admitters still use request candidates")
	require.Len(t, subset, 1, "pool evaluation must not replace scheduling candidates")
}

func TestPoolAdmissionCanRejectWithoutTakingSnapshot(t *testing.T) {
	denied := errors.New("denied without reading pool")
	pool := &poolScopeTestAdmitter{mockAdmissionPlugin: newMockAdmissionPlugin("pool", nil), evaluate: func(func() []fwksched.Endpoint) error { return denied }}
	d := &Director{requestControlPlugins: Config{admissionPlugins: []fwkrc.Admitter{pool}}}
	require.ErrorIs(t, d.runAdmissionPlugins(context.Background(), &fwksched.InferenceRequest{}, nil), denied)
}
