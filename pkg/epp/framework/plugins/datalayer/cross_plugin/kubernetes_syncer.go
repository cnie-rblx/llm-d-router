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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/log"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
)

const (
	KubernetesSyncerType = "kubernetes-syncer"

	defaultFlushInterval = 500 * time.Millisecond
	defaultSnapshotTTL   = 2 * time.Second
	snapshotDataKey      = "snapshot.json"
	groupLabelKey        = "llm-d.ai/cross-replica-sync-group"
	syncerLabelKey       = "llm-d.ai/cross-replica-syncer"
)

type kubernetesSyncerConfig struct {
	Namespace     string `json:"namespace"`
	Group         string `json:"group"`
	FlushInterval string `json:"flushInterval,omitempty"`
	SnapshotTTL   string `json:"snapshotTTL,omitempty"`
}

type wireSnapshot struct {
	ReplicaID string                     `json:"replicaID"`
	UpdatedAt time.Time                  `json:"updatedAt"`
	Values    map[string]json.RawMessage `json:"values"`
}

// KubernetesSyncer batches one replica's state into a ConfigMap and maintains
// an in-memory cache of peer snapshots. Kubernetes API calls are never made by
// Get, so request scheduling remains independent of API-server latency.
type KubernetesSyncer struct {
	typedName     fwkplugin.TypedName
	namespace     string
	group         string
	replicaID     string
	configMapName string
	client        kubernetes.Interface
	flushInterval time.Duration
	snapshotTTL   time.Duration
	now           func() time.Time
	created       atomic.Bool

	mu         sync.RWMutex
	local      map[string]any
	valueTypes map[string]reflect.Type
	peers      map[string]wireSnapshot
}

var _ fwkdl.CrossReplicaSyncer = (*KubernetesSyncer)(nil)

func newKubernetesSyncer(
	name, namespace, group, replicaID string,
	client kubernetes.Interface,
	flushInterval, snapshotTTL time.Duration,
) *KubernetesSyncer {
	digest := sha256.Sum256([]byte(group + "\x00" + replicaID))
	return &KubernetesSyncer{
		typedName:     fwkplugin.TypedName{Type: KubernetesSyncerType, Name: name},
		namespace:     namespace,
		group:         group,
		replicaID:     replicaID,
		configMapName: "llmd-epp-sync-" + hex.EncodeToString(digest[:8]),
		client:        client,
		flushInterval: flushInterval,
		snapshotTTL:   snapshotTTL,
		now:           time.Now,
		local:         map[string]any{},
		valueTypes:    map[string]reflect.Type{},
		peers:         map[string]wireSnapshot{},
	}
}

func KubernetesSyncerFactory(name string, decoder *json.Decoder, handle fwkplugin.Handle) (fwkplugin.Plugin, error) {
	if handle == nil {
		return nil, errors.New("handle is nil")
	}

	cfg := kubernetesSyncerConfig{
		Namespace:     os.Getenv("POD_NAMESPACE"),
		Group:         os.Getenv("EPP_SYNC_GROUP"),
		FlushInterval: defaultFlushInterval.String(),
		SnapshotTTL:   defaultSnapshotTTL.String(),
	}
	if decoder != nil {
		if err := decoder.Decode(&cfg); err != nil {
			return nil, fmt.Errorf("decode kubernetes syncer config: %w", err)
		}
	}
	if cfg.Namespace == "" {
		return nil, errors.New("kubernetes syncer namespace is required")
	}
	if cfg.Group == "" {
		return nil, errors.New("kubernetes syncer group is required")
	}
	flushInterval, err := time.ParseDuration(cfg.FlushInterval)
	if err != nil || flushInterval <= 0 {
		return nil, fmt.Errorf("invalid flushInterval %q", cfg.FlushInterval)
	}
	snapshotTTL, err := time.ParseDuration(cfg.SnapshotTTL)
	if err != nil || snapshotTTL < 2*flushInterval {
		return nil, fmt.Errorf("snapshotTTL %q must be at least twice flushInterval", cfg.SnapshotTTL)
	}

	replicaID := os.Getenv("POD_NAME")
	if replicaID == "" {
		replicaID, _ = os.Hostname()
	}
	if replicaID == "" {
		return nil, errors.New("kubernetes syncer replica identity is required")
	}
	restConfig, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("load in-cluster Kubernetes config: %w", err)
	}
	client, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("create Kubernetes client: %w", err)
	}

	syncer := newKubernetesSyncer(name, cfg.Namespace, cfg.Group, replicaID, client, flushInterval, snapshotTTL)
	ctx := handle.Context()
	if ctx == nil {
		ctx = context.Background()
	}
	go syncer.run(ctx)
	return syncer, nil
}

func (s *KubernetesSyncer) TypedName() fwkplugin.TypedName { return s.typedName }

func stateAddress(key fwkdl.StateKey, endpointID string) string {
	return string(key) + "\x00" + endpointID
}

func (s *KubernetesSyncer) Set(_ context.Context, key fwkdl.StateKey, endpointID string, value any) error {
	if value == nil {
		return errors.New("cross-replica value must not be nil")
	}
	address := stateAddress(key, endpointID)
	s.mu.Lock()
	s.local[address] = value
	s.valueTypes[address] = reflect.TypeOf(value)
	s.mu.Unlock()
	return nil
}

func (s *KubernetesSyncer) Get(
	_ context.Context,
	key fwkdl.StateKey,
	endpointID string,
	aggregate func([]any) any,
) (any, bool, error) {
	address := stateAddress(key, endpointID)
	now := s.now()

	s.mu.RLock()
	valueType, hasType := s.valueTypes[address]
	values := make([]any, 0, len(s.peers)+1)
	if local, ok := s.local[address]; ok {
		values = append(values, cloneValue(local))
	}
	for replicaID, snapshot := range s.peers {
		if replicaID == s.replicaID || now.Sub(snapshot.UpdatedAt) > s.snapshotTTL {
			continue
		}
		raw, ok := snapshot.Values[address]
		if !ok || !hasType {
			continue
		}
		decoded, err := decodeValue(raw, valueType)
		if err != nil {
			s.mu.RUnlock()
			return nil, false, fmt.Errorf("decode state from replica %s: %w", replicaID, err)
		}
		values = append(values, decoded)
	}
	s.mu.RUnlock()

	if len(values) == 0 {
		return nil, false, nil
	}
	return aggregate(values), true, nil
}

func (s *KubernetesSyncer) Delete(_ context.Context, key fwkdl.StateKey, endpointID string) error {
	address := stateAddress(key, endpointID)
	s.mu.Lock()
	delete(s.local, address)
	delete(s.valueTypes, address)
	s.mu.Unlock()
	return nil
}

func (s *KubernetesSyncer) run(ctx context.Context) {
	ticker := time.NewTicker(s.flushInterval)
	defer ticker.Stop()
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := s.client.CoreV1().ConfigMaps(s.namespace).Delete(cleanupCtx, s.configMapName, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			log.FromContext(ctx).Error(err, "delete cross-replica snapshot", "configMap", s.configMapName)
		}
	}()

	for {
		if err := s.flush(ctx); err != nil {
			log.FromContext(ctx).Error(err, "publish cross-replica snapshot", "configMap", s.configMapName)
		} else if err := s.refresh(ctx); err != nil {
			log.FromContext(ctx).Error(err, "refresh cross-replica snapshots", "group", s.group)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *KubernetesSyncer) flush(ctx context.Context) error {
	snapshot := wireSnapshot{
		ReplicaID: s.replicaID,
		UpdatedAt: s.now().UTC(),
		Values:    map[string]json.RawMessage{},
	}
	s.mu.RLock()
	for address, value := range s.local {
		raw, err := json.Marshal(value)
		if err != nil {
			s.mu.RUnlock()
			return fmt.Errorf("encode state %q: %w", address, err)
		}
		snapshot.Values[address] = raw
	}
	s.mu.RUnlock()

	rawSnapshot, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("encode snapshot: %w", err)
	}
	configMaps := s.client.CoreV1().ConfigMaps(s.namespace)
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      s.configMapName,
			Namespace: s.namespace,
			Labels: map[string]string{
				groupLabelKey:  s.group,
				syncerLabelKey: s.typedName.Name,
			},
		},
		Data: map[string]string{snapshotDataKey: string(rawSnapshot)},
	}

	if !s.created.Load() {
		if _, err = configMaps.Create(ctx, configMap, metav1.CreateOptions{}); err == nil {
			s.created.Store(true)
			return nil
		} else if !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("create snapshot ConfigMap: %w", err)
		}
		s.created.Store(true)
	}

	existing, err := configMaps.Get(ctx, s.configMapName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			s.created.Store(false)
			return s.flush(ctx)
		}
		return fmt.Errorf("get snapshot ConfigMap: %w", err)
	}
	configMap.ResourceVersion = existing.ResourceVersion
	if _, err = configMaps.Update(ctx, configMap, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("update snapshot ConfigMap: %w", err)
	}
	return nil
}

func (s *KubernetesSyncer) refresh(ctx context.Context) error {
	selector := labels.Set{
		groupLabelKey:  s.group,
		syncerLabelKey: s.typedName.Name,
	}.String()
	configMaps, err := s.client.CoreV1().ConfigMaps(s.namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return fmt.Errorf("list snapshot ConfigMaps: %w", err)
	}

	now := s.now()
	peers := make(map[string]wireSnapshot, len(configMaps.Items))
	for i := range configMaps.Items {
		raw := configMaps.Items[i].Data[snapshotDataKey]
		if raw == "" {
			continue
		}
		var snapshot wireSnapshot
		if err := json.Unmarshal([]byte(raw), &snapshot); err != nil {
			continue
		}
		if snapshot.ReplicaID == "" || now.Sub(snapshot.UpdatedAt) > s.snapshotTTL {
			continue
		}
		peers[snapshot.ReplicaID] = snapshot
	}
	s.mu.Lock()
	s.peers = peers
	s.mu.Unlock()
	return nil
}

func cloneValue(value any) any {
	if cloneable, ok := value.(fwkdl.Cloneable); ok {
		return cloneable.Clone()
	}
	return value
}

func decodeValue(raw json.RawMessage, valueType reflect.Type) (any, error) {
	if valueType.Kind() == reflect.Pointer {
		value := reflect.New(valueType.Elem())
		if err := json.Unmarshal(raw, value.Interface()); err != nil {
			return nil, err
		}
		return value.Interface(), nil
	}
	value := reflect.New(valueType)
	if err := json.Unmarshal(raw, value.Interface()); err != nil {
		return nil, err
	}
	return value.Elem().Interface(), nil
}
