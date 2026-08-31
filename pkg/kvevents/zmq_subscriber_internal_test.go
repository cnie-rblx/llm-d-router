// Copyright 2026 The llm-d Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kvevents

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseReplayEventFrame(t *testing.T) {
	sequence := make([]byte, 8)
	binary.BigEndian.PutUint64(sequence, 42)
	payload := []byte("payload")
	fallbackTopic := "kv@@Assistant/glm_shadow-traffic"

	tests := []struct {
		name      string
		frames    [][]byte
		wantTopic string
		wantValid bool
	}{
		{
			name:      "topic-bearing",
			frames:    [][]byte{[]byte("kv@pod@model"), sequence, payload},
			wantTopic: "kv@pod@model",
			wantValid: true,
		},
		{
			name:      "upstream-topicless",
			frames:    [][]byte{sequence, payload},
			wantTopic: fallbackTopic,
			wantValid: true,
		},
		{
			name:      "short-sequence",
			frames:    [][]byte{{1}, payload},
			wantValid: false,
		},
		{
			name:      "unexpected-frame-count",
			frames:    [][]byte{payload},
			wantValid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			topic, seq, gotPayload, ok := parseReplayEventFrame(tt.frames, fallbackTopic)
			assert.Equal(t, tt.wantValid, ok)
			if !tt.wantValid {
				return
			}
			assert.Equal(t, tt.wantTopic, topic)
			assert.Equal(t, uint64(42), seq)
			require.Equal(t, payload, gotPayload)
		})
	}
}

func TestReplayTerminalFrame(t *testing.T) {
	endSequence := make([]byte, 8)
	binary.BigEndian.PutUint64(endSequence, math.MaxUint64)

	tests := []struct {
		name   string
		frames [][]byte
		want   bool
	}{
		{name: "upstream-topicless", frames: [][]byte{endSequence, {}}, want: true},
		{name: "topic-bearing", frames: [][]byte{{}, endSequence, {}}, want: true},
		{name: "event", frames: [][]byte{[]byte("kv@pod@model"), make([]byte, 8), []byte("payload")}},
		{name: "empty-payload-not-end", frames: [][]byte{make([]byte, 8), {}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isReplayTerminalFrame(tt.frames))
		})
	}
}
