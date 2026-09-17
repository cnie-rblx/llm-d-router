// Copyright 2025 The llm-d Authors.
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
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"time"

	zmq4 "github.com/go-zeromq/zmq4"
	"golang.org/x/sync/semaphore"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/llm-d/llm-d-router/pkg/common/observability/logging"
	"github.com/llm-d/llm-d-router/pkg/kvcache/metrics"
)

const (
	retryInterval               = 5 * time.Second
	replayTimeout               = 2 * time.Minute
	replayAttemptIdleTimeout    = 2 * time.Second
	replayRetryBackoff          = 100 * time.Millisecond
	replayCooldown              = 30 * time.Second
	maxConcurrentReplay         = 2
	maxReplayNoProgressAttempts = 3
	maxReplayQueueDepth         = 512
	replayQueuePollInterval     = 5 * time.Millisecond
)

var processReplayLimiter = semaphore.NewWeighted(maxConcurrentReplay)

// zmqSubscriber connects to a ZMQ publisher and forwards messages to a pool.
type zmqSubscriber struct {
	pool           *Pool
	podIdentifier  string
	sourceEndpoint string
	endpoint       string
	replayEndpoint string
	remote         bool
	topicFilter    string

	// Replay state persists across reconnections within subscriber lifetime.
	lastSeq           uint64
	hasLastSeq        bool
	lastLiveSeq       uint64
	hasLastLiveSeq    bool
	lastReplayFailure time.Time
	// joinLiveAfterReplay allows one forward jump from an approximate,
	// re-anchored cold replay to the already-buffered live stream. Without it,
	// that expected handoff gap starts strict gap replay forever on a busy
	// engine whose replay cursor cannot catch the publisher.
	joinLiveAfterReplay bool
	// liveOnly stops cold-start replay attempts after one has failed, so the
	// index is rebuilt from live events instead of being cleared on every
	// cooldown. See the fallback in receiveLoop for why this is safe.
	liveOnly bool
}

// newZMQSubscriber creates a new ZMQ subscriber.
func newZMQSubscriber(
	pool *Pool,
	podIdentifier, sourceEndpoint, endpoint, replayEndpoint, topicFilter string,
	remote bool,
) *zmqSubscriber {
	return &zmqSubscriber{
		pool:           pool,
		podIdentifier:  podIdentifier,
		sourceEndpoint: sourceEndpoint,
		endpoint:       endpoint,
		replayEndpoint: replayEndpoint,
		remote:         remote,
		topicFilter:    topicFilter,
	}
}

// parseEventFrame validates and extracts a live or replayed event frame.
//
//nolint:gocritic // unnamedResult conflicts with nonamedreturns
func parseEventFrame(frames [][]byte) (string, uint64, []byte, bool) {
	if len(frames) != 3 || len(frames[1]) < 8 {
		return "", 0, nil, false
	}
	return string(frames[0]), binary.BigEndian.Uint64(frames[1]), frames[2], true
}

//nolint:gocritic // unnamedResult conflicts with nonamedreturns
func parseReplayEventFrame(frames [][]byte, fallbackTopic string) (string, uint64, []byte, bool) {
	if len(frames) == 2 {
		if len(frames[0]) < 8 {
			return "", 0, nil, false
		}
		return fallbackTopic, binary.BigEndian.Uint64(frames[0]), frames[1], true
	}
	return parseEventFrame(frames)
}

func isReplayTerminalFrame(frames [][]byte) bool {
	switch len(frames) {
	case 2:
		return len(frames[0]) >= 8 &&
			binary.BigEndian.Uint64(frames[0]) == math.MaxUint64 && len(frames[1]) == 0
	case 3:
		return len(frames[0]) == 0 && len(frames[1]) >= 8 &&
			binary.BigEndian.Uint64(frames[1]) == math.MaxUint64 && len(frames[2]) == 0
	default:
		return false
	}
}

// Start connects to a ZMQ PUB socket as a SUB, receives messages,
// wraps them in RawMessage structs, and pushes them into the pool.
// This loop will run until the provided context is canceled.
func (z *zmqSubscriber) Start(ctx context.Context) {
	logger := log.FromContext(ctx).WithName("zmq-subscriber")

	for {
		select {
		case <-ctx.Done():
			logger.Info("shutting down zmq-subscriber")
			return
		default:
			// We run the subscriber in a separate function to handle socket
			// setup/teardown and connection retries cleanly.
			z.runSubscriber(ctx)
			// wait before retrying, unless the context has been canceled.
			select {
			case <-time.After(retryInterval):
				metrics.SubscriberReconnections.WithLabelValues(z.podIdentifier).Inc()
				logger.Info("retrying zmq-subscriber")
			case <-ctx.Done():
				logger.Info("shutting down zmq-subscriber")
				return
			}
		}
	}
}

// runSubscriber connects to the ZMQ PUB socket, subscribes to the topic filter,
// and listens for messages.
func (z *zmqSubscriber) runSubscriber(ctx context.Context) {
	logger := log.FromContext(ctx).WithName("zmq-subscriber")

	// Disable zmq4's automatic reconnect to avoid a data race in the library:
	// when autoReconnect is true, scheduleRmConn calls Dial which writes
	// socket state without proper locking, racing with Close().
	// Reconnection is already handled by the outer retry loop in Start().
	sub := zmq4.NewSub(ctx)
	defer sub.Close()

	// Bind for local endpoints, connect for remote ones.
	if !z.remote {
		if err := sub.Listen(z.endpoint); err != nil {
			metrics.ZMQErrors.WithLabelValues(z.podIdentifier, "bind").Inc()
			logger.Error(err, "Failed to bind subscriber socket", "endpoint", z.endpoint)
			return
		}
		logger.Info("Bound subscriber socket", "endpoint", z.endpoint)
	} else {
		if err := sub.Dial(z.endpoint); err != nil {
			metrics.ZMQErrors.WithLabelValues(z.podIdentifier, "connect").Inc()
			logger.Error(err, "Failed to connect subscriber socket", "endpoint", z.endpoint)
			return
		}
		logger.Info("Connected subscriber socket", "endpoint", z.endpoint)
	}

	if err := sub.SetOption(zmq4.OptionSubscribe, z.topicFilter); err != nil {
		metrics.ZMQErrors.WithLabelValues(z.podIdentifier, "subscribe").Inc()
		logger.Error(err, "Failed to subscribe to topic filter", "topic", z.topicFilter)
		return
	}

	// Rebuild the index from buffered events without waiting for live traffic.
	if z.replayEndpoint != "" && !z.hasLastSeq && !z.liveOnly && z.canAttemptReplay() {
		logger.Info("Requesting proactive replay on connect",
			"endpoint", z.endpoint, "replayEndpoint", z.replayEndpoint)
		if !z.requestReplay(ctx, 0) {
			logger.Info("Proactive replay failed, falling back to indexing live events",
				"endpoint", z.endpoint, "replayEndpoint", z.replayEndpoint)
			z.liveOnly = true
		}
	}

	debugLogger := logger.V(logging.DEBUG)
	for {
		msg, err := sub.Recv()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			metrics.ZMQErrors.WithLabelValues(z.podIdentifier, "recv").Inc()
			debugLogger.Error(err, "Failed to receive message from zmq subscriber", "endpoint", z.endpoint)
			return
		}
		metrics.MessagesReceived.WithLabelValues(z.podIdentifier).Inc()
		topic, seq, payload, ok := parseEventFrame(msg.Frames)
		if !ok {
			debugLogger.Error(nil, "Malformed event frame",
				"frameCount", len(msg.Frames), "endpoint", z.endpoint)
			continue
		}

		if z.replayEndpoint == "" {
			z.addTask(topic, seq, payload)
			continue
		}

		replayAttempted := false
		if z.hasLastLiveSeq && seq < z.lastLiveSeq {
			logger.Info("Detected event sequence reset, rebuilding index",
				"lastLiveSeq", z.lastLiveSeq, "currentSeq", seq,
				"endpoint", z.endpoint)
			z.pool.resetForSource(topic, z.sourceEndpoint)
			z.lastSeq = 0
			z.hasLastSeq = false
			z.lastReplayFailure = time.Time{}
			z.joinLiveAfterReplay = false
			// A restarted engine starts a fresh, short buffer, so a full replay
			// can succeed again even if it failed for the previous lifetime.
			z.liveOnly = false
			replayAttempted = true
			if !z.requestReplay(ctx, 0) {
				z.liveOnly = true
			}
		}

		if z.hasLastLiveSeq && seq == z.lastLiveSeq {
			continue
		}
		z.lastLiveSeq = seq
		z.hasLastLiveSeq = true

		if z.hasLastSeq && seq <= z.lastSeq {
			continue
		}

		if z.hasLastSeq && seq > z.lastSeq+1 {
			missed := seq - z.lastSeq - 1
			if z.joinLiveAfterReplay {
				logger.Info("Joining live stream after approximate cold replay",
					"lastSeq", z.lastSeq, "currentSeq", seq, "missed", missed,
					"endpoint", z.endpoint)
				z.lastSeq = seq - 1
				z.joinLiveAfterReplay = false
			} else if !z.canAttemptReplay() {
				debugLogger.Info("Dropping event while replay is in cooldown",
					"lastSeq", z.lastSeq, "currentSeq", seq, "missed", missed,
					"endpoint", z.endpoint)
				continue
			} else {
				logger.Info("Detected gap in event sequence, requesting replay",
					"lastSeq", z.lastSeq, "currentSeq", seq, "missed", missed,
					"endpoint", z.endpoint)
				replayAttempted = true
				if !z.requestReplay(ctx, z.lastSeq+1) {
					continue
				}
			}
		}

		if !z.hasLastSeq && seq > 0 {
			switch {
			case z.liveOnly:
				// History replay is only a warmup optimisation, and some engines
				// page their replay buffer such that a cold replay can never be
				// contiguous. Indexing from here instead is strictly safe: the
				// pod was cleared when the replay failed, a removal for a block
				// we never recorded is a no-op, and every eviction from this
				// point on is observed. The index under-reports until traffic
				// refills it, but it never claims a block is resident when it
				// is not.
				debugLogger.Info("Indexing live events without replay",
					"currentSeq", seq, "endpoint", z.endpoint)
				z.lastSeq = seq - 1
				z.hasLastSeq = true
			case replayAttempted || !z.canAttemptReplay():
				continue
			default:
				logger.Info("Joining mid-stream, requesting full replay",
					"currentSeq", seq, "endpoint", z.endpoint)
				if !z.requestReplay(ctx, 0) {
					logger.Info("Full replay failed, falling back to indexing live events",
						"currentSeq", seq, "endpoint", z.endpoint)
					z.liveOnly = true
					continue
				}
			}
		}

		if z.hasLastSeq {
			if seq <= z.lastSeq || seq > z.lastSeq+1 {
				continue
			}
		}

		debugLogger.V(logging.TRACE).Info("Received message from zmq subscriber",
			"topic", topic, "seq", seq, "payloadSize", len(payload))
		z.addTask(topic, seq, payload)
		z.lastSeq = seq
		z.hasLastSeq = true
	}
}

func (z *zmqSubscriber) addTask(topic string, seq uint64, payload []byte) {
	z.pool.AddTask(&RawMessage{
		Topic:          topic,
		Sequence:       seq,
		Payload:        payload,
		SourceEndpoint: z.sourceEndpoint,
	})
}

// waitForReplayQueueCapacity prevents cold-start replay from filling the
// unbounded worker queues faster than the index can consume event batches.
// Live events remain non-blocking; replay is only a warm-up optimization and
// may time out and fall back to live indexing instead of risking an OOM.
func (z *zmqSubscriber) waitForReplayQueueCapacity(ctx context.Context) bool {
	for z.pool.queueDepth.Load() >= maxReplayQueueDepth {
		select {
		case <-ctx.Done():
			return false
		case <-time.After(replayQueuePollInterval):
		}
	}
	return true
}

func (z *zmqSubscriber) canAttemptReplay() bool {
	return z.lastReplayFailure.IsZero() || time.Since(z.lastReplayFailure) >= replayCooldown
}

func (z *zmqSubscriber) invalidateReplay(topic string) {
	z.pool.resetForSource(topic, z.sourceEndpoint)
	z.lastSeq = 0
	z.hasLastSeq = false
	z.joinLiveAfterReplay = false
	z.lastReplayFailure = time.Now()
}

// requestReplay requests buffered events starting from startSeq.
func (z *zmqSubscriber) requestReplay(ctx context.Context, startSeq uint64) bool {
	logger := log.FromContext(ctx).WithName("zmq-replay")
	debugLogger := logger.V(logging.DEBUG)

	waitStarted := time.Now()
	if err := processReplayLimiter.Acquire(ctx, 1); err != nil {
		metrics.ZMQErrors.WithLabelValues(z.podIdentifier, "replay-capacity").Inc()
		logger.Info("Replay canceled while waiting for process capacity",
			"waitDuration", time.Since(waitStarted),
			"replayEndpoint", z.replayEndpoint)
		return false
	}
	defer processReplayLimiter.Release(1)
	if waitDuration := time.Since(waitStarted); waitDuration >= time.Second {
		logger.Info("Replay admitted after waiting for process capacity",
			"waitDuration", waitDuration, "replayEndpoint", z.replayEndpoint)
	}

	replayCtx, cancel := context.WithTimeout(ctx, replayTimeout)
	defer cancel()

	replayed := 0
	reanchored := false
	nextSeq := startSeq
	attempt := 0
	noProgressAttempts := 0
	for {
		if replayCtx.Err() != nil {
			z.invalidateReplay(z.topicFilter)
			logger.Info("Replay timed out",
				"replayed", replayed, "attempts", attempt,
				"replayEndpoint", z.replayEndpoint)
			return false
		}
		attempt++

		attemptCtx, attemptCancel := context.WithCancel(replayCtx)
		dealer := zmq4.NewDealer(attemptCtx, zmq4.WithTimeout(replayAttemptIdleTimeout))
		if err := dealer.Dial(z.replayEndpoint); err != nil {
			attemptCancel()
			z.invalidateReplay(z.topicFilter)
			metrics.ZMQErrors.WithLabelValues(z.podIdentifier, "replay-connect").Inc()
			logger.Error(err, "Failed to connect replay socket",
				"replayEndpoint", z.replayEndpoint)
			return false
		}
		if replayCtx.Err() != nil {
			dealer.Close()
			attemptCancel()
			continue
		}
		seqBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(seqBytes, nextSeq)
		if err := dealer.SendMulti(zmq4.NewMsgFrom([]byte{}, seqBytes)); err != nil {
			dealer.Close()
			attemptCancel()
			z.invalidateReplay(z.topicFilter)
			metrics.ZMQErrors.WithLabelValues(z.podIdentifier, "replay-send").Inc()
			logger.Error(err, "Failed to send replay request",
				"nextSeq", nextSeq, "replayEndpoint", z.replayEndpoint)
			return false
		}

		idleTimer := time.AfterFunc(replayAttemptIdleTimeout, attemptCancel)
		attemptReplayed := 0
		complete := false
		expectedSeq := nextSeq
		var receiveErr error
		var terminalErr error
		for {
			msg, err := dealer.Recv()
			if err != nil {
				receiveErr = err
				break
			}
			idleTimer.Reset(replayAttemptIdleTimeout)

			frames := msg.Frames
			if len(frames) > 0 && len(frames[0]) == 0 {
				frames = frames[1:]
			}
			if isReplayTerminalFrame(frames) {
				complete = true
				break
			}

			topic, seq, payload, ok := parseReplayEventFrame(frames, z.topicFilter)
			if !ok {
				terminalErr = fmt.Errorf("malformed replay frame with %d frames", len(frames))
				break
			}
			if seq != expectedSeq {
				// A cold join (startSeq == 0) asks for the whole history, but an
				// engine may answer from its oldest retained sequence and may
				// page a replay with forward jumps between responses. Demanding
				// exact contiguity leaves the index permanently empty.
				//
				// A forward jump can hide an eviction of a block already
				// replayed, so retaining the prefix would be unsafe. Queue a
				// reset before the new suffix and re-anchor there instead. The
				// ordered per-source queue guarantees that only the final
				// contiguous suffix survives. Gap replays (startSeq > 0) remain
				// strict because they repair an already-serving index.
				if startSeq == 0 && seq > expectedSeq {
					if replayed > 0 {
						z.pool.resetForSource(topic, z.sourceEndpoint)
					}
					reanchored = true
					logger.Info("Re-anchoring cold-start replay at next available sequence",
						"requestedSeq", startSeq, "expectedSeq", expectedSeq,
						"anchorSeq", seq, "replayed", replayed,
						"replayEndpoint", z.replayEndpoint)
					expectedSeq = seq
				} else {
					terminalErr = fmt.Errorf("incomplete replay: expected sequence %d, got %d", expectedSeq, seq)
					break
				}
			}

			z.addTask(topic, seq, payload)
			z.lastSeq = seq
			z.hasLastSeq = true
			replayed++
			attemptReplayed++
			expectedSeq++
			if !z.waitForReplayQueueCapacity(replayCtx) {
				receiveErr = replayCtx.Err()
				break
			}
		}

		idleTimer.Stop()
		dealer.Close()
		attemptCancel()
		if terminalErr != nil {
			z.invalidateReplay(z.topicFilter)
			metrics.ZMQErrors.WithLabelValues(z.podIdentifier, "replay-incomplete").Inc()
			logger.Error(terminalErr, "Replay response is incomplete",
				"attempt", attempt, "replayed", replayed,
				"nextSeq", nextSeq, "replayEndpoint", z.replayEndpoint)
			return false
		}
		if complete {
			if replayed == 0 && startSeq > 0 {
				err := fmt.Errorf("incomplete replay: sequence %d was not available", startSeq)
				z.invalidateReplay(z.topicFilter)
				metrics.ZMQErrors.WithLabelValues(z.podIdentifier, "replay-incomplete").Inc()
				logger.Error(err, "Replay response is incomplete",
					"attempt", attempt, "replayed", replayed,
					"nextSeq", nextSeq, "replayEndpoint", z.replayEndpoint)
				return false
			}
			z.lastReplayFailure = time.Time{}
			if startSeq == 0 && reanchored {
				z.joinLiveAfterReplay = true
			}
			logger.Info("Replay complete", "replayed", replayed,
				"attempts", attempt, "startSeq", startSeq,
				"replayEndpoint", z.replayEndpoint)
			return true
		}
		if replayCtx.Err() != nil {
			continue
		}

		if attemptReplayed == 0 {
			noProgressAttempts++
			if noProgressAttempts >= maxReplayNoProgressAttempts {
				if receiveErr == nil {
					receiveErr = fmt.Errorf("replay response ended without progress")
				}
				z.invalidateReplay(z.topicFilter)
				metrics.ZMQErrors.WithLabelValues(z.podIdentifier, "replay-no-progress").Inc()
				logger.Error(receiveErr, "Replay stopped after no progress",
					"attempts", attempt, "replayed", replayed,
					"nextSeq", nextSeq, "replayEndpoint", z.replayEndpoint)
				return false
			}
		} else {
			noProgressAttempts = 0
			nextSeq = expectedSeq
		}
		debugLogger.Info("Replay response interrupted, resuming",
			"attempt", attempt, "attemptReplayed", attemptReplayed,
			"replayed", replayed, "nextSeq", nextSeq, "error", receiveErr,
			"replayEndpoint", z.replayEndpoint)

		select {
		case <-time.After(replayRetryBackoff):
		case <-replayCtx.Done():
		}
	}
}
