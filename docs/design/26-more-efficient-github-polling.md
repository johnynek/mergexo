---
issue: 26
priority: 3
touch_paths:
  - src/mergexo/config.py
  - src/mergexo/cli.py
  - src/mergexo/orchestrator.py
  - src/mergexo/state.py
  - src/mergexo/github_gateway.py
  - src/mergexo/observability.py
  - src/mergexo/github_event_engine.py
  - src/mergexo/github_event_engine_polling.py
  - src/mergexo/github_event_engine_streaming.py
  - src/mergexo/webhook_server.py
  - mergexo.toml.example
  - README.md
  - tests/test_config.py
  - tests/test_cli.py
  - tests/test_orchestrator.py
  - tests/test_state.py
  - tests/test_github_gateway.py
  - tests/test_github_event_engine_polling.py
  - tests/test_github_event_engine_streaming.py
depends_on: []
estimated_size: M
generated_at: 2026-02-22T17:56:18Z
---

# More Efficient GitHub Event Intake (Polling + Streaming Engines)

_Issue: #26 (https://github.com/johnynek/mergexo/issues/26)_

## Summary

Introduce a configurable GitHub event-engine abstraction with polling and webhook-based streaming modes, keep polling as the default for local-first reliability, and add a reconciliation safety net so streaming can be efficient without sacrificing correctness.

## Context

Today `Phase1Orchestrator.run` is timer-driven. Every poll cycle it does a full intake and feedback scan, then sleeps for `runtime.poll_interval_seconds`.

That model is simple and robust, but it is expensive when the repository is mostly idle:
1. Issue intake does label-based reads every cycle.
2. Feedback scan can repeatedly fetch PR snapshot/comments/files even when nothing changed.
3. Latency is bounded by the poll interval.

Issue #26 asks for a more efficient approach by adding a streaming/WebSocket path while keeping polling available.

## Problem Statement

We need an event-ingestion architecture that:
1. Reduces unnecessary GitHub API traffic.
2. Preserves reliability and crash recovery.
3. Stays GitHub-friendly.
4. Allows operators to switch between polling and streaming when one mode has operational issues.

## Goals

1. Add an explicit GitHub event-engine abstraction with at least `polling` and `streaming` implementations.
2. Keep existing behavior unchanged when `polling` is selected.
3. Implement a streaming path that wakes work quickly on GitHub events.
4. Keep correctness safeguards from current orchestration/state logic.
5. Provide clear operator configuration and rollback/fallback paths.
6. Make a recommendation for default engine and long-term engine strategy.

## Non-goals

1. Replacing the existing issue/PR processing semantics.
2. Rewriting the feedback-loop state machine.
3. Building a distributed multi-process event bus.
4. Guaranteeing exactly-once delivery from GitHub (not possible); we target idempotent at-least-once processing.

## GitHub Capability Constraints

1. GitHub does not provide a repository event WebSocket feed; practical "streaming" integration is webhook push.
2. GitHub REST activity events are explicitly optimized for polling, not real-time delivery.
3. Webhooks require a reachable callback URL; localhost-only listeners need a relay/reverse proxy/tunnel.
4. Webhook deliveries can fail, arrive out of order, and are not automatically redelivered by GitHub.

Because of (4), streaming mode must include periodic reconciliation polling.

## Option Analysis

### Option A: Polling-only

Pros:
1. Works in local-first environments without inbound network setup.
2. Operationally simple and already proven in this repo.
3. Naturally self-healing after downtime (next poll catches changes).

Cons:
1. Constant API load even during idle periods.
2. Higher median time-to-react.
3. Less GitHub-friendly at scale (repeated unchanged reads).

### Option B: Streaming-only (webhooks only)

Pros:
1. Best efficiency in steady state (no changes -> near-zero API reads).
2. Lowest reaction latency.
3. Most GitHub-friendly in terms of unnecessary polling.

Cons:
1. Requires inbound webhook delivery infrastructure.
2. Can miss events when listener is down unless reconciliation exists.
3. Higher operational complexity for local/home-machine deployments.

### Option C: Dual engine + shared reconciliation (recommended)

Pros:
1. Keeps simple fallback (`polling`) for local-first or incident recovery.
2. Enables efficient low-latency mode (`streaming`) for environments with webhook ingress.
3. Allows gradual rollout and empirical decision-making.

Cons:
1. More code surface area than single engine.
2. Requires careful interface boundaries to avoid duplicated logic.

## Proposed Architecture

### 1. Event Engine Interface

Add a new engine contract consumed by the orchestrator:

1. `GitHubEventEngine.start()`
2. `GitHubEventEngine.next_wake(timeout_seconds)`
3. `GitHubEventEngine.stop()`

`next_wake` returns a normalized wake payload:
1. `reason` (`issue_hint`, `pr_hint`, `full_reconcile`, `shutdown`)
2. Optional `issue_numbers`
3. Optional `pr_numbers`

The orchestrator remains the only place that mutates runtime state and performs GitHub writes.

### 2. Polling Engine

Refactor existing timer behavior into `PollingEventEngine`:
1. Emits `full_reconcile` every `poll_interval_seconds`.
2. Preserves current scan order and semantics.
3. Serves as the known-good fallback path.

### 3. Streaming Engine (Webhook Push)

Implement `StreamingEventEngine` with an embedded HTTP receiver thread.

Request handling:
1. Verify `X-Hub-Signature-256` against configured secret.
2. Read `X-GitHub-Event` and `X-GitHub-Delivery`.
3. Parse event payload and map it to issue/PR hints.
4. Persist delivery id + hint rows for dedupe/restart safety.
5. Return quickly (`202`) and process asynchronously.

Event mapping (initial):
1. `issues` -> issue hints.
2. `pull_request` -> PR hints (+ implementation promotion wake on merge/close actions).
3. `issue_comment` on PR -> PR hints.
4. `pull_request_review_comment` -> PR hints.
5. `pull_request_review` -> PR hints.

### 4. Mandatory Reconciliation in Streaming Mode

Even in streaming mode, run a low-frequency reconciliation tick (`github_reconcile_interval_seconds`, default 10 minutes):
1. Catch missed/failed webhook deliveries.
2. Recover from listener downtime.
3. Preserve eventual consistency guarantees.

### 5. Orchestrator Integration

Update `Phase1Orchestrator.run` to be wake-driven instead of unconditional sleep loops.

On `full_reconcile`:
1. Reap finished work.
2. Run issue intake scan.
3. Run implementation-candidate scan.
4. Run feedback scan (if enabled).

On `issue_hint`:
1. Reap finished work.
2. Run issue intake only.

On `pr_hint`:
1. Reap finished work.
2. Run feedback scan targeted to hinted PRs.
3. Run implementation-candidate scan when PR state-change hints indicate merge/close.

This keeps processing logic centralized while reducing unnecessary scans.

### 6. State Additions

Add durable delivery/hint storage in SQLite (names illustrative):
1. `github_webhook_deliveries(delivery_id PRIMARY KEY, event_type, received_at, accepted, error)`
2. `github_event_hints(id PK, delivery_id, issue_number, pr_number, hint_kind, created_at, processed_at)`

New `StateStore` APIs:
1. `record_webhook_delivery(...) -> bool` (false on duplicate).
2. `enqueue_event_hints(...)`.
3. `list_pending_event_hints(limit)`.
4. `mark_event_hints_processed(ids)`.

### 7. Config Surface

Extend TOML config with explicit engine selection:

1. `[runtime] github_event_engine = "polling" | "streaming" | "auto"`.
2. `[runtime] github_reconcile_interval_seconds` (streaming safety net).
3. `[github_streaming] listen_host`, `listen_port`, `webhook_path`, `secret_env`.

`auto` behavior:
1. Try streaming startup.
2. If listener cannot bind or config is invalid, log and fall back to polling.

### 8. Observability

Add engine-level events:
1. `github_event_engine_started`
2. `github_event_engine_fallback`
3. `webhook_received`
4. `webhook_rejected_signature`
5. `webhook_duplicate_delivery`
6. `event_hints_enqueued`
7. `event_hints_drained`
8. `reconcile_tick`

Key rollout metrics:
1. GitHub API reads/hour.
2. Median event-to-agent-turn latency.
3. Duplicate delivery rate.
4. Missed-event recoveries found by reconcile ticks.

## Efficiency, Reliability, and GitHub-Friendliness Tradeoffs

Efficiency:
1. Polling cost scales with time (`O(scans per interval)`), even at zero activity.
2. Streaming cost scales with actual event volume, plus periodic reconcile.

Reliability:
1. Polling is resilient to temporary process/network downtime.
2. Streaming needs durable dedupe + reconcile polling to match reliability.

GitHub-friendliness:
1. Streaming significantly reduces unnecessary REST polling load.
2. Polling remains acceptable with moderate intervals, but is noisier at scale.

## Recommendation

1. Implement both engines behind one abstraction now.
2. Keep `polling` as default in this issue because MergeXO is explicitly local-first and polling is the lowest operational burden.
3. Ship `streaming` as opt-in with required reconciliation polling.
4. Add `auto` mode as the practical production default candidate after canary success.

Long-term single-engine direction:
1. Do not remove polling immediately.
2. If streaming + reconcile proves stable across real deployments, deprecate high-frequency standalone polling and converge toward one practical default (`auto`/streaming-first with safe fallback).

## Implementation Plan

1. Introduce engine interfaces and polling-engine extraction.
2. Add config parsing and validation for engine selection and streaming settings.
3. Add durable webhook delivery/hint tables and state APIs.
4. Implement webhook receiver + streaming engine queueing.
5. Integrate wake-driven orchestration paths and targeted scans.
6. Add fallback behavior for `auto` mode.
7. Add logs/metrics and update docs/sample config.
8. Add unit + integration tests for polling parity, streaming dedupe, signature validation, fallback, and reconcile recovery.

## Acceptance Criteria

1. `github_event_engine = "polling"` preserves current behavior and existing polling tests continue to pass with equivalent semantics.
2. `github_event_engine = "streaming"` starts an HTTP webhook receiver and processes valid signed webhook deliveries.
3. Duplicate webhook deliveries (same `X-GitHub-Delivery`) do not trigger duplicate enqueue/processing.
4. Invalid/missing webhook signatures are rejected and do not enqueue work.
5. In streaming mode, PR/issue events wake orchestration without waiting for `poll_interval_seconds`.
6. Streaming mode runs reconciliation scans at configured cadence and catches missed changes after simulated webhook downtime.
7. `github_event_engine = "auto"` falls back to polling when streaming startup fails, with a clear log event.
8. Feedback-loop idempotency guarantees remain intact (no duplicate bot replies caused by engine changes).
9. README and `mergexo.toml.example` document setup, operational caveats, and fallback behavior.
10. New tests cover event-engine selection, webhook ingestion/dedupe, reconcile safety net, and targeted orchestrator wakes.

## Risks and Mitigations

1. Risk: Streaming setup is hard for local machines.
Mitigation: keep polling default; document reverse-proxy/tunnel requirements; provide `auto` fallback.

2. Risk: Missed webhook deliveries create stale state.
Mitigation: mandatory reconciliation polling and durable hint storage.

3. Risk: Event storms can overload workers.
Mitigation: bounded hint queue, coalescing by PR/issue number, and existing worker-capacity guardrails.

4. Risk: Security exposure of webhook endpoint.
Mitigation: signature verification, path scoping, localhost bind by default, and guidance for TLS termination.

5. Risk: Maintaining two engines increases complexity.
Mitigation: strict shared interface and shared downstream orchestration logic.

## Rollout Notes

1. Phase 1: land abstraction + polling engine extraction with no behavior change.
2. Phase 2: ship streaming engine behind explicit config flag.
3. Phase 3: run canary on one repo with verbose logging and compare API-call/latency metrics to polling baseline.
4. Phase 4: enable `auto` in controlled environments where webhook ingress is stable.
5. Reevaluate default after canary data; only consider deprecating standalone polling after sustained reliability.

## References

1. GitHub REST activity events are optimized for polling, not real-time:
https://docs.github.com/en/rest/activity/events?apiVersion=2022-11-28
2. Webhook best practices (ordering, quick responses, security):
https://docs.github.com/en/webhooks/using-webhooks/best-practices-for-using-webhooks
3. Failed webhook deliveries are not automatically redelivered:
https://docs.github.com/en/webhooks/using-webhooks/handling-failed-webhook-deliveries
4. Delivering webhooks to private/local systems requires a relay/reverse proxy:
https://docs.github.com/en/webhooks/using-webhooks/delivering-webhooks-to-private-systems
