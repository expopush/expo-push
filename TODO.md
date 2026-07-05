# Roadmap: Remaining Work

Complete accounting of open items from the 2026-07 code review, in agreed priority order.
Items completed in PRs #8 (review fixes) and #9 (SQS write-side batching) are not listed.

Explicitly **rejected** (do not revisit without new evidence):

- Expo 100-message batching via cross-SQS-receive accumulation — undermines SQS
  visibility/receive-count/redelivery semantics for negligible real throughput gain
  (Expo project cap ~600 notifications/s vs. 45 calls/s × 10 = 450/s per node already).
- Producer-side batch API (`enqueueAll` packing many commands into one SQS message) —
  no real use case; clients needing that level of control should integrate with Expo directly.

## 1. Resilience (next up — one PR)

### 1.1 [SQS] Self-healing 401 handling
`ExpoAuthException` currently calls `haltConsumer()`, permanently stopping the poll thread
(`isRunning()` goes false; only a JVM restart recovers). Replace with a critical-backoff
state: log CRITICAL, sleep a configurable interval (default ~5 min), resume polling.

### 1.2 [SQS] Poison messages: stop immediate deletion, lean on DLQ redrive
Messages that fail JSON deserialization are deleted on first sight — a breaking model
change in a deploy would destroy the entire in-flight queue. Instead: leave them in the
queue (visibility timeout → redelivery → DLQ redrive if configured); hard-delete with a
loud error only once the receive count exceeds the retry ceiling, so queues without a DLQ
cannot loop forever. Document the recommended DLQ `maxReceiveCount` relationship.

### 1.3 [SQS] Schema-version field in queue messages
`PushNotificationSqsMessage` / `PushReceiptSqsMessage` have no version discriminator.
Add `schemaVersion` (current = 1; absent/0 = legacy, accepted). Unknown future versions
are treated like poison (left for DLQ), not deleted.

### 1.4 Missing-handler protection
- Validate `handlerId` against the registry at `enqueue()` — fail fast with
  `NotificationSubmissionException` instead of discovering the mismatch at consume time.
- SQS consumers: check the handler exists at parse time, BEFORE calling Expo / resolving
  the message; unroutable messages stay in the queue (receive-count bounded) so a
  redeploy with the handler restored recovers them instead of silently losing outcomes.

## 2. Security / logging polish (quick PR)

- Mask push tokens in logs (`LogMasker.mask`) everywhere they currently appear raw —
  `NotificationCommand.toString`, `NotificationResult.toString`, SQS consumer log lines.
- Replace the JVM-global mutable `LogMasker` static with an instance-based masker (or at
  minimum document the multi-context/test-pollution hazard).

## 3. Notification model completeness — DONE

Shipped: `NotificationOptions` (data/channelId/sound/ttl/badge/subtitle/priority) on
`NotificationCommand`, shared `ExpoMessages` mapper in core, SQS schema version 2 with
`data`/`subtitle` encrypted at rest. Not yet exposed (add on demand): `categoryId`,
`mutableContent`, `displayInForeground`, `expiration`, critical-alert sound options.

## 4. Observability

Micrometer metrics (optional dependency, auto-configured when present): counters for
submissions/outcomes-by-type/retries/poison messages, timers for Expo call latency and
submit-to-terminal latency, gauges for local queue depth.

## 5. Cleanup batch (one PR)

- Local/H2 receipt orchestrators fetch receipts ONE ticket per HTTP call; Expo accepts
  300 IDs per call — batch the due tasks.
- H2 backend uses `DriverManagerDataSource` (new connection per JDBC op, including the
  1 Hz idle poll) — use a small pool (HikariCP).
- Explicit test for H2 `Instant` vs `CURRENT_TIMESTAMP` comparison under a non-UTC JVM
  timezone.
- Local backend silently drops queued receipt checks on shutdown — document; ideally
  drain-with-UNKNOWN on graceful stop.
- Deduplicate the helpers copied across all five backend classes (`notifyHandler`,
  `result(...)`, `extractError`, `sanitize`, DeviceNotRegistered→REJECTED mapping) —
  this drift already caused one real bug (ACCEPTED with non-null errorDetail). Also the
  two identical retry `@Bean` methods.
- Threading nits: `stop()`/`haltConsumer()` not synchronized with `start()`;
  "virtual threads" log lines imply a pool but each orchestrator runs one worker;
  `DelayedReceiptTask.compareTo` unchecked cast.
- JSR-303 validation on `ExpoPushProperties` (`@Validated`, ranges) — negative delays and
  zero attempt counts are currently accepted silently.
- Honor `Retry-After` on Expo 429 responses instead of blind exponential backoff.
- `@Import(FeignClientsConfiguration.class)` is unconditional; consider narrowing.
- SQS/H2 auto-configs assume an `ObjectMapper` bean exists — provide a fallback.
- `ExpoRateLimiter` javadoc promises a Redis-backed implementation that doesn't exist —
  reword until 6.1 lands.
- Harness repo README badges say Java 17 / Spring Boot 3.x; poms are Java 21 / Boot 4.

## 6. Post-1.0 candidates

### 6.1 [Core] Distributed rate limiting
`LocalExpoRateLimiter` is node-local; multi-node deployments can exceed Expo's project
cap. Provide a Redis-backed `DistributedExpoRateLimiter`.

### 6.2 [Simulator] Hardening
API-key support and Pydantic config validation for the Python test target.

## 7. Release / governance (tabled, tracked outside code)

- `expopush/.github` community-health repo (CODE_OF_CONDUCT, CONTRIBUTING, SECURITY
  inherited by all org repos — test-harnesses and test-target currently lack them).
- Docs site for expopush.dev (READMEs link to it; nothing is served). Either stand up
  GitHub Pages/Docusaurus or soften the README links.
- When 1.0.0-RC2 is published to Maven Central: re-pin the harness poms to the release
  and remove the "build expo-push from source" steps from both harness workflows.
