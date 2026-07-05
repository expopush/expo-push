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

## 4. Observability — DONE

Shipped: optional Micrometer instrumentation (submissions counter, outcomes-by-type
counter across all backends, per-attempt Expo call timer, local queue depth and H2
pending-rows gauges). Deferred (add on demand): explicit poison-message counter,
submit-to-terminal latency (needs an enqueue timestamp carried in queue messages),
Resilience4j retry-metrics binding.

## 5. Cleanup batch — DONE

Shipped: batched receipt fetches in the local/H2 orchestrators (up to 300 tickets per
Expo call), HikariCP pool for the H2 backend, non-UTC timezone test, drain-with-UNKNOWN
on local shutdown, helper dedup (ResultDispatcher / ExpoErrors / LogMasker.sanitize —
five copies each collapsed), single retry-bean builder, Retry-After honored on 429,
ObjectMapper fallback in SQS/H2 auto-configs, programmatic property range validation,
threading/log/javadoc nits.

Decisions of record:
- Property validation is programmatic, NOT JSR-303: Boot attempts bean validation
  whenever the jakarta.validation API is present and fails hard
  (NoProviderFoundException) when no implementation is — a starter must not plant that
  classpath landmine in consumer apps.
- `@Import(FeignClientsConfiguration.class)` stays: the Feign client needs its
  encoder/decoder/contract beans, and apps using spring-cloud-openfeign already have
  them. Revisit only if a bean-collision report ever materializes.
- Harness repo README badges tracked in the harness repo.

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
