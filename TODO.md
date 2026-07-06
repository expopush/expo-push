# Roadmap: Remaining Work

Complete accounting of open items from the 2026-07 code review, in agreed priority order.
Items completed in PRs #8 (review fixes) and #9 (SQS write-side batching) are not listed.

Explicitly **rejected** (do not revisit without new evidence):

- Expo 100-message batching via cross-SQS-receive accumulation — undermines SQS
  visibility/receive-count/redelivery semantics for negligible real throughput gain
  (Expo project cap ~600 notifications/s vs. 45 calls/s × 10 = 450/s per node already).
- Producer-side batch API (`enqueueAll` packing many commands into one SQS message) —
  no real use case; clients needing that level of control should integrate with Expo directly.

## 1. Resilience — DONE (PR #10)

Shipped: 401 critical-backoff (consumer resumes instead of halting permanently),
poison messages left for DLQ redrive with a receive-count ceiling instead of
delete-on-sight, `schemaVersion` discriminator in queue messages (unknown future
versions parked for DLQ, not deleted), handler validated at `enqueue()` and again at
parse time (unroutable messages stay in the queue, receive-count bounded).

## 2. Security / logging polish — DONE (PR #11)

Shipped: push tokens masked everywhere they appear in logs and `toString()` output.
`LogMasker` stays a JVM-global static (it must be reachable from record `toString()`
methods with no access to Spring beans); the multi-context/test-pollution hazard is
documented on the class, and masking defaults to ON so the failure mode is
over-masking, never a leak.

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

## 7. Release / governance — DONE (2026-07-06)

- `expopush/.github` community-health repo live (CODE_OF_CONDUCT, CONTRIBUTING,
  SECURITY inherited by all org repos).
- expopush.dev serves a landing page via GitHub Pages (expopush.github.io repo,
  custom domain with enforced HTTPS).
- 1.0.0-RC2 published to Maven Central (tag `v1.0.0-RC2`, GitHub release with notes);
  harness poms re-pinned to the release and build-from-source CI steps removed
  (test-harnesses #11).
