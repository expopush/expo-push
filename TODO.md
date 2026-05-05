# TODO: Future Improvements & Resilience Enhancements

This file tracks identified issues and architectural improvements to be addressed in future iterations.

## Resilience & Reliability

### [SQS] Self-Healing Authentication
- **Issue:** Consumers currently call `haltConsumer()` upon encountering an `ExpoAuthException` (401), permanently stopping the thread.
- **Goal:** Implement a "Critical Backoff" state. Instead of stopping, the consumer should log a critical error and sleep for a significant duration (e.g., 5 minutes) before attempting to resume.
- **Status:** Identified in adversarial review.

### [SQS] Safer Poison Pill Handling
- **Issue:** Messages that fail to deserialize are currently deleted immediately to prevent infinite loops. This risks mass data loss if a deployment introduces a breaking JSON model change.
- **Goal:** Stop immediate deletion of unparseable messages. Allow them to return to the queue and rely on the SQS native Dead Letter Queue (DLQ) redrive policy (maxReceiveCount) for safe removal.
- **Status:** Identified in adversarial review.

### [Core] Distributed Rate Limiting
- **Issue:** `LocalExpoRateLimiter` is node-local. In multi-node clusters, the aggregate traffic can easily exceed Expo's global limits.
- **Goal:** Provide a Redis-backed `DistributedExpoRateLimiter` implementation to synchronize permits across the cluster.
- **Status:** Identified in adversarial review.

## Operational & Scale

### [Simulator] Hardening
- **Issue:** The Python simulator lacks security headers and schema validation for `config.yaml`.
- **Goal:** Add basic API key support to the simulator and use Pydantic for robust configuration validation.
