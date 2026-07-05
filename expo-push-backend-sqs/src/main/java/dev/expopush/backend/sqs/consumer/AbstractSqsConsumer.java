package dev.expopush.backend.sqs.consumer;

import dev.expopush.api.LogMasker;
import dev.expopush.api.NotificationHandlerRegistry;
import dev.expopush.api.NotificationOutcome;
import dev.expopush.api.NotificationResult;
import dev.expopush.backend.api.ResultDispatcher;
import dev.expopush.backend.sqs.message.SqsNotificationMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.SmartLifecycle;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;

/**
 * Base class for SQS long-poll consumer threads.
 *
 * <p>Integrates with Spring's {@link SmartLifecycle} for graceful startup and shutdown.
 * Shutdown is asynchronous: the consumer thread is interrupted and the stop callback
 * fires once the thread exits (or the drain timeout elapses), allowing multiple consumers
 * to drain in parallel and playing well with
 * {@code spring.lifecycle.timeout-per-shutdown-phase}.
 */
@Slf4j
abstract class AbstractSqsConsumer implements SmartLifecycle {

    protected final SqsClient sqsClient;
    protected final NotificationHandlerRegistry registry;
    private final ResultDispatcher dispatcher;

    private final String threadName;
    private final long drainTimeoutMs;
    private final long criticalBackoffMs;
    /** Controls whether the poll loop keeps iterating. */
    private volatile boolean loopActive = false;
    /** SmartLifecycle state — returned by {@link #isRunning()}. */
    private volatile boolean running = false;
    private final java.util.concurrent.atomic.AtomicReference<Thread> consumerThread = new java.util.concurrent.atomic.AtomicReference<>();

    protected AbstractSqsConsumer(
        SqsClient sqsClient,
        NotificationHandlerRegistry registry,
        String threadName,
        long drainTimeoutMs,
        long criticalBackoffMs
    ) {
        this.sqsClient = sqsClient;
        this.registry = registry;
        this.dispatcher = new ResultDispatcher(registry);
        this.threadName = threadName;
        this.drainTimeoutMs = drainTimeoutMs;
        this.criticalBackoffMs = criticalBackoffMs;
    }

    // ─── SmartLifecycle ───────────────────────────────────────────────────────

    @Override
    public synchronized void start() {
        if (running) return;
        try {
            onStart();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to initialise " + threadName + " consumer", e);
        }
        loopActive = true;
        running = true;
        Thread t = new Thread(this::pollLoop, threadName);
        consumerThread.set(t);
        t.start();
        log.info("{} consumer started", threadName);
    }

    /**
     * Called once inside the synchronized {@link #start()} before the poll thread launches.
     * Subclasses should resolve queue URLs here rather than in their constructor so that a
     * temporarily unavailable SQS endpoint does not prevent the Spring context from starting.
     */
    protected void onStart() {}

    /**
     * Signals the consumer to stop and schedules a drain waiter thread that calls
     * {@code callback} once the consumer thread has exited. Spring will wait for the
     * callback up to {@code spring.lifecycle.timeout-per-shutdown-phase} (default 30 s).
     */
    @Override
    public synchronized void stop(Runnable callback) {
        loopActive = false;
        Thread t = consumerThread.get();
        if (t != null) {
            t.interrupt();
        }
        Thread drainWaiter = new Thread(() -> {
            try {
                if (t != null) {
                    t.join(drainTimeoutMs);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                running = false;
                log.info("{} consumer stopped", threadName);
                callback.run();
            }
        }, threadName + "-drain");
        drainWaiter.setDaemon(true);
        drainWaiter.start();
    }

    @Override
    public void stop() {
        stop(() -> {});
    }

    /**
     * Pauses the poll loop for the configured critical-backoff interval and then resumes.
     * Used for failures that no amount of per-message retrying can fix (e.g. Expo rejecting
     * our credentials with 401): messages stay safely in SQS, the consumer keeps its
     * lifecycle state, and polling resumes automatically once the interval elapses — a
     * fixed credential does not require a JVM restart to take effect.
     */
    protected void criticalBackoff(String reason) throws InterruptedException {
        log.error("CRITICAL: {} — {} pausing for {} ms before resuming polling. "
            + "Messages remain in SQS.", reason, threadName, criticalBackoffMs);
        Thread.sleep(criticalBackoffMs);
    }

    /**
     * Resolves a message that cannot be processed at all (unparseable JSON, unknown schema
     * version, unregistered handler). Below {@code receiveCeiling} the message is left in
     * the queue: it becomes visible again after the visibility timeout and, if the queue has
     * a DLQ redrive policy, lands in the DLQ once {@code maxReceiveCount} is reached —
     * recoverable either way. At or past the ceiling it is deleted with a loud error so a
     * queue WITHOUT a DLQ cannot redeliver it forever.
     *
     * <p>Operators should configure the DLQ redrive {@code maxReceiveCount} BELOW the
     * ceiling passed here, so unprocessable messages reach the DLQ before being deleted.
     */
    protected void resolveUnprocessable(String queueUrl, Message message, int receiveCeiling, String reason) {
        int receiveCount = parseReceiveCount(message);
        if (receiveCount >= receiveCeiling) {
            log.error("Unprocessable message exceeded {} receive(s) — DELETING (data loss; "
                + "configure a DLQ redrive policy with maxReceiveCount below this ceiling "
                + "to capture these instead): {}", receiveCeiling, reason);
            deleteMessage(queueUrl, message.receiptHandle());
        } else {
            log.warn("Unprocessable message left in queue for redelivery/DLQ "
                + "(receive {}/{}): {}", receiveCount, receiveCeiling, reason);
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    // ─── Poll loop ────────────────────────────────────────────────────────────

    private void pollLoop() {
        while (loopActive && !Thread.currentThread().isInterrupted()) {
            try {
                processOneBatch();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                log.error("Unexpected error in {} consumer — backing off 2 s", threadName, e);
                backOffQuietly(2_000);
            }
        }
        log.info("{} consumer poll loop exited", threadName);
    }

    private static void backOffQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Receives and processes one SQS batch. Must propagate {@link InterruptedException}
     * rather than swallowing it so the poll loop can exit cleanly on shutdown.
     */
    protected abstract void processOneBatch() throws InterruptedException;

    // ─── Shared helpers ───────────────────────────────────────────────────────

    /**
     * Routes a terminal {@link NotificationResult} to the registered handler.
     * Handler exceptions are caught and logged — a bad handler must not disrupt the consumer.
     */
    protected void notifyHandler(NotificationResult result) {
        dispatcher.dispatch(result);
    }

    /**
     * Builds a {@link NotificationResult} from an SQS notification message and an outcome.
     * {@code ticketId} and {@code errorDetail} are outcome-dependent and may be null.
     */
    protected NotificationResult result(
        NotificationOutcome outcome,
        SqsNotificationMessage msg,
        String ticketId,
        String errorDetail
    ) {
        return new NotificationResult(
            outcome, msg.handlerId(), msg.correlationId(),
            msg.pushToken(), msg.title(), msg.body(),
            ticketId, errorDetail, msg.metadata()
        );
    }

    /** See {@link LogMasker#sanitize} — kept as a local alias for subclass call sites. */
    protected static String sanitize(String value) {
        return LogMasker.sanitize(value);
    }

    /**
     * Returns the {@code ApproximateReceiveCount} attribute from an SQS message, defaulting
     * to 1 if absent or unparseable. Used to decide when a repeatedly-failing message has
     * exhausted its retry budget and should be marked FAILED.
     */
    protected static int parseReceiveCount(Message sqsMsg) {
        String raw = sqsMsg.attributes().get(MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT);
        try {
            return raw != null ? Integer.parseInt(raw) : 1;
        } catch (NumberFormatException e) {
            return 1;
        }
    }

    /**
     * Extends the visibility timeout of every message in the batch so that long processing
     * (rate-limit waits plus retry/backoff cycles) cannot outlive the queue's default
     * visibility timeout — which would let SQS redeliver messages that are still being
     * processed and cause duplicate sends. Best-effort: a failure here only means the
     * original queue timeout applies.
     */
    protected void extendVisibility(String queueUrl, java.util.List<Message> messages, int visibilitySeconds) {
        if (messages.isEmpty() || visibilitySeconds <= 0) return;
        try {
            var entries = new java.util.ArrayList<software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityBatchRequestEntry>(messages.size());
            for (int i = 0; i < messages.size(); i++) {
                entries.add(software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityBatchRequestEntry.builder()
                    .id(Integer.toString(i))
                    .receiptHandle(messages.get(i).receiptHandle())
                    .visibilityTimeout(visibilitySeconds)
                    .build());
            }
            sqsClient.changeMessageVisibilityBatch(
                software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityBatchRequest.builder()
                    .queueUrl(queueUrl)
                    .entries(entries)
                    .build());
        } catch (Exception e) {
            log.warn("Failed to extend visibility timeout for {} message(s) on {}: {}",
                messages.size(), queueUrl, e.getMessage());
        }
    }

    /**
     * Deletes an SQS message. Failures are logged as warnings — the message will
     * become visible again, which is preferable to crashing the consumer.
     */
    protected void deleteMessage(String queueUrl, String receiptHandle) {
        try {
            sqsClient.deleteMessage(DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(receiptHandle)
                .build());
        } catch (Exception e) {
            log.warn("Failed to delete SQS message from {}: {}", queueUrl, e.getMessage());
        }
    }

    /**
     * Deletes up to 10 SQS messages in one {@code DeleteMessageBatch} call instead of one
     * round trip per message. Failures — whole-call or per-entry — are logged as warnings:
     * the affected messages become visible again and are redelivered, which handlers must
     * already tolerate (at-least-once delivery).
     */
    protected void deleteMessageBatch(String queueUrl, java.util.List<Message> messages) {
        if (messages.isEmpty()) return;
        try {
            var entries = new java.util.ArrayList<software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequestEntry>(messages.size());
            for (int i = 0; i < messages.size(); i++) {
                entries.add(software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequestEntry.builder()
                    .id(Integer.toString(i))
                    .receiptHandle(messages.get(i).receiptHandle())
                    .build());
            }
            var response = sqsClient.deleteMessageBatch(
                software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest.builder()
                    .queueUrl(queueUrl)
                    .entries(entries)
                    .build());
            if (response != null && !response.failed().isEmpty()) {
                log.warn("Failed to delete {} of {} message(s) from {} — they will become visible "
                        + "again and be redelivered. First error: {}",
                    response.failed().size(), messages.size(), queueUrl,
                    response.failed().getFirst().message());
            }
        } catch (Exception e) {
            log.warn("Failed to batch-delete {} message(s) from {}: {}", messages.size(), queueUrl, e.getMessage());
        }
    }
}
