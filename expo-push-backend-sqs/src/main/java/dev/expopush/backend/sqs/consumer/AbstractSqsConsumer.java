package dev.expopush.backend.sqs.consumer;

import dev.expopush.api.NotificationHandlerRegistry;
import dev.expopush.api.NotificationOutcome;
import dev.expopush.api.NotificationResult;
import dev.expopush.api.NotificationResultHandler;
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

    private final String threadName;
    private final long drainTimeoutMs;
    /** Controls whether the poll loop keeps iterating. */
    private volatile boolean loopActive = false;
    /** SmartLifecycle state — returned by {@link #isRunning()}. */
    private volatile boolean running = false;
    private final java.util.concurrent.atomic.AtomicReference<Thread> consumerThread = new java.util.concurrent.atomic.AtomicReference<>();

    protected AbstractSqsConsumer(
        SqsClient sqsClient,
        NotificationHandlerRegistry registry,
        String threadName,
        long drainTimeoutMs
    ) {
        this.sqsClient = sqsClient;
        this.registry = registry;
        this.threadName = threadName;
        this.drainTimeoutMs = drainTimeoutMs;
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
    public void stop(Runnable callback) {
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
     * Signals the poll loop to exit without attempting to join the current thread.
     * Safe to call from within {@link #processOneBatch()} — the loop checks {@code loopActive}
     * after each batch and exits naturally, avoiding the self-join deadlock that would occur
     * if {@link #stop()} were called from the consumer thread itself.
     */
    protected void haltConsumer() {
        loopActive = false;
        running = false;
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
        NotificationResultHandler handler = registry.getHandler(result.handlerId());
        if (handler != null) {
            try {
                handler.handleResult(result);
            } catch (Exception e) {
                log.error("Handler failure requires manual intervention: handlerId={} outcome={} "
                        + "correlationId={} ticketId={} pushToken={}",
                    sanitize(result.handlerId()), result.outcome(), sanitize(result.correlationId()),
                    sanitize(result.ticketId()), sanitize(result.pushToken()), e);
            }
        }
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

    /**
     * Strips newline and carriage-return characters from a user-supplied string before
     * it is emitted to a log statement, preventing log-injection attacks.
     */
    protected static String sanitize(String value) {
        if (value == null) return "(null)";
        return value.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ');
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
}
