package dev.expopush.backend.local;

import dev.expopush.api.NotificationHandlerRegistry;
import dev.expopush.api.NotificationOutcome;
import dev.expopush.api.NotificationResult;
import dev.expopush.api.NotificationResultHandler;
import dev.expopush.core.ExpoGateway;
import dev.expopush.core.api.model.PushReceipt;
import dev.expopush.core.api.model.PushReceiptResponse;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Orchestrates the second phase of the Expo push process (receipt checking)
 * using a local DelayQueue and virtual threads.
 */
@Slf4j
public class LocalReceiptOrchestrator {

    private final DelayQueue<DelayedReceiptTask> queue = new DelayQueue<>();
    private final ExpoGateway expoGateway;
    private final NotificationHandlerRegistry registry;
    private final int maxAttempts;
    private final long retryDelayMillis;
    private final int maxQueueSize;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private Thread workerThread;

    public LocalReceiptOrchestrator(
            ExpoGateway expoGateway,
            NotificationHandlerRegistry registry,
            int maxAttempts,
            long retryDelayMillis,
            int maxQueueSize) {
        this.expoGateway = expoGateway;
        this.registry = registry;
        this.maxAttempts = maxAttempts;
        this.retryDelayMillis = retryDelayMillis;
        this.maxQueueSize = maxQueueSize;
    }

    @PostConstruct
    public void start() {
        workerThread = Thread.ofVirtual().name("expo-local-receipt-orchestrator").start(this::pollLoop);
        log.info("Local Receipt Orchestrator started using virtual threads (maxQueueSize={})", maxQueueSize);
    }

    @PreDestroy
    public void stop() {
        running.set(false);
        if (workerThread != null) {
            workerThread.interrupt();
        }
    }

    /** Current number of queued receipt-check tasks (used by the metrics gauge). */
    public int queueDepth() {
        return queue.size();
    }

    /**
     * Queues a receipt-check task. The size cap is approximate: the check-then-offer is not
     * atomic, so concurrent submitters may briefly overshoot {@code maxQueueSize}.
     */
    public boolean submitTask(DelayedReceiptTask task) {
        if (queue.size() >= maxQueueSize) {
            log.warn("Local receipt queue is full ({}); dropping task for ticket {}",
                maxQueueSize, sanitize(task.getTicketId()));
            return false;
        }
        queue.put(task); // DelayQueue is unbounded — put() never blocks
        return true;
    }

    private void pollLoop() {
        while (running.get()) {
            try {
                DelayedReceiptTask task = queue.take(); // Blocks until a task is ready
                processTask(task);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("Unexpected error in local receipt poll loop", e);
            }
        }
    }

    private void processTask(DelayedReceiptTask task) {
        try {
            log.debug("Checking receipt for ticket {} (attempt {})", task.getTicketId(), task.getAttempt());
            PushReceiptResponse response = expoGateway.getReceipts(List.of(task.getTicketId()));
            
            if (response == null || response.getData() == null || !response.getData().containsKey(task.getTicketId())) {
                handleMissingReceipt(task);
                return;
            }

            PushReceipt receipt = response.getData().get(task.getTicketId());
            boolean accepted = receipt.getStatus() == PushReceipt.StatusEnum.OK;
            NotificationOutcome outcome = accepted ? NotificationOutcome.ACCEPTED : mapError(receipt);

            notifyHandler(new NotificationResult(
                outcome,
                task.getCommand().handlerId(),
                task.getCommand().correlationId(),
                task.getCommand().pushToken(),
                task.getCommand().title(),
                task.getCommand().body(),
                task.getTicketId(),
                accepted ? null : extractError(receipt),
                task.getCommand().metadata()
            ));

        } catch (Exception e) {
            log.warn("Failed to fetch receipt for ticket {}; will retry if attempts remain: {}", task.getTicketId(), e.getMessage());
            handleMissingReceipt(task);
        }
    }

    private void handleMissingReceipt(DelayedReceiptTask task) {
        if (task.getAttempt() < maxAttempts) {
            if (queue.size() >= maxQueueSize) {
                log.warn("Local receipt queue full; cannot reschedule ticket {} — marking UNKNOWN",
                    sanitize(task.getTicketId()));
                notifyHandler(new NotificationResult(
                    NotificationOutcome.UNKNOWN,
                    task.getCommand().handlerId(),
                    task.getCommand().correlationId(),
                    task.getCommand().pushToken(),
                    task.getCommand().title(),
                    task.getCommand().body(),
                    task.getTicketId(),
                    "Local receipt queue full — retry abandoned",
                    task.getCommand().metadata()
                ));
            } else {
                // DelayQueue is unbounded — put() never blocks.
                queue.put(new DelayedReceiptTask(
                    task.getTicketId(), task.getCommand(), retryDelayMillis, task.getAttempt() + 1));
            }
        } else {
            log.warn("Max attempts reached for ticket {}; marking UNKNOWN", sanitize(task.getTicketId()));
            notifyHandler(new NotificationResult(
                NotificationOutcome.UNKNOWN,
                task.getCommand().handlerId(),
                task.getCommand().correlationId(),
                task.getCommand().pushToken(),
                task.getCommand().title(),
                task.getCommand().body(),
                task.getTicketId(),
                "Receipt not found after " + maxAttempts + " attempts",
                task.getCommand().metadata()
            ));
        }
    }

    private void notifyHandler(NotificationResult result) {
        NotificationResultHandler handler = registry.getHandler(result.handlerId());
        if (handler != null) {
            try {
                handler.handleResult(result);
            } catch (Exception e) {
                log.error("Handler threw exception for result {}", result, e);
            }
        } else {
            log.warn("No handler found for ID {}", sanitize(result.handlerId()));
        }
    }

    private NotificationOutcome mapError(PushReceipt receipt) {
        String error = extractError(receipt);
        return "DeviceNotRegistered".equals(error) ? NotificationOutcome.REJECTED : NotificationOutcome.INVALID;
    }

    private String extractError(PushReceipt receipt) {
        var details = receipt.getDetails();
        if (details != null && details.getError() != null) {
            return details.getError();
        }
        return receipt.getMessage();
    }

    private static String sanitize(String value) {
        if (value == null) return "(null)";
        return value.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ');
    }
}
