package dev.expopush.backend.local;

import dev.expopush.api.LogMasker;
import dev.expopush.api.NotificationHandlerRegistry;
import dev.expopush.api.NotificationOutcome;
import dev.expopush.api.NotificationResult;
import dev.expopush.backend.api.ResultDispatcher;
import dev.expopush.core.ExpoErrors;
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
    private final ResultDispatcher dispatcher;
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
        this.dispatcher = new ResultDispatcher(registry);
        this.maxAttempts = maxAttempts;
        this.retryDelayMillis = retryDelayMillis;
        this.maxQueueSize = maxQueueSize;
    }

    @PostConstruct
    public void start() {
        workerThread = Thread.ofVirtual().name("expo-local-receipt-orchestrator").start(this::pollLoop);
        log.info("Local Receipt Orchestrator started (single virtual-thread worker, maxQueueSize={})", maxQueueSize);
    }

    /**
     * Stops the worker and drains any still-queued receipt checks as UNKNOWN — this
     * backend is in-memory, so undelivered tasks would otherwise vanish silently and
     * their handlers would never learn a final outcome.
     */
    @PreDestroy
    public void stop() {
        running.set(false);
        if (workerThread != null) {
            workerThread.interrupt();
        }
        java.util.List<DelayedReceiptTask> undrained = new java.util.ArrayList<>();
        queue.drainTo(undrained);
        for (DelayedReceiptTask task : undrained) {
            dispatcher.dispatch(new NotificationResult(
                NotificationOutcome.UNKNOWN,
                task.getCommand().handlerId(),
                task.getCommand().correlationId(),
                task.getCommand().pushToken(),
                task.getCommand().title(),
                task.getCommand().body(),
                task.getTicketId(),
                "Shutdown before receipt confirmation — delivery state unknown",
                task.getCommand().metadata()
            ));
        }
        if (!undrained.isEmpty()) {
            log.warn("Drained {} pending receipt check(s) as UNKNOWN on shutdown", undrained.size());
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
                maxQueueSize, LogMasker.sanitize(task.getTicketId()));
            return false;
        }
        queue.put(task); // DelayQueue is unbounded — put() never blocks
        return true;
    }

    /** Expo accepts up to 300 ticket IDs per getReceipts call. */
    private static final int MAX_RECEIPTS_PER_CALL = 300;

    private void pollLoop() {
        while (running.get()) {
            try {
                // Block for the first due task, then drain every other already-due task
                // (DelayQueue.poll only returns expired elements) so one Expo call covers
                // the whole due set instead of one HTTP round trip per ticket.
                DelayedReceiptTask first = queue.take();
                List<DelayedReceiptTask> due = new java.util.ArrayList<>();
                due.add(first);
                DelayedReceiptTask next;
                while (due.size() < MAX_RECEIPTS_PER_CALL && (next = queue.poll()) != null) {
                    due.add(next);
                }
                processBatch(due);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("Unexpected error in local receipt poll loop", e);
            }
        }
    }

    private void processBatch(List<DelayedReceiptTask> tasks) {
        List<String> ticketIds = tasks.stream().map(DelayedReceiptTask::getTicketId).toList();
        java.util.Map<String, PushReceipt> receipts;
        try {
            log.debug("Checking receipts for {} ticket(s)", ticketIds.size());
            PushReceiptResponse response = expoGateway.getReceipts(ticketIds);
            receipts = (response != null && response.getData() != null)
                ? response.getData() : java.util.Map.of();
        } catch (Exception e) {
            log.warn("Failed to fetch {} receipt(s); will retry if attempts remain: {}",
                ticketIds.size(), e.getMessage());
            tasks.forEach(this::handleMissingReceipt);
            return;
        }

        for (DelayedReceiptTask task : tasks) {
            PushReceipt receipt = receipts.get(task.getTicketId());
            if (receipt == null) {
                handleMissingReceipt(task);
                continue;
            }
            boolean accepted = receipt.getStatus() == PushReceipt.StatusEnum.OK;
            NotificationOutcome outcome = accepted
                ? NotificationOutcome.ACCEPTED
                : ExpoErrors.outcomeFor(ExpoErrors.errorOf(receipt));
            dispatcher.dispatch(new NotificationResult(
                outcome,
                task.getCommand().handlerId(),
                task.getCommand().correlationId(),
                task.getCommand().pushToken(),
                task.getCommand().title(),
                task.getCommand().body(),
                task.getTicketId(),
                accepted ? null : ExpoErrors.errorOf(receipt),
                task.getCommand().metadata()
            ));
        }
    }

    private void handleMissingReceipt(DelayedReceiptTask task) {
        if (task.getAttempt() < maxAttempts) {
            if (queue.size() >= maxQueueSize) {
                log.warn("Local receipt queue full; cannot reschedule ticket {} — marking UNKNOWN",
                    LogMasker.sanitize(task.getTicketId()));
                dispatcher.dispatch(new NotificationResult(
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
            log.warn("Max attempts reached for ticket {}; marking UNKNOWN", LogMasker.sanitize(task.getTicketId()));
            dispatcher.dispatch(new NotificationResult(
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

}
