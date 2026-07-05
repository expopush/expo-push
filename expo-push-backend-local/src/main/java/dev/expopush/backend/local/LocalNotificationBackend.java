package dev.expopush.backend.local;

import dev.expopush.api.NotificationCommand;
import dev.expopush.api.NotificationHandlerRegistry;
import dev.expopush.api.NotificationOutcome;
import dev.expopush.api.NotificationSubmissionException;
import dev.expopush.backend.api.NotificationBackend;
import dev.expopush.backend.api.ResultDispatcher;
import dev.expopush.core.ExpoErrors;
import dev.expopush.core.ExpoGateway;
import dev.expopush.core.ExpoMessages;
import dev.expopush.core.api.model.PushMessage;
import dev.expopush.core.api.model.PushTicket;
import dev.expopush.core.api.model.PushTicketResponse;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

/**
 * Local implementation of {@link NotificationBackend} that uses a {@link LocalReceiptOrchestrator}
 * for asynchronous receipt polling.
 *
 * <p>{@link #submit} hands the command to {@code submissionExecutor} and returns immediately —
 * the Expo call (including its retry/backoff cycle) never runs on the caller's thread. Send
 * failures are therefore reported as {@link NotificationOutcome#FAILED} through the registered
 * handler, not as exceptions from {@code submit}.
 */
@Slf4j
public class LocalNotificationBackend implements NotificationBackend {

    private final ExpoGateway expoGateway;
    private final LocalReceiptOrchestrator orchestrator;
    private final ResultDispatcher dispatcher;
    private final Executor submissionExecutor;
    private final long initialDelayMillis;

    public LocalNotificationBackend(
            ExpoGateway expoGateway,
            LocalReceiptOrchestrator orchestrator,
            NotificationHandlerRegistry registry,
            Executor submissionExecutor,
            long initialDelayMillis) {
        this.expoGateway = expoGateway;
        this.orchestrator = orchestrator;
        this.dispatcher = new ResultDispatcher(registry);
        this.submissionExecutor = submissionExecutor;
        this.initialDelayMillis = initialDelayMillis;
    }

    @Override
    public void submit(NotificationCommand command) {
        log.debug("Locally submitting push notification for correlationId={}", command.correlationId());
        try {
            submissionExecutor.execute(() -> sendToExpo(command));
        } catch (RejectedExecutionException e) {
            throw new NotificationSubmissionException(
                "Local submission executor rejected the command (shutting down?)", e);
        }
    }

    private void sendToExpo(NotificationCommand command) {
        try {
            PushMessage expoMsg = ExpoMessages.toPushMessage(
                command.pushToken(), command.title(), command.body(), command.options());

            PushTicketResponse response = expoGateway.sendNotifications(List.of(expoMsg));

            if (response == null || response.getData() == null || response.getData().isEmpty()) {
                handleBatchError(command, response);
                return;
            }

            PushTicket ticket = response.getData().getFirst();
            if (ticket.getStatus() == PushTicket.StatusEnum.OK) {
                boolean queued = orchestrator.submitTask(new DelayedReceiptTask(
                    ticket.getId(), command, initialDelayMillis, 1));

                if (!queued) {
                    // Queue full, notify UNKNOWN immediately
                    dispatcher.dispatch(ResultDispatcher.result(NotificationOutcome.UNKNOWN, command, ticket.getId(), "Local queue full"));
                }
            } else {
                handleTicketError(command, ticket);
            }

        } catch (Exception e) {
            log.error("Failed to send local notification for correlationId={}", command.correlationId(), e);
            dispatcher.dispatch(ResultDispatcher.result(NotificationOutcome.FAILED, command, null,
                e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName()));
        }
    }

    private void handleBatchError(NotificationCommand command, PushTicketResponse response) {
        if (response != null && response.getErrors() != null && !response.getErrors().isEmpty()) {
            // Expo rejected the request outright — nothing was sent, safe to retry.
            dispatcher.dispatch(ResultDispatcher.result(NotificationOutcome.FAILED, command, null,
                response.getErrors().getFirst().getMessage()));
        } else {
            // 200 with no ticket and no error: malformed/truncated response. The request may
            // have been processed, so retrying risks a duplicate push.
            dispatcher.dispatch(ResultDispatcher.result(NotificationOutcome.UNKNOWN, command, null,
                "Empty response from Expo — delivery state unknown"));
        }
    }

    private void handleTicketError(NotificationCommand command, PushTicket ticket) {
        String detail = ExpoErrors.errorOf(ticket);
        dispatcher.dispatch(ResultDispatcher.result(
            ExpoErrors.outcomeFor(detail), command, null, detail));
    }
}
