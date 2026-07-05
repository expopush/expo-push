package dev.expopush.backend.local;

import dev.expopush.api.NotificationCommand;
import dev.expopush.api.NotificationHandlerRegistry;
import dev.expopush.api.NotificationOutcome;
import dev.expopush.api.NotificationResult;
import dev.expopush.api.NotificationResultHandler;
import dev.expopush.api.NotificationSubmissionException;
import dev.expopush.backend.api.NotificationBackend;
import dev.expopush.core.ExpoGateway;
import dev.expopush.core.api.model.PushMessage;
import dev.expopush.core.api.model.PushTicket;
import dev.expopush.core.api.model.PushTicketResponse;
import lombok.RequiredArgsConstructor;
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
@RequiredArgsConstructor
public class LocalNotificationBackend implements NotificationBackend {

    private final ExpoGateway expoGateway;
    private final LocalReceiptOrchestrator orchestrator;
    private final NotificationHandlerRegistry registry;
    private final Executor submissionExecutor;
    private final long initialDelayMillis;

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
            PushMessage expoMsg = new PushMessage();
            expoMsg.setTo(List.of(command.pushToken()));
            expoMsg.setTitle(command.title());
            expoMsg.setBody(command.body());
            expoMsg.setPriority(PushMessage.PriorityEnum.DEFAULT);

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
                    notifyHandler(result(NotificationOutcome.UNKNOWN, command, ticket.getId(), "Local queue full"));
                }
            } else {
                handleTicketError(command, ticket);
            }

        } catch (Exception e) {
            log.error("Failed to send local notification for correlationId={}", command.correlationId(), e);
            notifyHandler(result(NotificationOutcome.FAILED, command, null,
                e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName()));
        }
    }

    private void handleBatchError(NotificationCommand command, PushTicketResponse response) {
        if (response != null && response.getErrors() != null && !response.getErrors().isEmpty()) {
            // Expo rejected the request outright — nothing was sent, safe to retry.
            notifyHandler(result(NotificationOutcome.FAILED, command, null,
                response.getErrors().getFirst().getMessage()));
        } else {
            // 200 with no ticket and no error: malformed/truncated response. The request may
            // have been processed, so retrying risks a duplicate push.
            notifyHandler(result(NotificationOutcome.UNKNOWN, command, null,
                "Empty response from Expo — delivery state unknown"));
        }
    }

    private void handleTicketError(NotificationCommand command, PushTicket ticket) {
        String detail = extractError(ticket);
        NotificationOutcome outcome = "DeviceNotRegistered".equals(detail)
            ? NotificationOutcome.REJECTED
            : NotificationOutcome.INVALID;
        notifyHandler(result(outcome, command, null, detail));
    }

    private String extractError(PushTicket ticket) {
        var details = ticket.getDetails();
        if (details != null && details.getError() != null) {
            return details.getError();
        }
        return ticket.getMessage();
    }

    private void notifyHandler(NotificationResult result) {
        NotificationResultHandler handler = registry.getHandler(result.handlerId());
        if (handler != null) {
            try {
                handler.handleResult(result);
            } catch (Exception e) {
                log.error("Handler threw exception for handlerId={} correlationId={}",
                    result.handlerId(), result.correlationId(), e);
            }
        }
    }

    private NotificationResult result(NotificationOutcome outcome, NotificationCommand cmd, String ticketId, String error) {
        return new NotificationResult(
            outcome, cmd.handlerId(), cmd.correlationId(), cmd.pushToken(),
            cmd.title(), cmd.body(), ticketId, error, cmd.metadata()
        );
    }
}
