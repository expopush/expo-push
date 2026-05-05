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

/**
 * Local implementation of {@link NotificationBackend} that uses a {@link LocalReceiptOrchestrator}
 * for asynchronous receipt polling.
 */
@Slf4j
@RequiredArgsConstructor
public class LocalNotificationBackend implements NotificationBackend {

    private final ExpoGateway expoGateway;
    private final LocalReceiptOrchestrator orchestrator;
    private final NotificationHandlerRegistry registry;
    private final long initialDelayMillis;

    @Override
    public void submit(NotificationCommand command) {
        log.debug("Locally submitting push notification for correlationId={}", command.correlationId());
        
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
            log.error("Failed to submit local notification for correlationId={}", command.correlationId(), e);
            throw new NotificationSubmissionException("Failed to send to Expo", e);
        }
    }

    private void handleBatchError(NotificationCommand command, PushTicketResponse response) {
        String detail = (response != null && response.getErrors() != null && !response.getErrors().isEmpty())
            ? response.getErrors().getFirst().getMessage()
            : "Empty response from Expo";
        notifyHandler(result(NotificationOutcome.FAILED, command, null, detail));
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
