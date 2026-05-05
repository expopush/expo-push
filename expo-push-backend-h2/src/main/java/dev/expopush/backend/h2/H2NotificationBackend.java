package dev.expopush.backend.h2;

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
import dev.expopush.core.security.PayloadEncryptor;
import tools.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Instant;
import java.util.List;

/**
 * Persistent implementation of {@link NotificationBackend} that uses H2 database
 * to store pending receipt checks.
 */
@Slf4j
@RequiredArgsConstructor
public class H2NotificationBackend implements NotificationBackend {

    private final ExpoGateway expoGateway;
    private final JdbcTemplate jdbcTemplate;
    private final NotificationHandlerRegistry registry;
    private final ObjectMapper objectMapper;
    private final PayloadEncryptor encryptor;
    private final long initialDelaySeconds;

    @Override
    public void submit(NotificationCommand command) {
        log.debug("Persistent H2 submission for correlationId={}", command.correlationId());

        // Outbox write BEFORE calling Expo. A crash between here and activatePendingRow is
        // caught on the next startup by H2ReceiptOrchestrator.cleanupStalePendingRows.
        try {
            insertPendingRow(command);
        } catch (Exception e) {
            log.error("Failed to write outbox row for correlationId={}", command.correlationId(), e);
            throw new NotificationSubmissionException("Failed to persist outbox record", e);
        }

        try {
            PushMessage expoMsg = new PushMessage();
            expoMsg.setTo(List.of(command.pushToken()));
            expoMsg.setTitle(command.title());
            expoMsg.setBody(command.body());
            expoMsg.setPriority(PushMessage.PriorityEnum.DEFAULT);

            PushTicketResponse response = expoGateway.sendNotifications(List.of(expoMsg));

            if (response == null || response.getData() == null || response.getData().isEmpty()) {
                deletePendingRow(command.correlationId());
                handleBatchError(command, response);
                return;
            }

            PushTicket ticket = response.getData().getFirst();
            if (ticket.getStatus() == PushTicket.StatusEnum.OK) {
                activatePendingRow(command.correlationId(), ticket.getId());
            } else {
                deletePendingRow(command.correlationId());
                handleTicketError(command, ticket);
            }

        } catch (Exception e) {
            deletePendingRow(command.correlationId());
            log.error("Failed H2 submission for correlationId={}", command.correlationId(), e);
            throw new NotificationSubmissionException("Failed to send to Expo via H2 backend", e);
        }
    }

    private void insertPendingRow(NotificationCommand command) {
        String json;
        try {
            json = objectMapper.writeValueAsString(command);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to serialize notification command for correlationId=" + command.correlationId(), e);
        }
        jdbcTemplate.update(
            "INSERT INTO pending_receipts (correlation_id, ticket_id, command_json, attempt, check_after, state) "
                + "VALUES (?, NULL, ?, 1, ?, 'PENDING')",
            command.correlationId(), encryptor.encrypt(json), Instant.now().plusSeconds(initialDelaySeconds)
        );
    }

    private void activatePendingRow(String correlationId, String ticketId) {
        jdbcTemplate.update(
            "UPDATE pending_receipts SET ticket_id = ?, state = 'READY', check_after = ? WHERE correlation_id = ?",
            ticketId, Instant.now().plusSeconds(initialDelaySeconds), correlationId
        );
    }

    private void deletePendingRow(String correlationId) {
        jdbcTemplate.update("DELETE FROM pending_receipts WHERE correlation_id = ?", correlationId);
    }

    private void handleBatchError(NotificationCommand command, PushTicketResponse response) {
        String detail = (response != null && response.getErrors() != null && !response.getErrors().isEmpty())
            ? response.getErrors().get(0).getMessage()
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
