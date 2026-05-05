package dev.expopush.backend.sqs.consumer;

import dev.expopush.api.NotificationHandlerRegistry;
import dev.expopush.api.NotificationOutcome;
import dev.expopush.backend.sqs.message.PushNotificationSqsMessage;
import dev.expopush.backend.sqs.message.PushReceiptSqsMessage;
import dev.expopush.core.ExpoGateway;
import dev.expopush.core.api.model.PushError;
import dev.expopush.core.api.model.PushMessage;
import dev.expopush.core.api.model.PushTicket;
import dev.expopush.core.api.model.PushTicketResponse;
import dev.expopush.core.exception.ExpoAuthException;
import dev.expopush.core.exception.ExpoRateLimitException;
import dev.expopush.core.exception.ExpoServerException;
import dev.expopush.core.ratelimit.ExpoRateLimiter;
import dev.expopush.core.security.PayloadEncryptor;
import io.github.resilience4j.retry.Retry;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import tools.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Consumes the Push Notification Queue (PNQ), sends batches to Expo, and routes each
 * per-message outcome to the registered handler.
 *
 * <p>Ticket-level outcome mapping:
 * <ul>
 *   <li>Ticket OK → post to Receipt Queue; callback fires later from
 *       {@link PushReceiptQueueConsumer} once delivery is confirmed.
 *   <li>Ticket {@code DeviceNotRegistered} → {@link NotificationOutcome#REJECTED}.
 *   <li>Ticket other error → {@link NotificationOutcome#INVALID}.
 *   <li>{@link ExpoAuthException} → log CRITICAL, halt consumer; messages stay in SQS
 *       until credentials are fixed and the service restarts.
 *   <li>Retries exhausted → {@link NotificationOutcome#FAILED} per device.
 * </ul>
 */
@Slf4j
public class PushNotificationQueueConsumer extends AbstractSqsConsumer {

    /**
     * Configuration parameters for {@link PushNotificationQueueConsumer}.
     * Groups the tuning knobs to keep the constructor within the 7-parameter limit.
     */
    public record Config(
        Retry sqsRetry,
        int batchMaxSize,
        int receiptDelaySeconds,
        int receiptPublishMaxAttempts,
        int maxPushRetryReceives,
        long drainTimeoutMs,
        String pushQueueName,
        String receiptQueueName
    ) {}

    private final ExpoGateway expoGateway;
    private final ExpoRateLimiter rateLimiter;
    private final PayloadEncryptor encryptor;
    private final Retry sqsRetry;
    private final ObjectMapper objectMapper;
    private final int batchMaxSize;
    private final int receiptDelaySeconds;
    private final int receiptPublishMaxAttempts;
    private final int maxPushRetryReceives;
    private final String pushQueueName;
    private final String receiptQueueName;
    private volatile String pushQueueUrl;
    private volatile String receiptQueueUrl;

    public PushNotificationQueueConsumer(
        SqsClient sqsClient,
        NotificationHandlerRegistry registry,
        ExpoGateway expoGateway,
        ExpoRateLimiter rateLimiter,
        PayloadEncryptor encryptor,
        ObjectMapper objectMapper,
        Config config
    ) {
        super(sqsClient, registry, "sqs-push-consumer", config.drainTimeoutMs());
        this.expoGateway = expoGateway;
        this.rateLimiter = rateLimiter;
        this.encryptor = encryptor;
        this.sqsRetry = config.sqsRetry();
        this.objectMapper = objectMapper;
        this.batchMaxSize = config.batchMaxSize();
        this.receiptDelaySeconds = config.receiptDelaySeconds();
        this.receiptPublishMaxAttempts = config.receiptPublishMaxAttempts();
        this.maxPushRetryReceives = config.maxPushRetryReceives();
        this.pushQueueName = config.pushQueueName();
        this.receiptQueueName = config.receiptQueueName();
    }

    @Override
    protected void onStart() {
        this.pushQueueUrl = resolveQueueUrl(sqsClient, pushQueueName);
        this.receiptQueueUrl = resolveQueueUrl(sqsClient, receiptQueueName);
        log.info("Push Notification Queue consumer configured — polling {}", pushQueueUrl);
    }

    @Override
    protected void processOneBatch() throws InterruptedException {
        ReceiveMessageResponse response = sqsClient.receiveMessage(
            ReceiveMessageRequest.builder()
                .queueUrl(pushQueueUrl)
                .maxNumberOfMessages(Math.min(batchMaxSize, 10))
                .waitTimeSeconds(20)
                .messageSystemAttributeNames(MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT)
                .build()
        );

        if (response == null || response.messages() == null) return;
        List<Message> sqsMessages = response.messages();
        if (sqsMessages.isEmpty()) return;

        log.debug("PNQ received {} message(s)", sqsMessages.size());

        List<PushNotificationSqsMessage> pushMessages = new ArrayList<>(sqsMessages.size());
        List<Message> validSqsMessages = new ArrayList<>(sqsMessages.size());

        for (Message m : sqsMessages) {
            try {
                pushMessages.add(objectMapper.readValue(m.body(), PushNotificationSqsMessage.class));
                validSqsMessages.add(m);
            } catch (Exception e) {
                log.error("Failed to deserialize PNQ message — discarding poison message: {}", e.getMessage());
                deleteMessage(pushQueueUrl, m.receiptHandle());
            }
        }

        if (pushMessages.isEmpty()) return;

        rateLimiter.acquire();

        List<PushMessage> expoMessages = pushMessages.stream()
            .map(this::toExpoMessageDecrypted)
            .toList();

        try {
            PushTicketResponse ticketResponse = expoGateway.sendNotifications(expoMessages);
            dispatchTickets(pushMessages, validSqsMessages, ticketResponse);

        } catch (ExpoAuthException e) {
            log.error("CRITICAL: Expo authentication failure — halting PNQ consumer. "
                + "Messages remain in SQS. Restore credentials and restart. Error: {}", e.getMessage());
            haltConsumer();

        } catch (ExpoRateLimitException | ExpoServerException e) {
            // Resilience4j exhausted retries for a retryable error (Expo down / rate-limited).
            // Return messages to the queue via visibility timeout; fire FAILED only when the
            // per-message receive count exceeds the configured ceiling.
            log.warn("Expo batch send failed after retries ({}) for {} message(s) — returning to queue",
                e.getClass().getSimpleName(), pushMessages.size());
            handleRetryableExhausted(pushMessages, validSqsMessages);

        } catch (Exception e) {
            // Non-retryable or unclassified batch failure — retry each message individually
            // to isolate any single culprit before giving up on the whole batch.
            log.warn("Expo batch send failed ({}), retrying {} message(s) individually",
                e.getMessage(), pushMessages.size());
            retryIndividually(pushMessages, validSqsMessages);
        }
    }

    /**
     * For messages whose Resilience4j retry budget is exhausted due to a retryable error:
     * leave them in the queue (do not delete) so SQS redelivers them. Once a message's
     * receive count reaches {@code maxPushRetryReceives}, fire FAILED and delete it.
     */
    private void handleRetryableExhausted(
        List<PushNotificationSqsMessage> pushMessages,
        List<Message> sqsMessages
    ) {
        for (int i = 0; i < pushMessages.size(); i++) {
            int receiveCount = parseReceiveCount(sqsMessages.get(i));
            if (receiveCount >= maxPushRetryReceives) {
                PushNotificationSqsMessage msg = decrypt(pushMessages.get(i));
                log.error("Message reached max SQS retry receives ({}) — firing FAILED: "
                    + "correlationId={} pushToken={}",
                    maxPushRetryReceives,
                    sanitize(msg.correlationId()),
                    sanitize(msg.pushToken()));
                notifyHandler(result(NotificationOutcome.FAILED, msg, null,
                    "Exceeded maximum SQS retry receive count (" + maxPushRetryReceives + ")"));
                deleteMessage(pushQueueUrl, sqsMessages.get(i).receiptHandle());
            }
            // else: leave in queue for SQS to redeliver after visibility timeout
        }
    }

    /**
     * Retries each message individually after a batch-level failure. This isolates a single
     * bad message so the remaining messages in the batch can succeed independently.
     */
    private void retryIndividually(
        List<PushNotificationSqsMessage> pushMessages,
        List<Message> sqsMessages
    ) throws InterruptedException {
        for (int i = 0; i < pushMessages.size(); i++) {
            if (Thread.currentThread().isInterrupted()) throw new InterruptedException();

            PushNotificationSqsMessage msg = pushMessages.get(i);
            Message sqsMsg = sqsMessages.get(i);

            rateLimiter.acquire();

            try {
                PushTicketResponse ticketResponse = expoGateway.sendNotifications(
                    List.of(toExpoMessageDecrypted(msg)));
                dispatchTickets(List.of(msg), List.of(sqsMsg), ticketResponse);

            } catch (ExpoAuthException e) {
                log.error("CRITICAL: Expo auth failure during individual retry — halting consumer: {}",
                    e.getMessage());
                haltConsumer();
                return;

            } catch (ExpoRateLimitException | ExpoServerException e) {
                // Retryable exhausted for this individual message — apply same receive-count logic
                int receiveCount = parseReceiveCount(sqsMsg);
                if (receiveCount >= maxPushRetryReceives) {
                    PushNotificationSqsMessage decryptedMsg = decrypt(msg);
                    log.error("Individual message reached max SQS retry receives ({}) — firing FAILED: "
                        + "correlationId={}", maxPushRetryReceives, sanitize(decryptedMsg.correlationId()));
                    notifyHandler(result(NotificationOutcome.FAILED, decryptedMsg, null,
                        "Exceeded maximum SQS retry receive count (" + maxPushRetryReceives + ")"));
                    deleteMessage(pushQueueUrl, sqsMsg.receiptHandle());
                }
                // else: leave in queue

            } catch (Exception e) {
                // Non-retryable for this individual message
                PushNotificationSqsMessage decryptedMsg = decrypt(msg);
                log.error("Individual send failed with non-retryable error: correlationId={} error={}",
                    sanitize(decryptedMsg.correlationId()), e.getMessage());
                notifyHandler(result(NotificationOutcome.FAILED, decryptedMsg, null, e.getMessage()));
                deleteMessage(pushQueueUrl, sqsMsg.receiptHandle());
            }
        }
    }

    private void dispatchTickets(
        List<PushNotificationSqsMessage> pushMessages,
        List<Message> sqsMessages,
        PushTicketResponse response
    ) {
        List<PushError> batchErrors = (response != null && response.getErrors() != null)
            ? response.getErrors() : Collections.emptyList();

        if (!batchErrors.isEmpty()) {
            handleBatchErrors(pushMessages, sqsMessages, batchErrors);
            return;
        }

        List<PushTicket> tickets = (response != null && response.getData() != null)
            ? response.getData() : Collections.emptyList();

        for (int i = 0; i < pushMessages.size(); i++) {
            PushNotificationSqsMessage msg = pushMessages.get(i);
            PushTicket ticket = i < tickets.size() ? tickets.get(i) : null;
            processTicketResult(msg, sqsMessages.get(i), ticket);
        }
    }

    private void handleBatchErrors(
        List<PushNotificationSqsMessage> pushMessages,
        List<Message> sqsMessages,
        List<PushError> errors
    ) {
        String detail = errors.stream()
            .map(e -> e.getCode() + ": " + e.getMessage())
            .reduce((a, b) -> a + "; " + b).orElse("unknown");
        log.error("Expo batch-level errors — treating as INVALID for {} message(s): {}",
            pushMessages.size(), detail);
        for (int i = 0; i < pushMessages.size(); i++) {
            notifyHandler(result(NotificationOutcome.INVALID, decrypt(pushMessages.get(i)), null, detail));
            deleteMessage(pushQueueUrl, sqsMessages.get(i).receiptHandle());
        }
    }

    private void processTicketResult(PushNotificationSqsMessage msg, Message sqsMsg, PushTicket ticket) {
        if (ticket != null && ticket.getStatus() == PushTicket.StatusEnum.OK) {
            handleOkTicket(msg, sqsMsg, ticket.getId());
        } else {
            handleErrorTicket(msg, sqsMsg, ticket);
        }
    }

    private void handleOkTicket(PushNotificationSqsMessage msg, Message sqsMsg, String ticketId) {
        if (postToReceiptQueue(msg, ticketId)) {
            deleteMessage(pushQueueUrl, sqsMsg.receiptHandle());
        } else {
            notifyHandler(result(NotificationOutcome.UNKNOWN, decrypt(msg), ticketId, null));
            deleteMessage(pushQueueUrl, sqsMsg.receiptHandle());
        }
    }

    private void handleErrorTicket(PushNotificationSqsMessage msg, Message sqsMsg, PushTicket ticket) {
        String errorDetail = extractTicketError(ticket);
        NotificationOutcome outcome = "DeviceNotRegistered".equals(errorDetail)
            ? NotificationOutcome.REJECTED : NotificationOutcome.INVALID;
        notifyHandler(result(outcome, decrypt(msg), null, errorDetail));
        deleteMessage(pushQueueUrl, sqsMsg.receiptHandle());
    }

    private String extractTicketError(PushTicket ticket) {
        if (ticket == null) return "null ticket from Expo";
        var details = ticket.getDetails();
        if (details != null && details.getError() != null) {
            return details.getError();
        }
        return ticket.getMessage() != null ? ticket.getMessage() : "unknown ticket error";
    }

    private boolean postToReceiptQueue(PushNotificationSqsMessage msg, String ticketId) {
        try {
            sqsRetry.executeRunnable(() -> sendReceiptMessage(msg, ticketId));
            return true;
        } catch (Exception e) {
            log.error("Receipt follow-up could not be queued; marking UNKNOWN. "
                    + "ticketId={} correlationId={} handlerId={} pushToken={} receiptQueueUrl={} attempts<={}",
                sanitize(ticketId), sanitize(msg.correlationId()), sanitize(msg.handlerId()),
                sanitize(msg.pushToken()), receiptQueueUrl, receiptPublishMaxAttempts, e);
            return false;
        }
    }

    private void sendReceiptMessage(PushNotificationSqsMessage msg, String ticketId) {
        try {
            PushReceiptSqsMessage receiptMsg = new PushReceiptSqsMessage(
                ticketId, msg.pushToken(), msg.correlationId(),
                msg.metadata(), msg.handlerId(), msg.title(), msg.body());
            String body = objectMapper.writeValueAsString(receiptMsg);
            sqsClient.sendMessage(SendMessageRequest.builder()
                .queueUrl(receiptQueueUrl)
                .messageBody(body)
                .delaySeconds(receiptDelaySeconds)
                .build());
            log.debug("Posted receipt message for ticketId={} with {}s delay", sanitize(ticketId), receiptDelaySeconds);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to serialize receipt follow-up message", e);
        }
    }

    private PushMessage toExpoMessageDecrypted(PushNotificationSqsMessage msg) {
        PushMessage pm = new PushMessage();
        pm.setTo(List.of(msg.pushToken()));
        pm.setTitle(encryptor.decrypt(msg.title()));
        pm.setBody(encryptor.decrypt(msg.body()));
        pm.setPriority(PushMessage.PriorityEnum.DEFAULT);
        return pm;
    }

    private PushNotificationSqsMessage decrypt(PushNotificationSqsMessage msg) {
        return new PushNotificationSqsMessage(
            msg.pushToken(),
            encryptor.decrypt(msg.title()),
            encryptor.decrypt(msg.body()),
            msg.correlationId(),
            decryptMap(msg.metadata()),
            msg.handlerId()
        );
    }

    private Map<String, String> decryptMap(Map<String, String> map) {
        if (map == null) return Map.of();
        return map.entrySet().stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                e -> encryptor.decrypt(e.getValue())
            ));
    }

    private static String resolveQueueUrl(SqsClient client, String queueName) {
        return client.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build()).queueUrl();
    }
}
