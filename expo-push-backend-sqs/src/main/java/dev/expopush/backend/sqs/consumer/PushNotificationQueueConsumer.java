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
        int maxPushRetryReceives,
        int inFlightVisibilitySeconds,
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
    private final int maxPushRetryReceives;
    private final int inFlightVisibilitySeconds;
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
        this.maxPushRetryReceives = config.maxPushRetryReceives();
        this.inFlightVisibilitySeconds = config.inFlightVisibilitySeconds();
        this.pushQueueName = config.pushQueueName();
        this.receiptQueueName = config.receiptQueueName();
    }

    @Override
    protected void onStart() {
        this.pushQueueUrl = resolveQueueUrl(sqsClient, pushQueueName);
        this.receiptQueueUrl = resolveQueueUrl(sqsClient, receiptQueueName);
        log.info("Push Notification Queue consumer configured — polling {}", pushQueueUrl);
    }

    /**
     * One PNQ message in all the forms the pipeline needs: the raw (still-encrypted) payload
     * for receipt-queue forwarding, the decrypted payload for results, the SQS envelope, and
     * the Expo request object.
     */
    private record InFlight(
        PushNotificationSqsMessage raw,
        PushNotificationSqsMessage decrypted,
        Message sqsMessage,
        PushMessage expoMessage
    ) {}

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

        List<InFlight> batch = new ArrayList<>(sqsMessages.size());
        for (Message m : sqsMessages) {
            InFlight entry = parseAndDecrypt(m);
            if (entry != null) {
                batch.add(entry);
            }
        }

        if (batch.isEmpty()) return;

        // Processing can take minutes in the worst case (rate-limit wait + Resilience4j
        // backoff + per-message retries). Extend visibility up front so the queue's default
        // timeout cannot expire mid-processing and trigger a duplicate delivery.
        extendVisibility(pushQueueUrl,
            batch.stream().map(InFlight::sqsMessage).toList(),
            inFlightVisibilitySeconds);

        rateLimiter.acquire();

        List<PushMessage> expoMessages = batch.stream().map(InFlight::expoMessage).toList();

        try {
            PushTicketResponse ticketResponse = expoGateway.sendNotifications(expoMessages);
            dispatchTickets(batch, ticketResponse);

        } catch (ExpoAuthException e) {
            log.error("CRITICAL: Expo authentication failure — halting PNQ consumer. "
                + "Messages remain in SQS. Restore credentials and restart. Error: {}", e.getMessage());
            haltConsumer();

        } catch (ExpoRateLimitException | ExpoServerException e) {
            // Resilience4j exhausted retries for a retryable error (Expo down / rate-limited).
            // Return messages to the queue via visibility timeout; fire FAILED only when the
            // per-message receive count exceeds the configured ceiling.
            log.warn("Expo batch send failed after retries ({}) for {} message(s) — returning to queue",
                e.getClass().getSimpleName(), batch.size());
            handleRetryableExhausted(batch);

        } catch (Exception e) {
            // Non-retryable or unclassified batch failure — retry each message individually
            // to isolate any single culprit before giving up on the whole batch.
            log.warn("Expo batch send failed ({}), retrying {} message(s) individually",
                e.getMessage(), batch.size());
            retryIndividually(batch);
        }
    }

    /**
     * Deserializes and decrypts one SQS message, resolving unprocessable messages terminally:
     * JSON poison messages are deleted; undecryptable messages (rotated key, corrupt
     * ciphertext) fire FAILED and are deleted — either escaping to the poll loop would stall
     * the queue on endless redelivery of the same batch. Returns null when the message was
     * resolved here.
     */
    private InFlight parseAndDecrypt(Message m) {
        PushNotificationSqsMessage raw;
        try {
            raw = objectMapper.readValue(m.body(), PushNotificationSqsMessage.class);
        } catch (Exception e) {
            log.error("Failed to deserialize PNQ message — discarding poison message: {}", e.getMessage());
            deleteMessage(pushQueueUrl, m.receiptHandle());
            return null;
        }
        try {
            PushNotificationSqsMessage decrypted = decrypt(raw);
            return new InFlight(raw, decrypted, m, toExpoMessage(decrypted));
        } catch (Exception e) {
            log.error("Failed to decrypt PNQ message — firing FAILED: correlationId={} error={}",
                sanitize(raw.correlationId()), e.getMessage());
            notifyHandler(result(NotificationOutcome.FAILED,
                new PushNotificationSqsMessage(raw.pushToken(), null, null,
                    raw.correlationId(), Map.of(), raw.handlerId()),
                null, "Payload decryption failed — verify the configured encryption key"));
            deleteMessage(pushQueueUrl, m.receiptHandle());
            return null;
        }
    }

    /**
     * For messages whose Resilience4j retry budget is exhausted due to a retryable error:
     * leave them in the queue (do not delete) so SQS redelivers them. Once a message's
     * receive count reaches {@code maxPushRetryReceives}, fire FAILED and delete it.
     */
    private void handleRetryableExhausted(List<InFlight> batch) {
        for (InFlight entry : batch) {
            int receiveCount = parseReceiveCount(entry.sqsMessage());
            if (receiveCount >= maxPushRetryReceives) {
                log.error("Message reached max SQS retry receives ({}) — firing FAILED: "
                    + "correlationId={} pushToken={}",
                    maxPushRetryReceives,
                    sanitize(entry.decrypted().correlationId()),
                    sanitize(entry.decrypted().pushToken()));
                notifyHandler(result(NotificationOutcome.FAILED, entry.decrypted(), null,
                    "Exceeded maximum SQS retry receive count (" + maxPushRetryReceives + ")"));
                deleteMessage(pushQueueUrl, entry.sqsMessage().receiptHandle());
            }
            // else: leave in queue for SQS to redeliver after visibility timeout
        }
    }

    /**
     * Retries each message individually after a batch-level failure. This isolates a single
     * bad message so the remaining messages in the batch can succeed independently.
     */
    private void retryIndividually(List<InFlight> batch) throws InterruptedException {
        for (InFlight entry : batch) {
            if (Thread.currentThread().isInterrupted()) throw new InterruptedException();

            rateLimiter.acquire();

            try {
                PushTicketResponse ticketResponse = expoGateway.sendNotifications(
                    List.of(entry.expoMessage()));
                dispatchTickets(List.of(entry), ticketResponse);

            } catch (ExpoAuthException e) {
                log.error("CRITICAL: Expo auth failure during individual retry — halting consumer: {}",
                    e.getMessage());
                haltConsumer();
                return;

            } catch (ExpoRateLimitException | ExpoServerException e) {
                // Retryable exhausted for this individual message — apply same receive-count logic
                int receiveCount = parseReceiveCount(entry.sqsMessage());
                if (receiveCount >= maxPushRetryReceives) {
                    log.error("Individual message reached max SQS retry receives ({}) — firing FAILED: "
                        + "correlationId={}", maxPushRetryReceives, sanitize(entry.decrypted().correlationId()));
                    notifyHandler(result(NotificationOutcome.FAILED, entry.decrypted(), null,
                        "Exceeded maximum SQS retry receive count (" + maxPushRetryReceives + ")"));
                    deleteMessage(pushQueueUrl, entry.sqsMessage().receiptHandle());
                }
                // else: leave in queue

            } catch (Exception e) {
                // Non-retryable for this individual message
                log.error("Individual send failed with non-retryable error: correlationId={} error={}",
                    sanitize(entry.decrypted().correlationId()), e.getMessage());
                notifyHandler(result(NotificationOutcome.FAILED, entry.decrypted(), null, e.getMessage()));
                deleteMessage(pushQueueUrl, entry.sqsMessage().receiptHandle());
            }
        }
    }

    private void dispatchTickets(List<InFlight> batch, PushTicketResponse response) {
        List<PushError> batchErrors = (response != null && response.getErrors() != null)
            ? response.getErrors() : Collections.emptyList();

        if (!batchErrors.isEmpty()) {
            handleBatchErrors(batch, batchErrors);
            return;
        }

        List<PushTicket> tickets = (response != null && response.getData() != null)
            ? response.getData() : Collections.emptyList();

        for (int i = 0; i < batch.size(); i++) {
            PushTicket ticket = i < tickets.size() ? tickets.get(i) : null;
            processTicketResult(batch.get(i), ticket);
        }
    }

    private void handleBatchErrors(List<InFlight> batch, List<PushError> errors) {
        String detail = errors.stream()
            .map(e -> e.getCode() + ": " + e.getMessage())
            .reduce((a, b) -> a + "; " + b).orElse("unknown");
        log.error("Expo batch-level errors — treating as INVALID for {} message(s): {}",
            batch.size(), detail);
        for (InFlight entry : batch) {
            notifyHandler(result(NotificationOutcome.INVALID, entry.decrypted(), null, detail));
            deleteMessage(pushQueueUrl, entry.sqsMessage().receiptHandle());
        }
    }

    private void processTicketResult(InFlight entry, PushTicket ticket) {
        if (ticket == null) {
            // Expo returned 200 but no ticket for this message — the response is malformed or
            // truncated, so the batch may or may not have been processed. UNKNOWN (not INVALID):
            // the token is not known to be bad, and retrying risks a duplicate push.
            log.error("No ticket in Expo response for correlationId={} — firing UNKNOWN",
                sanitize(entry.decrypted().correlationId()));
            notifyHandler(result(NotificationOutcome.UNKNOWN, entry.decrypted(), null,
                "No ticket in Expo response — delivery state unknown"));
            deleteMessage(pushQueueUrl, entry.sqsMessage().receiptHandle());
        } else if (ticket.getStatus() == PushTicket.StatusEnum.OK) {
            handleOkTicket(entry, ticket.getId());
        } else {
            handleErrorTicket(entry, ticket);
        }
    }

    private void handleOkTicket(InFlight entry, String ticketId) {
        // The receipt-queue message is built from the RAW payload so title/body/metadata stay
        // encrypted at rest on the receipt queue.
        if (postToReceiptQueue(entry.raw(), ticketId)) {
            deleteMessage(pushQueueUrl, entry.sqsMessage().receiptHandle());
        } else {
            notifyHandler(result(NotificationOutcome.UNKNOWN, entry.decrypted(), ticketId, null));
            deleteMessage(pushQueueUrl, entry.sqsMessage().receiptHandle());
        }
    }

    private void handleErrorTicket(InFlight entry, PushTicket ticket) {
        String errorDetail = extractTicketError(ticket);
        NotificationOutcome outcome = "DeviceNotRegistered".equals(errorDetail)
            ? NotificationOutcome.REJECTED : NotificationOutcome.INVALID;
        notifyHandler(result(outcome, entry.decrypted(), null, errorDetail));
        deleteMessage(pushQueueUrl, entry.sqsMessage().receiptHandle());
    }

    private String extractTicketError(PushTicket ticket) {
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
            log.error("Receipt follow-up could not be queued after retries; marking UNKNOWN. "
                    + "ticketId={} correlationId={} handlerId={} pushToken={} receiptQueueUrl={}",
                sanitize(ticketId), sanitize(msg.correlationId()), sanitize(msg.handlerId()),
                sanitize(msg.pushToken()), receiptQueueUrl, e);
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

    /** Builds the Expo request from an already-decrypted message. */
    private PushMessage toExpoMessage(PushNotificationSqsMessage decrypted) {
        PushMessage pm = new PushMessage();
        pm.setTo(List.of(decrypted.pushToken()));
        pm.setTitle(decrypted.title());
        pm.setBody(decrypted.body());
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
