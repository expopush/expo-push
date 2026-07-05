package dev.expopush.backend.sqs.consumer;

import dev.expopush.api.NotificationHandlerRegistry;
import dev.expopush.api.NotificationOutcome;
import dev.expopush.backend.sqs.message.PushNotificationSqsMessage;
import dev.expopush.backend.sqs.message.PushReceiptSqsMessage;
import dev.expopush.backend.sqs.message.SqsNotificationMessage;
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
import software.amazon.awssdk.services.sqs.model.BatchResultErrorEntry;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResponse;
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
        long authFailureBackoffMs,
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
        super(sqsClient, registry, "sqs-push-consumer", config.drainTimeoutMs(), config.authFailureBackoffMs());
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
            // No amount of per-message retrying fixes bad credentials. Back off and resume:
            // a fixed access token takes effect without a restart.
            criticalBackoff("Expo authentication failure (" + e.getMessage() + ") — verify expo.push.access-token");

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
     * Deserializes, validates, and decrypts one SQS message. Returns null when the message
     * was resolved here instead of joining the batch:
     * <ul>
     *   <li>Unparseable JSON, an unknown (newer) schema version, or an unregistered handler
     *       → {@link #resolveUnprocessable}: left in the queue for redelivery/DLQ, deleted
     *       only past the receive ceiling. All three are deploy-mismatch conditions that a
     *       redeploy can fix — deleting them on first sight would destroy the in-flight
     *       queue during a bad rollout. Crucially this happens BEFORE the Expo call, so
     *       redelivery cannot duplicate a push.
     *   <li>Undecryptable payload (rotated key, corrupt ciphertext) → fires FAILED and
     *       deletes: enough plaintext fields survive to resolve it terminally, which beats
     *       parking it in a DLQ.
     * </ul>
     */
    private InFlight parseAndDecrypt(Message m) {
        PushNotificationSqsMessage raw;
        try {
            raw = objectMapper.readValue(m.body(), PushNotificationSqsMessage.class);
        } catch (Exception e) {
            resolveUnprocessable(pushQueueUrl, m, maxPushRetryReceives,
                "PNQ message failed to deserialize: " + e.getMessage());
            return null;
        }
        if (raw.schemaVersion() > SqsNotificationMessage.CURRENT_SCHEMA_VERSION) {
            resolveUnprocessable(pushQueueUrl, m, maxPushRetryReceives,
                "PNQ message has schema version " + raw.schemaVersion() + " but this node only "
                    + "understands <= " + SqsNotificationMessage.CURRENT_SCHEMA_VERSION
                    + " — is a newer producer deployed alongside an older consumer?");
            return null;
        }
        if (registry.getHandler(raw.handlerId()) == null) {
            resolveUnprocessable(pushQueueUrl, m, maxPushRetryReceives,
                "No NotificationResultHandler registered for handlerId=" + sanitize(raw.handlerId())
                    + " (correlationId=" + sanitize(raw.correlationId()) + ") — redeploy with the "
                    + "handler restored to recover this message");
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
                // Remaining messages stay in the queue and are redelivered after the backoff.
                criticalBackoff("Expo authentication failure during individual retry ("
                    + e.getMessage() + ") — verify expo.push.access-token");
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

    /** An OK-ticket entry awaiting its receipt-queue follow-up message. */
    private record ReceiptDispatch(InFlight entry, String ticketId) {}

    /**
     * Routes each ticket to its outcome, then flushes the receipt-queue sends and the
     * push-queue deletes as single batch calls (instead of one round trip per message).
     * Sends run before deletes so a crash between the two only causes redelivery — the
     * documented at-least-once behavior — never a lost receipt follow-up.
     */
    private void dispatchTickets(List<InFlight> batch, PushTicketResponse response) {
        List<PushError> batchErrors = (response != null && response.getErrors() != null)
            ? response.getErrors() : Collections.emptyList();
        List<Message> toDelete = new ArrayList<>(batch.size());

        if (!batchErrors.isEmpty()) {
            String detail = batchErrors.stream()
                .map(e -> e.getCode() + ": " + e.getMessage())
                .reduce((a, b) -> a + "; " + b).orElse("unknown");
            log.error("Expo batch-level errors — treating as INVALID for {} message(s): {}",
                batch.size(), detail);
            for (InFlight entry : batch) {
                notifyHandler(result(NotificationOutcome.INVALID, entry.decrypted(), null, detail));
                toDelete.add(entry.sqsMessage());
            }
            deleteMessageBatch(pushQueueUrl, toDelete);
            return;
        }

        List<PushTicket> tickets = (response != null && response.getData() != null)
            ? response.getData() : Collections.emptyList();
        List<ReceiptDispatch> receiptDispatches = new ArrayList<>(batch.size());

        for (int i = 0; i < batch.size(); i++) {
            InFlight entry = batch.get(i);
            PushTicket ticket = i < tickets.size() ? tickets.get(i) : null;
            if (ticket == null) {
                // Expo returned 200 but no ticket for this message — the response is malformed or
                // truncated, so the batch may or may not have been processed. UNKNOWN (not INVALID):
                // the token is not known to be bad, and retrying risks a duplicate push.
                log.error("No ticket in Expo response for correlationId={} — firing UNKNOWN",
                    sanitize(entry.decrypted().correlationId()));
                notifyHandler(result(NotificationOutcome.UNKNOWN, entry.decrypted(), null,
                    "No ticket in Expo response — delivery state unknown"));
            } else if (ticket.getStatus() == PushTicket.StatusEnum.OK) {
                receiptDispatches.add(new ReceiptDispatch(entry, ticket.getId()));
            } else {
                String errorDetail = extractTicketError(ticket);
                NotificationOutcome outcome = "DeviceNotRegistered".equals(errorDetail)
                    ? NotificationOutcome.REJECTED : NotificationOutcome.INVALID;
                notifyHandler(result(outcome, entry.decrypted(), null, errorDetail));
            }
            toDelete.add(entry.sqsMessage());
        }

        sendReceiptBatch(receiptDispatches);
        deleteMessageBatch(pushQueueUrl, toDelete);
    }

    /**
     * Posts all receipt follow-ups in one {@code SendMessageBatch} call. Entries that fail
     * within an otherwise-successful batch call — and the whole batch if the call itself
     * fails after retries — fall back to the individual send path (which has its own retry
     * budget); entries that still fail there are resolved as UNKNOWN, matching the previous
     * per-message behavior. Receipt messages are built from the RAW payload so
     * title/body/metadata stay encrypted at rest on the receipt queue.
     */
    private void sendReceiptBatch(List<ReceiptDispatch> dispatches) {
        if (dispatches.isEmpty()) return;

        List<SendMessageBatchRequestEntry> entries = new ArrayList<>(dispatches.size());
        for (int i = 0; i < dispatches.size(); i++) {
            ReceiptDispatch d = dispatches.get(i);
            entries.add(SendMessageBatchRequestEntry.builder()
                .id(Integer.toString(i))
                .messageBody(receiptMessageBody(d.entry().raw(), d.ticketId()))
                .delaySeconds(receiptDelaySeconds)
                .build());
        }

        List<ReceiptDispatch> fallback = new ArrayList<>();
        try {
            SendMessageBatchResponse response = sqsRetry.executeSupplier(() ->
                sqsClient.sendMessageBatch(SendMessageBatchRequest.builder()
                    .queueUrl(receiptQueueUrl)
                    .entries(entries)
                    .build()));
            if (response != null) {
                for (BatchResultErrorEntry failed : response.failed()) {
                    fallback.add(dispatches.get(Integer.parseInt(failed.id())));
                }
            } else {
                fallback.addAll(dispatches);
            }
        } catch (Exception e) {
            log.warn("Receipt batch send failed after retries ({}) — falling back to individual sends",
                e.getMessage());
            fallback.addAll(dispatches);
        }

        for (ReceiptDispatch d : fallback) {
            if (!postToReceiptQueue(d.entry().raw(), d.ticketId())) {
                notifyHandler(result(NotificationOutcome.UNKNOWN, d.entry().decrypted(), d.ticketId(), null));
            }
        }
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
        sqsClient.sendMessage(SendMessageRequest.builder()
            .queueUrl(receiptQueueUrl)
            .messageBody(receiptMessageBody(msg, ticketId))
            .delaySeconds(receiptDelaySeconds)
            .build());
        log.debug("Posted receipt message for ticketId={} with {}s delay", sanitize(ticketId), receiptDelaySeconds);
    }

    private String receiptMessageBody(PushNotificationSqsMessage msg, String ticketId) {
        try {
            PushReceiptSqsMessage receiptMsg = new PushReceiptSqsMessage(
                ticketId, msg.pushToken(), msg.correlationId(),
                msg.metadata(), msg.handlerId(), msg.title(), msg.body());
            return objectMapper.writeValueAsString(receiptMsg);
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
