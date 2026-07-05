package dev.expopush.backend.h2;

import dev.expopush.api.NotificationCommand;
import dev.expopush.api.NotificationHandlerRegistry;
import dev.expopush.api.NotificationOutcome;
import dev.expopush.api.NotificationResult;
import dev.expopush.api.NotificationResultHandler;
import dev.expopush.core.ExpoGateway;
import dev.expopush.core.api.model.PushReceipt;
import dev.expopush.core.api.model.PushReceiptResponse;
import dev.expopush.core.security.PayloadEncryptor;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Persistently orchestrates receipt checking by polling an H2 database.
 */
@Slf4j
public class H2ReceiptOrchestrator {

    private static final String CORRELATION_ID_COL = "correlation_id";

    private final JdbcTemplate jdbcTemplate;
    private final ExpoGateway expoGateway;
    private final NotificationHandlerRegistry registry;
    private final ObjectMapper objectMapper;
    private final PayloadEncryptor encryptor;
    private final int maxAttempts;
    private final long retryDelaySeconds;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private Thread workerThread;

    public H2ReceiptOrchestrator(
            JdbcTemplate jdbcTemplate,
            ExpoGateway expoGateway,
            NotificationHandlerRegistry registry,
            ObjectMapper objectMapper,
            PayloadEncryptor encryptor,
            int maxAttempts,
            long retryDelaySeconds) {
        this.jdbcTemplate = jdbcTemplate;
        this.expoGateway = expoGateway;
        this.registry = registry;
        this.objectMapper = objectMapper;
        this.encryptor = encryptor;
        this.maxAttempts = maxAttempts;
        this.retryDelaySeconds = retryDelaySeconds;
    }

    @PostConstruct
    public void start() {
        initSchema();
        cleanupStalePendingRows();
        workerThread = Thread.ofVirtual().name("expo-h2-receipt-orchestrator").start(this::pollLoop);
        log.info("H2 Receipt Orchestrator started using virtual threads");
    }

    /**
     * On startup, rows left in PENDING state were written before an Expo call that never
     * completed (JVM crash / kill between the outbox INSERT and the ticket UPDATE). The crash
     * may have happened before the send, mid-send, or after Expo accepted but before the row
     * was activated — we cannot distinguish these cases, so we fire UNKNOWN (which carries the
     * duplicate-on-retry warning) and remove the row.
     */
    private void cleanupStalePendingRows() {
        List<PendingTask> stale = jdbcTemplate.query(
            "SELECT id, correlation_id, ticket_id, command_json, attempt FROM pending_receipts WHERE state = 'PENDING'",
            (rs, rowNum) -> {
                try {
                    String decryptedJson = encryptor.decrypt(rs.getString("command_json"));
                    NotificationCommand cmd = objectMapper.readValue(decryptedJson, NotificationCommand.class);
                    return new PendingTask(rs.getString("id"), rs.getString(CORRELATION_ID_COL),
                        rs.getString("ticket_id"), cmd, rs.getInt("attempt"));
                } catch (Exception e) {
                    log.error("Failed to deserialise stale PENDING row: correlationId={}",
                        sanitize(rs.getString(CORRELATION_ID_COL)), e);
                    return null;
                }
            }
        ).stream().filter(java.util.Objects::nonNull).toList();

        for (PendingTask task : stale) {
            log.warn("Removing stale PENDING outbox row from previous run — firing UNKNOWN: correlationId={}",
                sanitize(task.correlationId()));
            notifyHandler(new NotificationResult(
                NotificationOutcome.UNKNOWN,
                task.command().handlerId(),
                task.command().correlationId(),
                task.command().pushToken(),
                task.command().title(),
                task.command().body(),
                null,
                "Interrupted mid-submission by a previous shutdown — Expo may or may not have "
                    + "accepted this notification; re-submitting risks a duplicate push",
                task.command().metadata()
            ));
            jdbcTemplate.update("DELETE FROM pending_receipts WHERE id = ?", task.id());
        }

        if (!stale.isEmpty()) {
            log.warn("Cleaned up {} stale PENDING outbox row(s) on startup", stale.size());
        }
    }

    private void initSchema() {
        renameIncompatibleLegacyTable();
        // id is a starter-generated surrogate key; correlation_id is caller-assigned, opaque,
        // and deliberately NOT unique — two in-flight submissions may share one.
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS pending_receipts (
                id             VARCHAR(36) PRIMARY KEY,
                correlation_id VARCHAR(255) NOT NULL,
                ticket_id      VARCHAR(255),
                command_json   CLOB NOT NULL,
                attempt        INT NOT NULL,
                check_after    TIMESTAMP NOT NULL,
                state          VARCHAR(10) NOT NULL
            )
        """);
        jdbcTemplate.execute(
            "CREATE INDEX IF NOT EXISTS idx_state_check_after ON pending_receipts(state, check_after)");
        jdbcTemplate.execute(
            "CREATE INDEX IF NOT EXISTS idx_ticket_id ON pending_receipts(ticket_id)");
    }

    /**
     * A database file created by a pre-RC2 version has a pending_receipts table without the
     * {@code id} surrogate-key column. {@code CREATE TABLE IF NOT EXISTS} would silently keep
     * that incompatible shape and every later statement would fail, wedging startup. Rename
     * the legacy table aside (preserving its data for manual inspection) and let a fresh
     * table be created.
     */
    private void renameIncompatibleLegacyTable() {
        Integer tables = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'PENDING_RECEIPTS'",
            Integer.class);
        if (tables == null || tables == 0) return;

        Integer idColumns = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS "
                + "WHERE TABLE_NAME = 'PENDING_RECEIPTS' AND COLUMN_NAME = 'ID'",
            Integer.class);
        if (idColumns != null && idColumns > 0) return;

        String backupName = "pending_receipts_legacy_" + System.currentTimeMillis();
        log.warn("Existing pending_receipts table has an incompatible pre-RC2 schema — "
            + "renaming it to {} and creating a fresh table. Receipt checks pending in the "
            + "legacy table will NOT be resumed; inspect or drop it manually.", backupName);
        renameTable(backupName);
    }

    /**
     * DDL identifiers cannot be bound as parameters. {@code backupName} is built entirely
     * from a constant prefix and {@code System.currentTimeMillis()} — no external input —
     * so the concatenation is not injectable.
     */
    @SuppressWarnings("java:S2077")
    private void renameTable(String backupName) {
        jdbcTemplate.execute("ALTER TABLE pending_receipts RENAME TO " + backupName);
    }

    @PreDestroy
    public void stop() {
        running.set(false);
        if (workerThread != null) {
            workerThread.interrupt();
        }
    }

    private void pollLoop() {
        while (running.get() && !Thread.currentThread().isInterrupted()) {
            try {
                List<PendingTask> tasks = fetchDueTasks();
                if (tasks.isEmpty()) {
                    Thread.sleep(1000); // Poll every 1s when idle
                } else {
                    for (PendingTask task : tasks) {
                        processTask(task);
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                log.error("Error in H2 receipt poll loop", e);
                backOffQuietly(10_000);
            }
        }
    }

    private static void backOffQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    private List<PendingTask> fetchDueTasks() {
        return jdbcTemplate.query(
            "SELECT id, correlation_id, ticket_id, command_json, attempt FROM pending_receipts "
                + "WHERE state = 'READY' AND check_after <= CURRENT_TIMESTAMP LIMIT 10",
            (rs, rowNum) -> {
                try {
                    String decryptedJson = encryptor.decrypt(rs.getString("command_json"));
                    NotificationCommand cmd = objectMapper.readValue(decryptedJson, NotificationCommand.class);
                    return new PendingTask(rs.getString("id"), rs.getString(CORRELATION_ID_COL),
                        rs.getString("ticket_id"), cmd, rs.getInt("attempt"));
                } catch (Exception e) {
                    log.error("Failed to deserialise command for correlationId={}",
                        sanitize(rs.getString(CORRELATION_ID_COL)), e);
                    return null;
                }
            }
        ).stream().filter(java.util.Objects::nonNull).toList();
    }

    private void processTask(PendingTask task) {
        try {
            log.debug("H2 checking receipt for ticket {} (attempt {})", sanitize(task.ticketId()), task.attempt());
            PushReceiptResponse response = expoGateway.getReceipts(List.of(task.ticketId()));

            if (response == null || response.getData() == null || !response.getData().containsKey(task.ticketId())) {
                rescheduleOrMarkUnknown(task);
                return;
            }

            PushReceipt receipt = response.getData().get(task.ticketId());
            boolean accepted = receipt.getStatus() == PushReceipt.StatusEnum.OK;
            NotificationOutcome outcome = accepted ? NotificationOutcome.ACCEPTED : mapError(receipt);

            notifyHandler(new NotificationResult(
                outcome,
                task.command().handlerId(),
                task.command().correlationId(),
                task.command().pushToken(),
                task.command().title(),
                task.command().body(),
                task.ticketId(),
                accepted ? null : extractError(receipt),
                task.command().metadata()
            ));

            removeTask(task.id());

        } catch (Exception e) {
            log.warn("H2 failed to fetch receipt for ticket {}; will retry: {}", sanitize(task.ticketId()), e.getMessage());
            rescheduleOrMarkUnknown(task);
        }
    }

    private void rescheduleOrMarkUnknown(PendingTask task) {
        if (task.attempt() < maxAttempts) {
            jdbcTemplate.update(
                "UPDATE pending_receipts SET attempt = attempt + 1, check_after = ? WHERE id = ?",
                Instant.now().plusSeconds(retryDelaySeconds), task.id()
            );
        } else {
            log.warn("H2 max attempts reached for ticket {}; marking UNKNOWN", sanitize(task.ticketId()));
            notifyHandler(new NotificationResult(
                NotificationOutcome.UNKNOWN,
                task.command().handlerId(),
                task.command().correlationId(),
                task.command().pushToken(),
                task.command().title(),
                task.command().body(),
                task.ticketId(),
                "Receipt not found after persistence window",
                task.command().metadata()
            ));
            removeTask(task.id());
        }
    }

    private void removeTask(String rowId) {
        jdbcTemplate.update("DELETE FROM pending_receipts WHERE id = ?", rowId);
    }

    private void notifyHandler(NotificationResult result) {
        NotificationResultHandler handler = registry.getHandler(result.handlerId());
        if (handler != null) {
            try {
                handler.handleResult(result);
            } catch (Exception e) {
                log.error("H2 handler threw exception for result {}", result, e);
            }
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

    private record PendingTask(String id, String correlationId, String ticketId,
                               NotificationCommand command, int attempt) {}

    private static String sanitize(String value) {
        if (value == null) return "(null)";
        return value.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ');
    }
}
