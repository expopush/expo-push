package dev.expopush.backend.h2;

import dev.expopush.api.NotificationCommand;
import dev.expopush.api.NotificationHandlerRegistry;
import dev.expopush.api.NotificationOutcome;
import dev.expopush.api.NotificationResult;
import dev.expopush.api.NotificationResultHandler;
import dev.expopush.core.ExpoGateway;
import dev.expopush.core.api.model.PushReceipt;
import dev.expopush.core.api.model.PushReceiptDetails;
import dev.expopush.core.api.model.PushReceiptResponse;
import dev.expopush.core.api.model.PushTicket;
import dev.expopush.core.api.model.PushTicketResponse;
import dev.expopush.core.security.NoOpPayloadEncryptor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import tools.jackson.databind.ObjectMapper;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class H2BackendIntegrationTest {

    @Mock private ExpoGateway expoGateway;
    @Mock private NotificationHandlerRegistry registry;
    @Mock private NotificationResultHandler resultHandler;
    private final ObjectMapper objectMapper = new ObjectMapper();

    private H2NotificationBackend backend;
    private H2ReceiptOrchestrator orchestrator;
    private JdbcTemplate jdbcTemplate;

    private static final NotificationCommand CMD = new NotificationCommand(
        "ExponentPushToken[test]", "Title", "Body", "corr-1", Map.of(), "h-1");

    @BeforeEach
    void setUp() throws Exception {
        Path dbPath = Files.createTempDirectory("expo-h2-test");
        String url = "jdbc:h2:" + dbPath.resolve("testdb").toAbsolutePath() + ";DB_CLOSE_DELAY=-1";

        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("org.h2.Driver");
        dataSource.setUrl(url);
        dataSource.setUsername("sa");
        dataSource.setPassword("");

        jdbcTemplate = new JdbcTemplate(dataSource);

        var encryptor = new NoOpPayloadEncryptor();

        orchestrator = new H2ReceiptOrchestrator(jdbcTemplate, expoGateway, registry, objectMapper, encryptor, 3, 1);
        orchestrator.start();

        backend = new H2NotificationBackend(expoGateway, jdbcTemplate, registry, objectMapper, encryptor, Runnable::run, 1);

        lenient().when(registry.getHandler(anyString())).thenReturn(resultHandler);
    }

    @AfterEach
    void tearDown() {
        orchestrator.stop();
        jdbcTemplate.execute("SHUTDOWN IMMEDIATELY");
    }

    // ─── End-to-end happy path ────────────────────────────────────────────────

    @Test
    @Timeout(10)
    void persistentEndToEndFlow() {
        PushTicket ticket = new PushTicket();
        ticket.setStatus(PushTicket.StatusEnum.OK);
        ticket.setId("h2-ticket");
        PushTicketResponse ticketResponse = new PushTicketResponse();
        ticketResponse.setData(List.of(ticket));

        when(expoGateway.sendNotifications(anyList())).thenReturn(ticketResponse);

        PushReceipt receipt = new PushReceipt();
        receipt.setStatus(PushReceipt.StatusEnum.OK);
        PushReceiptResponse receiptResponse = new PushReceiptResponse();
        receiptResponse.setData(Map.of("h2-ticket", receipt));

        when(expoGateway.getReceipts(List.of("h2-ticket"))).thenReturn(receiptResponse);

        backend.submit(CMD);

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
            verify(resultHandler).handleResult(argThat(res ->
                res.outcome() == NotificationOutcome.ACCEPTED && "h2-ticket".equals(res.ticketId())))
        );
    }

    // ─── Receipt error outcomes ───────────────────────────────────────────────

    @Test
    @Timeout(10)
    void receiptDeviceNotRegisteredFiresRejected() throws Exception {
        insertReadyRow("corr-1", "ticket-1", CMD, 1);

        PushReceiptResponse response = receiptResponse("ticket-1", PushReceipt.StatusEnum.ERROR, "DeviceNotRegistered");
        when(expoGateway.getReceipts(anyList())).thenReturn(response);

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            ArgumentCaptor<NotificationResult> cap = ArgumentCaptor.forClass(NotificationResult.class);
            verify(resultHandler).handleResult(cap.capture());
            assertThat(cap.getValue().outcome()).isEqualTo(NotificationOutcome.REJECTED);
        });
    }

    @Test
    @Timeout(10)
    void receiptOtherErrorFiresInvalid() throws Exception {
        insertReadyRow("corr-1", "ticket-1", CMD, 1);

        PushReceiptResponse response = receiptResponse("ticket-1", PushReceipt.StatusEnum.ERROR, "MessageTooBig");
        when(expoGateway.getReceipts(anyList())).thenReturn(response);

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            ArgumentCaptor<NotificationResult> cap = ArgumentCaptor.forClass(NotificationResult.class);
            verify(resultHandler).handleResult(cap.capture());
            assertThat(cap.getValue().outcome()).isEqualTo(NotificationOutcome.INVALID);
        });
    }

    // ─── Max-attempt exhaustion ───────────────────────────────────────────────

    @Test
    @Timeout(10)
    void receiptNotFoundAtMaxAttemptsFiresUnknown() throws Exception {
        insertReadyRow("corr-1", "ticket-1", CMD, 3); // attempt = maxAttempts=3
        when(expoGateway.getReceipts(anyList())).thenReturn(new PushReceiptResponse()); // empty

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            ArgumentCaptor<NotificationResult> cap = ArgumentCaptor.forClass(NotificationResult.class);
            verify(resultHandler).handleResult(cap.capture());
            assertThat(cap.getValue().outcome()).isEqualTo(NotificationOutcome.UNKNOWN);
        });
    }

    @Test
    @Timeout(10)
    void expoExceptionAtMaxAttemptsFiresUnknown() throws Exception {
        insertReadyRow("corr-1", "ticket-1", CMD, 3); // at max attempts
        when(expoGateway.getReceipts(anyList())).thenThrow(new RuntimeException("Expo down"));

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            ArgumentCaptor<NotificationResult> cap = ArgumentCaptor.forClass(NotificationResult.class);
            verify(resultHandler).handleResult(cap.capture());
            assertThat(cap.getValue().outcome()).isEqualTo(NotificationOutcome.UNKNOWN);
        });
    }

    // ─── Stale PENDING row cleanup on startup ─────────────────────────────────

    @Test
    @Timeout(10)
    void stalePendingRowsFireUnknownOnStartup() throws Exception {
        // Use a command whose correlationId matches the DB row key
        NotificationCommand staleCmd = new NotificationCommand(
            "ExponentPushToken[stale]", "Title", "Body", "corr-stale", Map.of(), "h-1");
        insertPendingRow("corr-stale", staleCmd);

        // Stop running orchestrator and restart a new one to trigger cleanup
        orchestrator.stop();
        orchestrator = new H2ReceiptOrchestrator(
            jdbcTemplate, expoGateway, registry, objectMapper, new NoOpPayloadEncryptor(), 3, 30);
        orchestrator.start(); // triggers cleanupStalePendingRows() synchronously

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            ArgumentCaptor<NotificationResult> cap = ArgumentCaptor.forClass(NotificationResult.class);
            verify(resultHandler).handleResult(cap.capture());
            assertThat(cap.getValue().outcome()).isEqualTo(NotificationOutcome.UNKNOWN);
            assertThat(cap.getValue().correlationId()).isEqualTo("corr-stale");
        });
    }

    // ─── Legacy schema migration ──────────────────────────────────────────────

    @Test
    @Timeout(10)
    void incompatibleLegacyTableIsRenamedAsideOnStartup() {
        // Simulate a database file created by a pre-RC2 version: pending_receipts exists
        // but has no surrogate-key id column. Startup must rename it and create the new
        // schema instead of wedging on CREATE INDEX.
        orchestrator.stop();
        jdbcTemplate.execute("DROP TABLE pending_receipts");
        jdbcTemplate.execute("""
            CREATE TABLE pending_receipts (
                correlation_id VARCHAR(255) PRIMARY KEY,
                ticket_id      VARCHAR(255),
                command_json   CLOB NOT NULL,
                attempt        INT NOT NULL,
                check_after    TIMESTAMP NOT NULL,
                state          VARCHAR(10) NOT NULL
            )
        """);
        jdbcTemplate.update(
            "INSERT INTO pending_receipts (correlation_id, ticket_id, command_json, attempt, check_after, state) "
                + "VALUES ('legacy-corr', 'legacy-ticket', '{}', 1, CURRENT_TIMESTAMP, 'READY')");

        orchestrator = new H2ReceiptOrchestrator(
            jdbcTemplate, expoGateway, registry, objectMapper, new NoOpPayloadEncryptor(), 3, 30);
        orchestrator.start(); // must not throw

        Integer idColumns = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS "
                + "WHERE TABLE_NAME = 'PENDING_RECEIPTS' AND COLUMN_NAME = 'ID'",
            Integer.class);
        assertThat(idColumns).isEqualTo(1);

        Integer legacyTables = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES "
                + "WHERE TABLE_NAME LIKE 'PENDING_RECEIPTS_LEGACY_%'",
            Integer.class);
        assertThat(legacyTables).isEqualTo(1);
    }

    // ─── Retry below max: row stays, count increments ─────────────────────────

    @Test
    @Timeout(10)
    void receiptNotFoundBelowMaxIncreasesAttemptCount() throws Exception {
        insertReadyRow("corr-1", "ticket-1", CMD, 1); // below maxAttempts=3
        when(expoGateway.getReceipts(anyList())).thenReturn(new PushReceiptResponse()); // no receipt

        // Wait for attempt increment in DB (row updated, not deleted)
        await().atMost(5, TimeUnit.SECONDS).until(() -> {
            Integer attempt = jdbcTemplate.queryForObject(
                "SELECT attempt FROM pending_receipts WHERE correlation_id = 'corr-1'",
                Integer.class);
            return attempt != null && attempt >= 2;
        });
    }

    // ─── Helpers ─────────────────────────────────────────────────────────────

    private void insertReadyRow(String correlationId, String ticketId, NotificationCommand cmd, int attempt) throws Exception {
        String json = objectMapper.writeValueAsString(cmd);
        jdbcTemplate.update(
            "INSERT INTO pending_receipts (id, correlation_id, ticket_id, command_json, attempt, check_after, state) VALUES (?, ?, ?, ?, ?, ?, ?)",
            java.util.UUID.randomUUID().toString(), correlationId, ticketId, json, attempt, Instant.now().minusSeconds(10), "READY"
        );
    }

    private void insertPendingRow(String correlationId, NotificationCommand cmd) throws Exception {
        String json = objectMapper.writeValueAsString(cmd);
        jdbcTemplate.update(
            "INSERT INTO pending_receipts (id, correlation_id, ticket_id, command_json, attempt, check_after, state) VALUES (?, ?, ?, ?, ?, ?, ?)",
            java.util.UUID.randomUUID().toString(), correlationId, null, json, 1, Instant.now(), "PENDING"
        );
    }

    private static PushReceiptResponse receiptResponse(String ticketId, PushReceipt.StatusEnum status, String errorCode) {
        PushReceipt receipt = new PushReceipt();
        receipt.setStatus(status);
        if (errorCode != null) {
            PushReceiptDetails details = new PushReceiptDetails();
            details.setError(errorCode);
            receipt.setDetails(details);
        }
        PushReceiptResponse response = new PushReceiptResponse();
        response.setData(Map.of(ticketId, receipt));
        return response;
    }
}
