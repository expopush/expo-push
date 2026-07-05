package dev.expopush.autoconfigure;

import feign.Logger;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Configuration properties for the Expo Push Notification starter.
 */
@ConfigurationProperties(prefix = "expo.push")
@Data
public class ExpoPushProperties {

    /** Expo Push API base URL. Must use https in production. */
    private String apiUrl = "https://exp.host/--/api/v2";

    /**
     * Allow non-https API URLs. Must only be set to {@code true} in local development
     * (e.g. pointing at a local test stub). Setting this in production exposes the
     * bearer token in plaintext.
     */
    private boolean allowInsecureApiUrl = false;

    /** Expo access token. Required. */
    private String accessToken;

    /**
     * Feign log level for Expo API calls. Defaults to NONE to prevent bearer token
     * and notification body from appearing in logs. Set to BASIC for URL/status
     * logging, or FULL only in non-production environments.
     */
    private Logger.Level logLevel = Logger.Level.NONE;

    /** TCP connect timeout for Expo API calls. */
    private Duration connectTimeout = Duration.ofSeconds(10);

    /**
     * Read timeout for Expo API calls. Bounds how long a consumer thread can hang on an
     * unresponsive Expo endpoint per attempt.
     */
    private Duration readTimeout = Duration.ofSeconds(30);

    /**
     * How long to wait for SQS consumer threads to finish their current batch during
     * shutdown before the drain waiter fires the stop callback anyway. Should be aligned
     * with {@code spring.lifecycle.timeout-per-shutdown-phase}.
     */
    private java.time.Duration shutdownTimeout = java.time.Duration.ofSeconds(30);

    /**
     * Backend implementation to activate. Must be explicitly set.
     * Supported values: {@code sqs}, {@code local}, {@code h2}.
     */
    private String backend;

    private Batch batch = new Batch();
    private RateLimit rateLimit = new RateLimit();
    private Security security = new Security();
    private Sqs sqs = new Sqs();
    private Local local = new Local();
    private H2 h2 = new H2();

    @Data
    public static class Batch {
        /** Maximum push messages per Expo API request (Expo hard-limit: 100). */
        private int maxSize = 100;
        /** Total attempts (1 initial + N-1 retries) for retryable Expo errors. */
        private int maxRetryAttempts = 3;
        /** Initial backoff before the first retry; doubles on each subsequent attempt. */
        private Duration retryBackoff = Duration.ofSeconds(1);
    }

    @Data
    public static class Security {
        /**
         * Mask sensitive data (title, body, metadata) in logs using LogMasker.
         * Enabled by default to prevent PII leakage.
         */
        private boolean maskSensitiveDataInLogs = true;

        /**
         * Encrypt sensitive notification payloads (title, body, metadata) before storage 
         * in persistent backends (SQS or H2). 
         * Requires {@code encryption-key} to be set if true.
         */
        private boolean encryptPayloads = true;

        /**
         * 256-bit AES encryption key in Base64 format. 
         * Required if {@code encrypt-payloads} is true.
         */
        private String encryptionKey;
    }

    @Data
    public static class RateLimit {
        /** Maximum Expo send API calls per second on this node. */
        private int sendPermitsPerSecond = 45;
        /** Maximum Expo receipt API calls per second on this node. */
        private int receiptPermitsPerSecond = 45;
        /** How long to wait for a rate-limit permit before throwing. */
        private Duration timeout = Duration.ofSeconds(5);
    }

    @Data
    public static class Sqs {
        /** SQS queue name for outbound push notifications. */
        private String pushQueueName = "expo-push-notifications";
        /** SQS queue name for push receipt verification. */
        private String receiptQueueName = "expo-push-receipts";
        /** AWS region. */
        private String region = "us-east-1";

        /**
         * Whether to start the SQS consumer threads for polling push notifications and receipts.
         * Default is true. Set to false to disable polling (useful for tests or send-only nodes).
         */
        private boolean consumersEnabled = true;

        /**
         * Optional endpoint override (e.g. LocalStack URL, VPC endpoint, FIPS endpoint).
         * Leave blank for standard AWS SQS.
         */
        private String endpointOverride;

        /**
         * When true, authenticates with static {@code "test"/"test"} credentials instead of
         * the default AWS credential chain. Only intended for local development against
         * LocalStack or similar. Never set this in production.
         */
        private boolean useStaticCredentials = false;
        /**
         * Seconds to delay a receipt-verification message after its ticket is received.
         * Expo typically processes receipts within 15 minutes.
         * SQS standard-queue delay ceiling is 900 seconds.
         */
        private int receiptDelaySeconds = 900;
        /** Maximum SQS receive attempts before a receipt message is treated as timed out. */
        private int maxReceiptAttempts = 3;
        /**
         * Maximum number of full SQS redelivery cycles for a push notification message before
         * it is marked FAILED and deleted. Each cycle represents one Resilience4j retry sequence
         * (up to maxRetryAttempts attempts) against Expo. Only applies to retryable failures
         * (Expo down, rate-limited); non-retryable errors fire FAILED immediately.
         */
        private int maxPushRetryReceives = 5;
        /** Total attempts to enqueue a follow-up receipt request after Expo issues a ticket. */
        private int receiptPublishMaxAttempts = 3;
        /** Initial backoff before retrying a recoverable receipt-queue publish failure. */
        private Duration receiptPublishRetryBackoff = Duration.ofMillis(250);
        /**
         * Visibility timeout (seconds) applied to a push-queue batch when the consumer starts
         * processing it. Must exceed the worst-case processing time of one batch — roughly
         * {@code rate-limit.timeout + maxRetryAttempts × retryBackoff}, per message when a
         * batch falls back to individual retries. Prevents SQS from redelivering (and thus
         * duplicating) messages that are still in flight. Set to 0 to disable and rely on
         * the queue's own visibility timeout.
         */
        private int inFlightVisibilitySeconds = 300;
    }

    @Data
    public static class Local {
        /** Delay in seconds before checking the first receipt. */
        private int receiptDelaySeconds = 600;
        /** Maximum attempts to check for a receipt before giving up. */
        private int maxReceiptAttempts = 3;
        /** Maximum number of tasks allowed in the local queue. */
        private int maxQueueSize = 10000;
    }

    @Data
    public static class H2 {
        /** File path for the H2 database (e.g. ./expo-push). Must not contain ';', '=', or ':'. */
        private String filePath = "./expo-push-db";
        /** Delay in seconds before checking the first receipt. */
        private int receiptDelaySeconds = 600;
        /** Maximum attempts to check for a receipt before giving up. */
        private int maxReceiptAttempts = 5;
        /** H2 JDBC username. */
        private String username = "sa";
        /** H2 JDBC password. */
        private String password = "";
    }
}
