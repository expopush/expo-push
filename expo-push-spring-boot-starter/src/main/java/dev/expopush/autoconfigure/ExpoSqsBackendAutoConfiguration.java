package dev.expopush.autoconfigure;

import dev.expopush.api.NotificationHandlerRegistry;
import dev.expopush.backend.sqs.SqsNotificationBackend;
import dev.expopush.backend.sqs.consumer.PushNotificationQueueConsumer;
import dev.expopush.backend.sqs.consumer.PushReceiptQueueConsumer;
import dev.expopush.core.ExpoGateway;
import dev.expopush.core.ratelimit.ExpoRateLimiter;
import dev.expopush.core.security.PayloadEncryptor;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;

/**
 * Auto-configuration for the SQS-backed notification backend.
 */
@Slf4j
@AutoConfiguration(after = ExpoPushAutoConfiguration.class)
@ConditionalOnClass(SqsClient.class)
@ConditionalOnProperty(prefix = "expo.push", name = "backend", havingValue = "sqs")
@EnableConfigurationProperties(ExpoPushProperties.class)
public class ExpoSqsBackendAutoConfiguration {

    // ─── SQS client ───────────────────────────────────────────────────────────

    @Bean
    @ConditionalOnMissingBean(SqsClient.class)
    public SqsClient sqsClient(ExpoPushProperties properties) {
        ExpoPushProperties.Sqs sqs = properties.getSqs();
        String override = sqs.getEndpointOverride();

        var builder = SqsClient.builder()
            .region(validatedRegion(sqs.getRegion()));

        if (override != null && !override.isBlank()) {
            builder.endpointOverride(URI.create(override));
        }

        if (sqs.isUseStaticCredentials()) {
            builder.credentialsProvider(StaticCredentialsProvider.create(
                AwsBasicCredentials.create("test", "test")));
        }

        return builder.build();
    }

    // ─── SQS retry ────────────────────────────────────────────────────────────

    @Bean(name = "sqsRetry")
    @ConditionalOnMissingBean(name = "sqsRetry")
    public Retry sqsRetry(ExpoPushProperties properties) {
        ExpoPushProperties.Sqs sqs = properties.getSqs();
        long baseBackoffMs = sqs.getReceiptPublishRetryBackoff().toMillis();

        RetryConfig config = RetryConfig.custom()
            .maxAttempts(sqs.getReceiptPublishMaxAttempts())
            .intervalFunction(IntervalFunction.ofExponentialRandomBackoff(baseBackoffMs, 2.0, 0.5))
            .retryOnException(t ->
                t instanceof software.amazon.awssdk.core.exception.SdkClientException
                    || (t instanceof software.amazon.awssdk.services.sqs.model.SqsException sqsEx
                    && (sqsEx.statusCode() == 429 || sqsEx.statusCode() >= 500)))
            .build();

        return RetryRegistry.of(config).retry("expo-sqs");
    }

    // ─── Backend and consumers ────────────────────────────────────────────────

    private static Region validatedRegion(String region) {
        if (region == null || region.isBlank()) {
            throw new IllegalArgumentException(
                "expo.push.sqs.region must not be blank");
        }
        Region r = Region.of(region);
        if (!Region.regions().contains(r)) {
            log.warn("expo.push.sqs.region '{}' is not a recognised AWS region — "
                + "proceeding, but verify this is intentional", region);
        }
        return r;
    }

    private static int validatedDelaySeconds(int delay) {
        if (delay > 900) {
            log.warn("expo.push.sqs.receipt-delay-seconds ({}) exceeds SQS maximum (900) — clamping to 900s", delay);
            return 900;
        }
        return Math.max(0, delay);
    }

    private static int validatedBatchMaxSize(int maxSize) {
        if (maxSize > 10) {
            log.warn("expo.push.batch.max-size ({}) exceeds the SQS receive limit of 10 — clamping to 10. "
                + "The SQS backend sends one receive batch per Expo call and does not accumulate "
                + "across receives, so batch sizes above 10 are not supported.", maxSize);
            return 10;
        }
        if (maxSize < 1) {
            log.warn("expo.push.batch.max-size ({}) is below 1 — clamping to 1", maxSize);
            return 1;
        }
        return maxSize;
    }

    @Bean
    @ConditionalOnMissingBean
    public SqsNotificationBackend sqsNotificationBackend(
        SqsClient sqsClient,
        ObjectMapper objectMapper,
        PayloadEncryptor payloadEncryptor,
        ExpoPushProperties properties
    ) {
        return new SqsNotificationBackend(
            sqsClient, objectMapper, payloadEncryptor, properties.getSqs().getPushQueueName());
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "expo.push.sqs", name = "consumers-enabled", havingValue = "true", matchIfMissing = true)
    public PushNotificationQueueConsumer pushNotificationQueueConsumer(
        SqsClient sqsClient,
        NotificationHandlerRegistry registry,
        ExpoGateway expoGateway,
        @Qualifier("expoSendRateLimiter") ExpoRateLimiter sendRateLimiter,
        PayloadEncryptor payloadEncryptor,
        @Qualifier("sqsRetry") Retry sqsRetry,
        ObjectMapper objectMapper,
        ExpoPushProperties properties
    ) {
        ExpoPushProperties.Sqs sqs = properties.getSqs();
        var config = new PushNotificationQueueConsumer.Config(
            sqsRetry,
            validatedBatchMaxSize(properties.getBatch().getMaxSize()),
            validatedDelaySeconds(sqs.getReceiptDelaySeconds()),
            sqs.getMaxPushRetryReceives(),
            sqs.getInFlightVisibilitySeconds(),
            properties.getShutdownTimeout().toMillis(),
            sqs.getPushQueueName(),
            sqs.getReceiptQueueName()
        );
        return new PushNotificationQueueConsumer(
            sqsClient, registry, expoGateway, sendRateLimiter, payloadEncryptor, objectMapper, config);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "expo.push.sqs", name = "consumers-enabled", havingValue = "true", matchIfMissing = true)
    public PushReceiptQueueConsumer pushReceiptQueueConsumer(
        SqsClient sqsClient,
        NotificationHandlerRegistry registry,
        ExpoGateway expoGateway,
        @Qualifier("expoReceiptRateLimiter") ExpoRateLimiter receiptRateLimiter,
        PayloadEncryptor payloadEncryptor,
        ObjectMapper objectMapper,
        ExpoPushProperties properties
    ) {
        ExpoPushProperties.Sqs sqs = properties.getSqs();
        var config = new PushReceiptQueueConsumer.Config(
            sqs.getMaxReceiptAttempts(),
            properties.getShutdownTimeout().toMillis(),
            sqs.getReceiptQueueName()
        );
        return new PushReceiptQueueConsumer(
            sqsClient, registry, expoGateway, receiptRateLimiter, payloadEncryptor, objectMapper, config);
    }
}
