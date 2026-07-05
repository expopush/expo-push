package dev.expopush.autoconfigure;

import dev.expopush.api.AsyncNotificationService;
import dev.expopush.api.LogMasker;
import dev.expopush.api.NotificationHandlerRegistry;
import dev.expopush.api.NotificationResultHandler;
import dev.expopush.backend.api.NotificationBackend;
import dev.expopush.core.ExpoBearerTokenInterceptor;
import dev.expopush.core.ExpoClientErrorDecoder;
import dev.expopush.core.ExpoGateway;
import dev.expopush.core.api.PushApi;
import dev.expopush.core.exception.ExpoRateLimitException;
import dev.expopush.core.exception.ExpoServerException;
import dev.expopush.core.ratelimit.ExpoRateLimiter;
import dev.expopush.core.ratelimit.LocalExpoRateLimiter;
import dev.expopush.core.security.AesPayloadEncryptor;
import dev.expopush.core.security.NoOpPayloadEncryptor;
import dev.expopush.core.security.PayloadEncryptor;
import feign.Feign;
import feign.slf4j.Slf4jLogger;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.ratelimiter.RateLimiterConfig;
import io.github.resilience4j.ratelimiter.RateLimiterRegistry;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.openfeign.FeignClientsConfiguration;
import org.springframework.cloud.openfeign.support.FeignHttpMessageConverters;
import org.springframework.cloud.openfeign.support.ResponseEntityDecoder;
import org.springframework.cloud.openfeign.support.SpringDecoder;
import org.springframework.cloud.openfeign.support.SpringEncoder;
import org.springframework.cloud.openfeign.support.SpringMvcContract;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

import java.util.List;

/**
 * Core auto-configuration for the Expo Push Notification starter.
 *
 * <p>Always registers:
 * <ul>
 *   <li>{@link PushApi} — Feign client for the Expo HTTP API.
 *   <li>{@link Retry expoRetry} — Resilience4j retry for Expo API calls.
 *   <li>{@link ExpoRateLimiter} — node-local rate limiter (replaceable via {@code @Bean}).
 *   <li>{@link ExpoGateway} — thin HTTP gateway wrapping the Feign client.
 *   <li>{@link NotificationHandlerRegistry} — discovers all handler beans.
 *   <li>{@link AsyncNotificationService} — public façade (requires exactly one {@link NotificationBackend}).
 * </ul>
 *
 * <p>{@link AsyncNotificationService} is only created when a single {@link NotificationBackend}
 * is present in the context. To use a custom backend, register your own {@link NotificationBackend}
 * bean and the service will delegate to it automatically.
 */
@Slf4j
@AutoConfiguration
@EnableConfigurationProperties(ExpoPushProperties.class)
@Import(FeignClientsConfiguration.class)
public class ExpoPushAutoConfiguration {

    private final ExpoPushProperties properties;

    public ExpoPushAutoConfiguration(ExpoPushProperties properties) {
        this.properties = properties;
    }

    @PostConstruct
    public void init() {
        validateRanges(properties);
        LogMasker.setMaskingEnabled(properties.getSecurity().isMaskSensitiveDataInLogs());
        log.info("Expo Push security initialized (maskSensitiveDataInLogs={}, encryptPayloads={})",
            properties.getSecurity().isMaskSensitiveDataInLogs(),
            properties.getSecurity().isEncryptPayloads());
    }

    /**
     * Programmatic range validation, deliberately NOT JSR-303: Spring Boot attempts bean
     * validation whenever the jakarta.validation API is present and fails with
     * NoProviderFoundException when no implementation is — a classpath landmine a starter
     * must not plant in consumer applications.
     */
    static void validateRanges(ExpoPushProperties p) {
        requireAtLeast(1, p.getBatch().getMaxSize(), "expo.push.batch.max-size");
        requireAtLeast(1, p.getBatch().getMaxRetryAttempts(), "expo.push.batch.max-retry-attempts");
        requireAtLeast(1, p.getRateLimit().getSendPermitsPerSecond(), "expo.push.rate-limit.send-permits-per-second");
        requireAtLeast(1, p.getRateLimit().getReceiptPermitsPerSecond(), "expo.push.rate-limit.receipt-permits-per-second");
        requireAtLeast(0, p.getSqs().getReceiptDelaySeconds(), "expo.push.sqs.receipt-delay-seconds");
        requireAtLeast(1, p.getSqs().getMaxReceiptAttempts(), "expo.push.sqs.max-receipt-attempts");
        requireAtLeast(1, p.getSqs().getMaxPushRetryReceives(), "expo.push.sqs.max-push-retry-receives");
        requireAtLeast(0, p.getSqs().getInFlightVisibilitySeconds(), "expo.push.sqs.in-flight-visibility-seconds");
        requireAtLeast(1, p.getSqs().getReceiptPublishMaxAttempts(), "expo.push.sqs.receipt-publish-max-attempts");
        requireAtLeast(0, p.getLocal().getReceiptDelaySeconds(), "expo.push.local.receipt-delay-seconds");
        requireAtLeast(1, p.getLocal().getMaxReceiptAttempts(), "expo.push.local.max-receipt-attempts");
        requireAtLeast(1, p.getLocal().getMaxQueueSize(), "expo.push.local.max-queue-size");
        requireAtLeast(0, p.getH2().getReceiptDelaySeconds(), "expo.push.h2.receipt-delay-seconds");
        requireAtLeast(1, p.getH2().getMaxReceiptAttempts(), "expo.push.h2.max-receipt-attempts");
    }

    private static void requireAtLeast(int min, int actual, String property) {
        if (actual < min) {
            throw new IllegalStateException(
                property + " must be >= " + min + " (got " + actual + ")");
        }
    }

    // ─── Security ─────────────────────────────────────────────────────────────

    @Bean
    @ConditionalOnMissingBean
    public PayloadEncryptor payloadEncryptor() {
        if (properties.getSecurity().isEncryptPayloads()) {
            return new AesPayloadEncryptor(properties.getSecurity().getEncryptionKey());
        }
        return new NoOpPayloadEncryptor();
    }

    // ─── Feign HTTP client ────────────────────────────────────────────────────

    @Bean
    @ConditionalOnMissingBean
    public PushApi expoPushClient(
        ObjectProvider<FeignHttpMessageConverters> messageConverters
    ) {
        validateApiUrl(properties);
        return Feign.builder()
            .contract(new SpringMvcContract())
            .encoder(new SpringEncoder(messageConverters))
            .decoder(new ResponseEntityDecoder(new SpringDecoder(messageConverters)))
            .requestInterceptor(new ExpoBearerTokenInterceptor(properties.getAccessToken()))
            .errorDecoder(new ExpoClientErrorDecoder())
            .logger(new Slf4jLogger(PushApi.class))
            .logLevel(properties.getLogLevel())
            // Explicit timeouts: an unresponsive Expo endpoint must not hang a consumer
            // thread for Feign's 60 s default read timeout per attempt.
            .options(new feign.Request.Options(
                properties.getConnectTimeout(), properties.getReadTimeout(), true))
            .target(PushApi.class, properties.getApiUrl());
    }

    private static void validateApiUrl(ExpoPushProperties properties) {
        String url = properties.getApiUrl();
        if (url == null || url.isBlank()) {
            throw new IllegalArgumentException("expo.push.api-url must not be blank");
        }
        boolean isHttps = url.toLowerCase().startsWith("https://");
        if (!isHttps) {
            if (properties.isAllowInsecureApiUrl()) {
                log.warn("expo.push.api-url uses a non-https scheme ({}). "
                    + "The bearer token will be transmitted in plaintext. "
                    + "Do not use allow-insecure-api-url=true in production.", url);
            } else {
                throw new IllegalArgumentException(
                    "expo.push.api-url must use https (got: " + url + "). "
                    + "Set expo.push.allow-insecure-api-url=true to override in non-production environments.");
            }
        }

        String token = properties.getAccessToken();
        if (token == null || token.isBlank()) {
            throw new IllegalArgumentException(
                "expo.push.access-token must not be blank — set it to your Expo access token");
        }
    }

    // ─── Retry ────────────────────────────────────────────────────────────────

    @Bean(name = "expoSendRetry")
    @ConditionalOnMissingBean(name = "expoSendRetry")
    public Retry expoSendRetry(ExpoPushProperties properties) {
        return buildExpoRetry("expo-send", properties);
    }

    @Bean(name = "expoReceiptRetry")
    @ConditionalOnMissingBean(name = "expoReceiptRetry")
    public Retry expoReceiptRetry(ExpoPushProperties properties) {
        return buildExpoRetry("expo-receipt", properties);
    }

    /**
     * Retries transient Expo failures (429 rate limit, 5xx) with exponential backoff.
     * When Expo supplies a {@code Retry-After} on a 429, that wait is honored instead
     * of the computed backoff for the attempt.
     */
    private static Retry buildExpoRetry(String name, ExpoPushProperties properties) {
        ExpoPushProperties.Batch batch = properties.getBatch();
        long baseBackoffMs = batch.getRetryBackoff().toMillis();
        IntervalFunction backoff = IntervalFunction.ofExponentialRandomBackoff(baseBackoffMs, 2.0, 0.5);

        RetryConfig config = RetryConfig.custom()
            .maxAttempts(batch.getMaxRetryAttempts())
            .intervalBiFunction((attempt, either) -> {
                if (either != null && either.isLeft()
                    && either.getLeft() instanceof ExpoRateLimitException rle
                    && rle.getRetryAfterSeconds() != null) {
                    return rle.getRetryAfterSeconds() * 1000L;
                }
                return backoff.apply(attempt);
            })
            .retryOnException(t ->
                t instanceof ExpoRateLimitException || t instanceof ExpoServerException)
            .build();

        return RetryRegistry.of(config).retry(name);
    }

    // ─── Rate limiters ────────────────────────────────────────────────────────

    @Bean(name = "expoSendRateLimiter")
    @ConditionalOnMissingBean(name = "expoSendRateLimiter")
    public ExpoRateLimiter expoSendRateLimiter(ExpoPushProperties properties) {
        ExpoPushProperties.RateLimit rl = properties.getRateLimit();
        RateLimiterConfig config = RateLimiterConfig.custom()
            .limitForPeriod(rl.getSendPermitsPerSecond())
            .limitRefreshPeriod(java.time.Duration.ofSeconds(1))
            .timeoutDuration(rl.getTimeout())
            .build();
        return new LocalExpoRateLimiter(RateLimiterRegistry.of(config).rateLimiter("expo-send"));
    }

    @Bean(name = "expoReceiptRateLimiter")
    @ConditionalOnMissingBean(name = "expoReceiptRateLimiter")
    public ExpoRateLimiter expoReceiptRateLimiter(ExpoPushProperties properties) {
        ExpoPushProperties.RateLimit rl = properties.getRateLimit();
        RateLimiterConfig config = RateLimiterConfig.custom()
            .limitForPeriod(rl.getReceiptPermitsPerSecond())
            .limitRefreshPeriod(java.time.Duration.ofSeconds(1))
            .timeoutDuration(rl.getTimeout())
            .build();
        return new LocalExpoRateLimiter(RateLimiterRegistry.of(config).rateLimiter("expo-receipt"));
    }

    // ─── Gateway ──────────────────────────────────────────────────────────────

    @Bean
    @ConditionalOnMissingBean
    public ExpoGateway expoGateway(
        PushApi pushApi,
        @Qualifier("expoSendRetry") Retry expoSendRetry,
        @Qualifier("expoReceiptRetry") Retry expoReceiptRetry
    ) {
        return new ExpoGateway(pushApi, expoSendRetry, expoReceiptRetry);
    }

    // ─── Handler registry ─────────────────────────────────────────────────────

    @Bean
    @ConditionalOnMissingBean
    public NotificationHandlerRegistry notificationHandlerRegistry(
        List<NotificationResultHandler> handlers
    ) {
        return new DefaultNotificationHandlerRegistry(handlers);
    }

}
