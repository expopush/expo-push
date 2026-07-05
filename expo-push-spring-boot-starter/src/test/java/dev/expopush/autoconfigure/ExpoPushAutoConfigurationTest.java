package dev.expopush.autoconfigure;

import dev.expopush.api.NotificationCommand;
import dev.expopush.api.NotificationResult;
import dev.expopush.api.AsyncNotificationService;
import dev.expopush.api.NotificationHandlerRegistry;
import dev.expopush.api.NotificationResultHandler;
import dev.expopush.backend.api.NotificationBackend;
import dev.expopush.core.ExpoGateway;
import dev.expopush.core.api.PushApi;
import dev.expopush.core.exception.ExpoRateLimitException;
import dev.expopush.core.exception.ExpoServerException;
import dev.expopush.core.ratelimit.ExpoRateLimiter;
import dev.expopush.core.security.AesPayloadEncryptor;
import dev.expopush.core.security.PayloadEncryptor;
import io.github.resilience4j.retry.Retry;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Tests for {@link ExpoPushAutoConfiguration}.
 *
 * <p>A mock {@link PushApi} bean is registered in every test to bypass Feign client
 * construction (which requires Spring MVC infrastructure not present in unit tests).
 */
class ExpoPushAutoConfigurationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(
            ExpoPushAutoConfiguration.class,
            ExpoPushServiceAutoConfiguration.class
        ))
        .withUserConfiguration(MockPushApiConfig.class);

    // ─── Core bean registration ───────────────────────────────────────────────

    @Test
    void coreBeansRegisteredWithMinimalProperties() {
        runner
            .withPropertyValues(
                "expo.push.access-token=test-token",
                "expo.push.security.encrypt-payloads=false"
            )
            .run(ctx -> {
                assertThat(ctx).hasSingleBean(ExpoGateway.class);
                assertThat(ctx).hasBean("expoSendRateLimiter");
                assertThat(ctx).hasBean("expoReceiptRateLimiter");
                assertThat(ctx).hasSingleBean(NotificationHandlerRegistry.class);
            });
    }

    @Test
    void asyncNotificationServiceRegisteredWhenSingleBackendPresent() {
        runner
            .withPropertyValues(
                "expo.push.access-token=test-token",
                "expo.push.security.encrypt-payloads=false"
            )
            .withUserConfiguration(MockBackendConfig.class)
            .run(ctx -> assertThat(ctx).hasSingleBean(AsyncNotificationService.class));
    }

    @Test
    void asyncNotificationServiceAbsentWithoutBackend() {
        runner
            .withPropertyValues(
                "expo.push.access-token=test-token",
                "expo.push.security.encrypt-payloads=false"
            )
            .run(ctx -> assertThat(ctx).doesNotHaveBean(AsyncNotificationService.class));
    }

    // ─── Property binding ─────────────────────────────────────────────────────

    @Test
    void propertiesBindCorrectly() {
        runner
            .withPropertyValues(
                "expo.push.access-token=my-token",
                "expo.push.api-url=https://custom.expo.host/api/v2",
                "expo.push.batch.max-size=50",
                "expo.push.batch.max-retry-attempts=5",
                "expo.push.rate-limit.receipt-permits-per-second=20",
                "expo.push.sqs.region=eu-west-1",
                "expo.push.sqs.receipt-delay-seconds=600",
                "expo.push.security.encrypt-payloads=false"
            )
            .run(ctx -> {
                ExpoPushProperties props = ctx.getBean(ExpoPushProperties.class);
                assertThat(props.getAccessToken()).isEqualTo("my-token");
                assertThat(props.getApiUrl()).isEqualTo("https://custom.expo.host/api/v2");
                assertThat(props.getBatch().getMaxSize()).isEqualTo(50);
                assertThat(props.getBatch().getMaxRetryAttempts()).isEqualTo(5);
                assertThat(props.getRateLimit().getReceiptPermitsPerSecond()).isEqualTo(20);
                assertThat(props.getSqs().getRegion()).isEqualTo("eu-west-1");
                assertThat(props.getSqs().getReceiptDelaySeconds()).isEqualTo(600);
            });
    }

    @Test
    void defaultPropertyValuesApplied() {
        runner
            .withPropertyValues(
                "expo.push.access-token=token",
                "expo.push.security.encrypt-payloads=false"
            )
            .run(ctx -> {
                ExpoPushProperties props = ctx.getBean(ExpoPushProperties.class);
                assertThat(props.getApiUrl()).isEqualTo("https://exp.host/--/api/v2");
                assertThat(props.getBatch().getMaxSize()).isEqualTo(10);
                assertThat(props.getRateLimit().getReceiptPermitsPerSecond()).isEqualTo(45);
                assertThat(props.getSqs().getPushQueueName()).isEqualTo("expo-push-notifications");
                assertThat(props.getSqs().getReceiptDelaySeconds()).isEqualTo(900);
            });
    }

    // ─── Bean override ────────────────────────────────────────────────────────

    @Test
    void customRateLimiterOverridesDefault() {
        runner
            .withPropertyValues(
                "expo.push.access-token=token",
                "expo.push.security.encrypt-payloads=false"
            )
            .withUserConfiguration(CustomRateLimiterConfig.class)
            .run(ctx -> {
                assertThat(ctx).hasBean("expoSendRateLimiter");
                assertThat(ctx.getBean("expoSendRateLimiter"))
                    .isInstanceOf(CustomRateLimiterConfig.NoOpRateLimiter.class);
            });
    }

    @Test
    void customAsyncNotificationServiceOverridesDefault() {
        runner
            .withPropertyValues(
                "expo.push.access-token=token",
                "expo.push.security.encrypt-payloads=false"
            )
            .withUserConfiguration(MockBackendConfig.class, CustomNotificationServiceConfig.class)
            .run(ctx -> {
                assertThat(ctx).hasSingleBean(AsyncNotificationService.class);
                assertThat(ctx.getBean(AsyncNotificationService.class))
                    .isInstanceOf(CustomNotificationServiceConfig.CustomService.class);
            });
    }

    // ─── API URL validation ───────────────────────────────────────────────────

    /** Runner without mock PushApi so expoPushClient() (and validateApiUrl()) is actually invoked. */
    private ApplicationContextRunner validationRunner() {
        return new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(ExpoPushAutoConfiguration.class));
    }

    @Test
    void blankApiUrlFailsContext() {
        validationRunner()
            .withPropertyValues(
                "expo.push.access-token=token",
                "expo.push.api-url= ",
                "expo.push.security.encrypt-payloads=false"
            )
            .run(ctx -> assertThat(ctx).hasFailed()
                .getFailure().hasMessageContaining("api-url"));
    }

    @Test
    void httpApiUrlWithoutOverrideFlagFailsContext() {
        validationRunner()
            .withPropertyValues(
                "expo.push.access-token=token",
                "expo.push.api-url=http://exp.host/--/api/v2",
                "expo.push.security.encrypt-payloads=false"
            )
            .run(ctx -> assertThat(ctx).hasFailed()
                .getFailure().hasMessageContaining("https"));
    }

    @Test
    void httpApiUrlWithAllowInsecureFlagSucceeds() {
        validationRunner()
            .withPropertyValues(
                "expo.push.access-token=token",
                "expo.push.api-url=http://exp.host/--/api/v2",
                "expo.push.allow-insecure-api-url=true",
                "expo.push.security.encrypt-payloads=false"
            )
            .run(ctx -> assertThat(ctx).hasNotFailed());
    }

    @Test
    void blankAccessTokenFailsContext() {
        validationRunner()
            .withPropertyValues(
                "expo.push.api-url=https://exp.host/--/api/v2",
                "expo.push.security.encrypt-payloads=false"
            )
            .run(ctx -> assertThat(ctx).hasFailed()
                .getFailure().hasMessageContaining("access-token"));
    }

    // ─── Payload encryption ───────────────────────────────────────────────────

    @Test
    void encryptionEnabledProducesAesEncryptor() {
        String key = Base64.getEncoder().encodeToString(new byte[32]);
        runner
            .withPropertyValues(
                "expo.push.access-token=token",
                "expo.push.security.encrypt-payloads=true",
                "expo.push.security.encryption-key=" + key
            )
            .run(ctx -> {
                assertThat(ctx).hasSingleBean(PayloadEncryptor.class);
                assertThat(ctx.getBean(PayloadEncryptor.class))
                    .isInstanceOf(AesPayloadEncryptor.class);
            });
    }

    // ─── Retry predicate ──────────────────────────────────────────────────────

    @Test
    void sendRetryPredicate() {
        runner
            .withPropertyValues("expo.push.access-token=token", "expo.push.security.encrypt-payloads=false")
            .run(ctx -> {
                Retry retry = ctx.getBean("expoSendRetry", Retry.class);
                var predicate = retry.getRetryConfig().getExceptionPredicate();
                assertThat(predicate.test(new ExpoRateLimitException("rate limited"))).isTrue();
                assertThat(predicate.test(new ExpoServerException("server error"))).isTrue();
                assertThat(predicate.test(new RuntimeException("other"))).isFalse();
            });
    }

    @Test
    void receiptRetryPredicate() {
        runner
            .withPropertyValues("expo.push.access-token=token", "expo.push.security.encrypt-payloads=false")
            .run(ctx -> {
                Retry retry = ctx.getBean("expoReceiptRetry", Retry.class);
                var predicate = retry.getRetryConfig().getExceptionPredicate();
                assertThat(predicate.test(new ExpoRateLimitException("rate limited"))).isTrue();
                assertThat(predicate.test(new ExpoServerException("server error"))).isTrue();
                assertThat(predicate.test(new RuntimeException("other"))).isFalse();
            });
    }

    // ─── Handler registry ─────────────────────────────────────────────────────

    @Test
    void handlerRegistryDiscoversRegisteredHandlers() {
        runner
            .withPropertyValues(
                "expo.push.access-token=token",
                "expo.push.security.encrypt-payloads=false"
            )
            .withUserConfiguration(HandlerConfig.class)
            .run(ctx -> {
                NotificationHandlerRegistry registry = ctx.getBean(NotificationHandlerRegistry.class);
                assertThat(registry.getHandler("test-handler")).isNotNull();
                assertThat(registry.getHandler("missing-handler")).isNull();
            });
    }

    // ─── Test support configurations ─────────────────────────────────────────

    /** Bypasses Feign client construction in unit tests. */
    @Configuration
    static class MockPushApiConfig {
        @Bean
        PushApi mockPushApi() { return mock(PushApi.class); }
    }

    @Configuration
    static class MockBackendConfig {
        @Bean
        NotificationBackend mockBackend() { return mock(NotificationBackend.class); }
    }

    @Configuration
    static class CustomRateLimiterConfig {
        @Bean(name = "expoSendRateLimiter")
        ExpoRateLimiter customRateLimiter() { return new NoOpRateLimiter(); }

        static class NoOpRateLimiter implements ExpoRateLimiter {
            @Override public void acquire() { /* intentionally empty — rate limiting disabled in tests */ }
        }
    }

    @Configuration
    static class CustomNotificationServiceConfig {
        @Bean
        AsyncNotificationService customService() { return new CustomService(); }

        static class CustomService implements AsyncNotificationService {
            @Override
            public void enqueue(NotificationCommand command) {
                // no-op test stub — intentionally empty
            }
        }
    }

    @Configuration
    static class HandlerConfig {
        @Bean
        NotificationResultHandler testHandler() {
            return new NotificationResultHandler() {
                @Override public String handlerId() { return "test-handler"; }
                @Override public void handleResult(NotificationResult result) {
                    // no-op test stub — intentionally empty
                }
            };
        }
    }
}
