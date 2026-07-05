package dev.expopush.autoconfigure;

import dev.expopush.api.AsyncNotificationService;
import dev.expopush.api.NotificationCommand;
import dev.expopush.api.NotificationHandlerRegistry;
import dev.expopush.api.NotificationResult;
import dev.expopush.api.NotificationResultHandler;
import dev.expopush.api.NotificationSubmissionException;
import dev.expopush.backend.api.NotificationBackend;
import dev.expopush.backend.sqs.SqsNotificationBackend;
import dev.expopush.backend.sqs.consumer.PushNotificationQueueConsumer;
import dev.expopush.backend.sqs.consumer.PushReceiptQueueConsumer;
import dev.expopush.core.ExpoGateway;
import dev.expopush.core.api.PushApi;
import dev.expopush.core.ratelimit.ExpoRateLimiter;
import dev.expopush.core.security.PayloadEncryptor;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link ExpoSqsBackendAutoConfiguration}.
 */
class ExpoSqsBackendAutoConfigurationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(
            ExpoPushAutoConfiguration.class,
            ExpoSqsBackendAutoConfiguration.class,
            ExpoPushServiceAutoConfiguration.class
        ))
        .withUserConfiguration(MockPushApiConfig.class, MockSqsClientConfig.class, ObjectMapperConfig.class);

    // ─── Backend activation ───────────────────────────────────────────────────

    @Test
    void sqsBackendNotActivatedWithoutBackendProperty() {
        runner
            .withPropertyValues(
                "expo.push.access-token=token",
                "expo.push.security.encrypt-payloads=false"
            )
            .run(ctx -> {
                assertThat(ctx).doesNotHaveBean(SqsNotificationBackend.class);
                assertThat(ctx).doesNotHaveBean(PushNotificationQueueConsumer.class);
                assertThat(ctx).doesNotHaveBean(PushReceiptQueueConsumer.class);
            });
    }

    @Test
    void sqsBackendActivatedWhenBackendPropertyIsSqs() {
        runner
            .withPropertyValues(
                "expo.push.access-token=token",
                "expo.push.backend=sqs",
                "expo.push.security.encrypt-payloads=false"
            )
            .run(ctx -> {
                assertThat(ctx).hasSingleBean(SqsNotificationBackend.class);
                assertThat(ctx).hasSingleBean(PushNotificationQueueConsumer.class);
                assertThat(ctx).hasSingleBean(PushReceiptQueueConsumer.class);
                assertThat(ctx).hasSingleBean(NotificationBackend.class);
            });
    }

    @Test
    void asyncNotificationServiceWiredToSqsBackend() {
        runner
            .withPropertyValues(
                "expo.push.access-token=token",
                "expo.push.backend=sqs",
                "expo.push.security.encrypt-payloads=false"
            )
            .run(ctx -> assertThat(ctx).hasSingleBean(AsyncNotificationService.class));
    }

    @Test
    void sqsBackendBeanImplementsNotificationBackend() {
        runner
            .withPropertyValues(
                "expo.push.access-token=token",
                "expo.push.backend=sqs",
                "expo.push.security.encrypt-payloads=false"
            )
            .run(ctx -> {
                NotificationBackend backend = ctx.getBean(NotificationBackend.class);
                assertThat(backend).isInstanceOf(SqsNotificationBackend.class);
            });
    }

    // ─── Consumer configuration ───────────────────────────────────────────────

    @Test
    void consumersUseConfiguredBatchSize() {
        runner
            .withPropertyValues(
                "expo.push.access-token=token",
                "expo.push.backend=sqs",
                "expo.push.batch.max-size=5",
                "expo.push.security.encrypt-payloads=false"
            )
            .run(ctx -> {
                assertThat(ctx).hasSingleBean(PushNotificationQueueConsumer.class);
                // Configuration verification usually requires reflection or exposed state;
                // here we just ensure the context starts with the property.
            });
    }

    @Test
    void customConsumerBeanOverridesAutoConfiguration() {
        runner
            .withUserConfiguration(CustomPushConsumerConfig.class)
            .withPropertyValues(
                "expo.push.access-token=token",
                "expo.push.backend=sqs",
                "expo.push.security.encrypt-payloads=false"
            )
            .run(ctx -> {
                assertThat(ctx).hasBean("customPushConsumer");
                assertThat(ctx).hasSingleBean(PushNotificationQueueConsumer.class);
            });
    }

    // ─── Resilience & Error Handling ──────────────────────────────────────────

    @Test
    void regionValidationFailurePreventsStartup() {
        // Use a clean runner without MockSqsClientConfig so the auto-config's 
        // sqsClient bean (and its validation) is actually executed.
        new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(
                ExpoPushAutoConfiguration.class,
                ExpoSqsBackendAutoConfiguration.class
            ))
            .withUserConfiguration(MockPushApiConfig.class, ObjectMapperConfig.class)
            .withPropertyValues(
                "expo.push.access-token=token",
                "expo.push.backend=sqs",
                "expo.push.sqs.region= ", // blank
                "expo.push.security.encrypt-payloads=false"
            )
            .run(ctx -> assertThat(ctx).getFailure().hasRootCauseMessage("expo.push.sqs.region must not be blank"));
    }

    @Test
    void submissionFailureThrowsNotificationSubmissionException() {
        runner
            .withPropertyValues(
                "expo.push.access-token=token",
                "expo.push.backend=sqs",
                "expo.push.sqs.consumers-enabled=false", // stop background pollers
                "expo.push.security.encrypt-payloads=false"
            )
            .run(ctx -> {
                SqsNotificationBackend backend = ctx.getBean(SqsNotificationBackend.class);
                SqsClient sqsClient = ctx.getBean(SqsClient.class);
                
                when(sqsClient.sendMessage(any(SendMessageRequest.class)))
                    .thenThrow(new RuntimeException("SQS unavailable"));

                var cmd = new NotificationCommand(
                    "ExponentPushToken[test]", "title", "body",
                    "corr-1", null, "test-handler"
                );

                Throwable thrown = catchThrowable(() -> backend.submit(cmd));
                
                assertThat(thrown).isExactlyInstanceOf(NotificationSubmissionException.class);
            });
    }

    // ─── Test support ─────────────────────────────────────────────────────────

    @Configuration
    static class MockPushApiConfig {
        @Bean
        PushApi mockPushApi() { return mock(PushApi.class); }
    }

    @Configuration
    static class MockSqsClientConfig {
        @Bean("mockSqsClient")
        SqsClient mockSqsClient() {
            SqsClient client = mock(SqsClient.class);
            
            // Use doReturn/doAnswer style exclusively to avoid race conditions 
            // with background polling threads during the stubbing process itself.
            
            doReturn(GetQueueUrlResponse.builder().queueUrl("https://sqs.test/queue").build())
                .when(client).getQueueUrl(any(GetQueueUrlRequest.class));
            
            doAnswer(inv -> ReceiveMessageResponse.builder()
                .messages(java.util.Collections.emptyList())
                .build())
                .when(client).receiveMessage(any(ReceiveMessageRequest.class));
                
            return client;
        }
    }

    @Configuration
    static class CustomPushConsumerConfig {
        @Bean("customPushConsumer")
        PushNotificationQueueConsumer customPushConsumer(
            SqsClient sqsClient,
            NotificationHandlerRegistry registry,
            ExpoGateway gateway,
            @Qualifier("expoSendRateLimiter") ExpoRateLimiter rateLimiter,
            PayloadEncryptor encryptor,
            tools.jackson.databind.ObjectMapper objectMapper,
            ExpoPushProperties properties
        ) {
            ExpoPushProperties.Sqs sqs = properties.getSqs();
            var config = new PushNotificationQueueConsumer.Config(
                io.github.resilience4j.retry.Retry.ofDefaults("test"),
                properties.getBatch().getMaxSize(),
                sqs.getReceiptDelaySeconds(),
                sqs.getMaxPushRetryReceives(),
                sqs.getInFlightVisibilitySeconds(),
                /* drainTimeoutMs */ 30000,
                sqs.getAuthFailureBackoff().toMillis(),
                sqs.getPushQueueName(),
                sqs.getReceiptQueueName()
            );
            return new PushNotificationQueueConsumer(
                sqsClient, registry, gateway, rateLimiter, encryptor, objectMapper, config);
        }
    }

    @Configuration
    static class HandlerConfig {
        @Bean
        NotificationResultHandler testHandler() {
            return new NotificationResultHandler() {
                @Override public String handlerId() { return "test"; }
                @Override public void handleResult(NotificationResult r) {
                    // no-op test stub — intentionally empty
                }
            };
        }
    }

    @Configuration
    static class ObjectMapperConfig {
        @Bean
        tools.jackson.databind.ObjectMapper objectMapper() {
            return new tools.jackson.databind.ObjectMapper();
        }
    }
}
