package dev.expopush.autoconfigure;

import dev.expopush.api.NotificationHandlerRegistry;
import dev.expopush.backend.api.NotificationBackend;
import dev.expopush.backend.h2.H2NotificationBackend;
import dev.expopush.backend.h2.H2ReceiptOrchestrator;
import dev.expopush.core.api.PushApi;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class ExpoH2BackendAutoConfigurationTest {

    @TempDir
    Path tempDir;

    private ApplicationContextRunner runner() {
        return new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(
                ExpoPushAutoConfiguration.class,
                ExpoH2BackendAutoConfiguration.class,
                ExpoPushServiceAutoConfiguration.class
            ))
            .withUserConfiguration(MockPushApiConfig.class, MockRegistryConfig.class, ObjectMapperConfig.class);
    }

    // ─── Activation guard ─────────────────────────────────────────────────────

    @Test
    void h2BeansAbsentWithoutBackendProperty() {
        runner()
            .withPropertyValues(
                "expo.push.access-token=token",
                "expo.push.security.encrypt-payloads=false"
            )
            .run(ctx -> {
                assertThat(ctx).doesNotHaveBean(H2NotificationBackend.class);
                assertThat(ctx).doesNotHaveBean(H2ReceiptOrchestrator.class);
            });
    }

    // ─── Activation ───────────────────────────────────────────────────────────

    @Test
    void h2BeansCreatedWhenBackendIsH2() {
        runner()
            .withPropertyValues(
                "expo.push.access-token=token",
                "expo.push.backend=h2",
                "expo.push.security.encrypt-payloads=false",
                "expo.push.h2.file-path=" + tempDir.resolve("testdb").toAbsolutePath()
            )
            .run(ctx -> {
                assertThat(ctx).hasSingleBean(H2ReceiptOrchestrator.class);
                assertThat(ctx).hasSingleBean(NotificationBackend.class);
                assertThat(ctx.getBean(NotificationBackend.class))
                    .isInstanceOf(H2NotificationBackend.class);
            });
    }

    @Test
    void h2BackendDoesNotPublishADataSourceBean() {
        // A starter-provided DataSource bean would make Boot's DataSourceAutoConfiguration
        // back off and break by-type injection of the host app's own datasource.
        runner()
            .withPropertyValues(
                "expo.push.access-token=token",
                "expo.push.backend=h2",
                "expo.push.security.encrypt-payloads=false",
                "expo.push.h2.file-path=" + tempDir.resolve("nodstestdb").toAbsolutePath()
            )
            .run(ctx -> assertThat(ctx).doesNotHaveBean(javax.sql.DataSource.class));
    }

    // ─── H2 DataSource validation ─────────────────────────────────────────────

    @Test
    void blankFilePathFailsContextStartup() {
        runner()
            .withPropertyValues(
                "expo.push.access-token=token",
                "expo.push.backend=h2",
                "expo.push.security.encrypt-payloads=false",
                "expo.push.h2.file-path= "
            )
            .run(ctx -> assertThat(ctx).hasFailed()
                .getFailure().hasMessageContaining("file-path"));
    }

    @Test
    void filePathWithSemicolonFailsContextStartup() {
        runner()
            .withPropertyValues(
                "expo.push.access-token=token",
                "expo.push.backend=h2",
                "expo.push.security.encrypt-payloads=false",
                "expo.push.h2.file-path=./db;INIT=RUNSCRIPT FROM 'evil'"
            )
            .run(ctx -> assertThat(ctx).hasFailed()
                .getFailure().hasMessageContaining("';'"));
    }

    @Test
    void filePathWithEqualsFailsContextStartup() {
        runner()
            .withPropertyValues(
                "expo.push.access-token=token",
                "expo.push.backend=h2",
                "expo.push.security.encrypt-payloads=false",
                "expo.push.h2.file-path=./db=evil"
            )
            .run(ctx -> assertThat(ctx).hasFailed()
                .getFailure().hasMessageContaining("'='"));
    }

    @Test
    void filePathWithColonFailsContextStartup() {
        runner()
            .withPropertyValues(
                "expo.push.access-token=token",
                "expo.push.backend=h2",
                "expo.push.security.encrypt-payloads=false",
                "expo.push.h2.file-path=C:/Users/db"
            )
            .run(ctx -> assertThat(ctx).hasFailed()
                .getFailure().hasMessageContaining("':'"));
    }

    // ─── Test support ─────────────────────────────────────────────────────────

    @Configuration
    static class MockPushApiConfig {
        @Bean
        PushApi mockPushApi() { return mock(PushApi.class); }
    }

    @Configuration
    static class MockRegistryConfig {
        @Bean
        NotificationHandlerRegistry mockRegistry() { return mock(NotificationHandlerRegistry.class); }
    }

    @Configuration
    static class ObjectMapperConfig {
        @Bean
        tools.jackson.databind.ObjectMapper objectMapper() {
            return new tools.jackson.databind.ObjectMapper();
        }
    }
}
