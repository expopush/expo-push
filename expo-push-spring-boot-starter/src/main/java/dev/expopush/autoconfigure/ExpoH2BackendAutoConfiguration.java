package dev.expopush.autoconfigure;

import dev.expopush.api.NotificationHandlerRegistry;
import dev.expopush.backend.api.NotificationBackend;
import dev.expopush.backend.h2.H2NotificationBackend;
import dev.expopush.backend.h2.H2ReceiptOrchestrator;
import dev.expopush.core.ExpoGateway;
import dev.expopush.core.security.PayloadEncryptor;
import tools.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Auto-configuration for the persistent H2-backed notification backend.
 */
@AutoConfiguration(after = ExpoPushAutoConfiguration.class)
@ConditionalOnClass({H2NotificationBackend.class, JdbcTemplate.class})
@ConditionalOnProperty(prefix = "expo.push", name = "backend", havingValue = "h2")
@EnableConfigurationProperties(ExpoPushProperties.class)
public class ExpoH2BackendAutoConfiguration {

    /**
     * The backend's private H2 database, exposed only as a named {@link JdbcTemplate}.
     * Deliberately NOT registered as a {@link DataSource} bean: a starter-provided
     * {@code DataSource} would make Spring Boot's {@code DataSourceAutoConfiguration}
     * back off and break by-type injection of the host application's own datasource.
     */
    @Bean
    @ConditionalOnMissingBean(name = "expoH2JdbcTemplate")
    public JdbcTemplate expoH2JdbcTemplate(ExpoPushProperties properties) {
        return new JdbcTemplate(buildExpoH2DataSource(properties.getH2()));
    }

    private static DataSource buildExpoH2DataSource(ExpoPushProperties.H2 h2) {
        String filePath = h2.getFilePath();
        if (filePath == null || filePath.isBlank()) {
            throw new IllegalArgumentException("expo.push.h2.file-path must not be blank");
        }
        // Reject characters that H2 treats as URL option delimiters to prevent injection
        // of options like ;INIT=RUNSCRIPT FROM '...' (CVE-2018-10054 family).
        if (filePath.contains(";") || filePath.contains("=") || filePath.contains(":")) {
            throw new IllegalArgumentException(
                "expo.push.h2.file-path must not contain ';', '=', or ':' — value: " + filePath);
        }
        // Pooled: the orchestrator polls every second and the backend does 2-3 JDBC ops per
        // submission — DriverManagerDataSource would open a fresh connection for each.
        com.zaxxer.hikari.HikariDataSource dataSource = new com.zaxxer.hikari.HikariDataSource();
        dataSource.setPoolName("expo-h2");
        dataSource.setDriverClassName("org.h2.Driver");
        dataSource.setJdbcUrl("jdbc:h2:file:" + filePath + ";DB_CLOSE_DELAY=-1");
        dataSource.setUsername(h2.getUsername());
        dataSource.setPassword(h2.getPassword());
        dataSource.setMaximumPoolSize(4);
        dataSource.setMinimumIdle(1);
        return dataSource;
    }

    @Bean
    @ConditionalOnMissingBean
    public H2ReceiptOrchestrator h2ReceiptOrchestrator(
            JdbcTemplate expoH2JdbcTemplate,
            ExpoGateway expoGateway,
            NotificationHandlerRegistry registry,
            ObjectProvider<ObjectMapper> objectMapper,
            PayloadEncryptor payloadEncryptor,
            ExpoPushProperties properties) {
        ExpoPushProperties.H2 h2 = properties.getH2();
        return new H2ReceiptOrchestrator(
            expoH2JdbcTemplate,
            expoGateway,
            registry,
            objectMapper.getIfAvailable(ObjectMapper::new),
            payloadEncryptor,
            h2.getMaxReceiptAttempts(),
            h2.getReceiptDelaySeconds()
        );
    }

    /**
     * Single-threaded so sends stay sequential (matching the throughput characteristics of
     * the previous synchronous behaviour) while keeping the Expo call — and its retry/backoff
     * cycle — off the caller's thread.
     */
    @Bean(name = "expoH2SubmissionExecutor", destroyMethod = "shutdown")
    @ConditionalOnMissingBean(name = "expoH2SubmissionExecutor")
    public ExecutorService expoH2SubmissionExecutor() {
        return Executors.newSingleThreadExecutor(
            Thread.ofVirtual().name("expo-h2-submitter").factory());
    }

    @Bean
    @ConditionalOnMissingBean
    public NotificationBackend h2NotificationBackend(
            ExpoGateway expoGateway,
            JdbcTemplate expoH2JdbcTemplate,
            NotificationHandlerRegistry registry,
            ObjectProvider<ObjectMapper> objectMapper,
            PayloadEncryptor payloadEncryptor,
            ExecutorService expoH2SubmissionExecutor,
            ExpoPushProperties properties) {
        return new H2NotificationBackend(
            expoGateway,
            expoH2JdbcTemplate,
            registry,
            objectMapper.getIfAvailable(ObjectMapper::new),
            payloadEncryptor,
            expoH2SubmissionExecutor,
            properties.getH2().getReceiptDelaySeconds()
        );
    }
}
