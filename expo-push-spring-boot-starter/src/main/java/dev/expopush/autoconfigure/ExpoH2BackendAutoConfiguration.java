package dev.expopush.autoconfigure;

import dev.expopush.api.NotificationHandlerRegistry;
import dev.expopush.backend.api.NotificationBackend;
import dev.expopush.backend.h2.H2NotificationBackend;
import dev.expopush.backend.h2.H2ReceiptOrchestrator;
import dev.expopush.core.ExpoGateway;
import dev.expopush.core.security.PayloadEncryptor;
import tools.jackson.databind.ObjectMapper;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;

/**
 * Auto-configuration for the persistent H2-backed notification backend.
 */
@AutoConfiguration(after = ExpoPushAutoConfiguration.class)
@ConditionalOnClass({H2NotificationBackend.class, JdbcTemplate.class})
@ConditionalOnProperty(prefix = "expo.push", name = "backend", havingValue = "h2")
@EnableConfigurationProperties(ExpoPushProperties.class)
public class ExpoH2BackendAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(name = "expoH2DataSource")
    public DataSource expoH2DataSource(ExpoPushProperties properties) {
        ExpoPushProperties.H2 h2 = properties.getH2();
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
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("org.h2.Driver");
        dataSource.setUrl("jdbc:h2:file:" + filePath + ";DB_CLOSE_DELAY=-1");
        dataSource.setUsername(h2.getUsername());
        dataSource.setPassword(h2.getPassword());
        return dataSource;
    }

    @Bean
    @ConditionalOnMissingBean(name = "expoH2JdbcTemplate")
    public JdbcTemplate expoH2JdbcTemplate(DataSource expoH2DataSource) {
        return new JdbcTemplate(expoH2DataSource);
    }

    @Bean
    @ConditionalOnMissingBean
    public H2ReceiptOrchestrator h2ReceiptOrchestrator(
            JdbcTemplate expoH2JdbcTemplate,
            ExpoGateway expoGateway,
            NotificationHandlerRegistry registry,
            ObjectMapper objectMapper,
            PayloadEncryptor payloadEncryptor,
            ExpoPushProperties properties) {
        ExpoPushProperties.H2 h2 = properties.getH2();
        return new H2ReceiptOrchestrator(
            expoH2JdbcTemplate,
            expoGateway,
            registry,
            objectMapper,
            payloadEncryptor,
            h2.getMaxReceiptAttempts(),
            h2.getReceiptDelaySeconds()
        );
    }

    @Bean
    @ConditionalOnMissingBean
    public NotificationBackend h2NotificationBackend(
            ExpoGateway expoGateway,
            JdbcTemplate expoH2JdbcTemplate,
            NotificationHandlerRegistry registry,
            ObjectMapper objectMapper,
            PayloadEncryptor payloadEncryptor,
            ExpoPushProperties properties) {
        return new H2NotificationBackend(
            expoGateway,
            expoH2JdbcTemplate,
            registry,
            objectMapper,
            payloadEncryptor,
            properties.getH2().getReceiptDelaySeconds()
        );
    }
}
