# Expo Push

[![Java CI with Maven and Sonar](https://github.com/expopush/expo-push/actions/workflows/build.yml/badge.svg)](https://github.com/expopush/expo-push/actions/workflows/build.yml)
[![Maven Central](https://img.shields.io/maven-central/v/dev.expopush/expo-push.svg)](https://central.sonatype.com/namespace/dev.expopush)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Java Version](https://img.shields.io/badge/Java-21-ED8B00?logo=openjdk)](https://www.oracle.com/java/technologies/downloads/#java21)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-4.x-6DB33F?logo=springboot)](https://spring.io/projects/spring-boot)
[![Dependabot](https://img.shields.io/badge/Dependabot-enabled-brightgreen.svg?logo=dependabot)](https://github.com/expopush/expo-push/network/updates)
[![CodeQL](https://github.com/expopush/expo-push/actions/workflows/codeql-analysis.yml/badge.svg)](https://github.com/expopush/expo-push/actions/workflows/codeql-analysis.yml)

A high-performance, enterprise-ready Spring Boot starter for sending push notifications via [Expo.io](https://expo.dev).

## Features

- **Multi-Backend Support**: Send notifications via SQS (for scale), H2/JDBC (for persistence), or Local/In-Memory (for testing).
- **Auto-Configuration**: Drop the starter into your Spring Boot project and start sending notifications in minutes.
- **Resilient**: Built-in retry and rate-limiting logic to handle Expo API constraints.
- **Type-Safe**: Complete Java API for the Expo Push Ticket and Receipt models.

## Quick Start

### 1. Add Dependency

```xml
<dependency>
    <groupId>dev.expopush</groupId>
    <artifactId>expo-push-spring-boot-starter</artifactId>
    <version>1.0.0-RC2</version>
</dependency>
```

### 2. Configure Your Backend

Add to your `application.yaml`:

```yaml
expo:
  push:
    access-token: ${EXPO_ACCESS_TOKEN}   # required — your Expo access token
    backend: sqs                         # or h2, local
    security:
      # Payload encryption at rest is ON by default and needs a 256-bit Base64 key
      # (generate one with: openssl rand -base64 32).
      encryption-key: ${EXPO_ENCRYPTION_KEY}
      # ...or opt out (title/body/metadata stored in plaintext on SQS/H2):
      # encrypt-payloads: false
    sqs:
      push-queue-name: expo-push-notifications
      receipt-queue-name: expo-push-receipts
      region: us-east-1
```

### 3. Register a Result Handler

Every notification's terminal outcome is routed to the handler named in the command:

```java
@Component
public class MyResultHandler implements NotificationResultHandler {

    @Override
    public String handlerId() {
        return "my-handler"; // stable across deployments — it travels inside queue messages
    }

    @Override
    public void handleResult(NotificationResult result) {
        switch (result.outcome()) {
            case ACCEPTED -> log.info("Delivered: {}", result.correlationId());
            case REJECTED -> deactivateToken(result.pushToken()); // DeviceNotRegistered
            case INVALID, UNKNOWN, FAILED -> log.warn("Not delivered: {}", result);
        }
    }
}
```

### 4. Send a Notification

```java
@Autowired
private AsyncNotificationService notificationService;

public void notifyUser(String expoPushToken) {
    notificationService.enqueue(new NotificationCommand(
        expoPushToken,
        "Hello",                        // title
        "Hello World!",                 // body
        UUID.randomUUID().toString(),   // correlationId — echoed back in the result
        Map.of(),                       // optional metadata, echoed back in the result
        "my-handler"                    // handlerId of the result handler above
    ));
}
```

Delivery options (custom `data` payload, Android channel, sound, ttl, badge, subtitle,
priority) go in an optional `NotificationOptions`:

```java
notificationService.enqueue(new NotificationCommand(
    expoPushToken, "Order update", "Your order shipped!",
    correlationId, Map.of(), "my-handler",
    new NotificationOptions(
        Map.of("screen", "orders", "orderId", 4711), // data — delivered to the app
        "order-updates",                             // Android channelId
        "default",                                   // iOS sound
        3600,                                        // ttl seconds
        1,                                           // iOS badge
        "Order #4711",                               // iOS subtitle
        NotificationPriority.HIGH
    )
));
```

On the persistent backends (SQS, H2), `data` and `subtitle` are encrypted at rest like
title and body.

## Documentation

Full documentation is available at [expopush.dev](https://expopush.dev).

## Contributing

We welcome contributions! Please see our [CONTRIBUTING.md](CONTRIBUTING.md) for details on how to get started.

## Security

If you discover a security vulnerability, please follow our [Security Policy](SECURITY.md).

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.

---

**Disclaimer**: This project is an independent, open-source work and is not affiliated with, endorsed by, or sponsored by 650 Industries, Inc. or the official Expo project. "Expo" is a trademark of 650 Industries, Inc.
