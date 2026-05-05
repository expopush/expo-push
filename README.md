# Expo Push

[![Java CI with Maven and Sonar](https://github.com/expopush/expo-push/actions/workflows/build.yml/badge.svg)](https://github.com/expopush/expo-push/actions/workflows/build.yml)
[![Maven Central](https://img.shields.io/maven-central/v/dev.expopush/expo-push.svg)](https://central.sonatype.com/namespace/dev.expopush)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Java Version](https://img.shields.io/badge/Java-21-ED8B00?logo=openjdk)](https://www.oracle.com/java/technologies/downloads/#java21)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.4-6DB33F?logo=springboot)](https://spring.io/projects/spring-boot)
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
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

### 2. Configure Your Backend

Add to your `application.yaml`:

```yaml
expo:
  push:
    backend: sqs # or h2, local
    sqs:
      queue-url: https://sqs.us-east-1.amazonaws.com/...
```

### 3. Send a Notification

```java
@Autowired
private ExpoPushService pushService;

public void notifyUser(String expoPushToken) {
    ExpoMessage message = new ExpoMessage(expoPushToken, "Hello World!");
    pushService.send(message);
}
```

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
