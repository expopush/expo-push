# Contributing to Expo Push

First off, thank you for considering contributing to Expo Push! It's people like you that make the open-source community such a great place to learn, inspire, and create.

## Code of Conduct

This project and everyone participating in it is governed by the [Expo Push Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code.

## How Can I Contribute?

### Reporting Bugs

Bugs are tracked as [GitHub issues](https://github.com/expopush/expo-push/issues). When creating a bug report, please include as many details as possible:

*   **Use a clear and descriptive title** for the issue to identify the problem.
*   **Describe the exact steps which reproduce the problem** in as many details as possible.
*   **Explain which behavior you expected to see and why** and why you think the current behavior is a bug.
*   **Include screenshots and animated GIFs** which help you demonstrate the steps or the part of the Expo Push system that the issue is about.

### Suggesting Enhancements

If you have an idea for a new feature or an improvement, please open a GitHub issue!

### Pull Requests

*   **Fill in the pull request template** (if provided).
*   **Include screenshots and animated GIFs** in your pull request description if they help explain your changes.
*   **Follow the Java/Spring coding style** used in the project.
*   **Add tests!** We won't accept a pull request without appropriate test coverage.
*   **Ensure the build passes.** Run `mvn verify` locally before submitting.

## Styleguides

### Git Commit Messages

*   Use the present tense ("Add feature" not "Added feature")
*   Use the imperative mood ("Move cursor to..." not "Moves cursor to...")
*   Limit the first line to 72 characters or less

## Technical Setup

### Prerequisites

*   JDK 21
*   Maven 3.9+

### Build and Test

```bash
mvn clean verify
```

To run a specific backend test (e.g., SQS):

```bash
mvn -pl expo-push-spring-boot-starter test -Dtest=ExpoSqsBackendAutoConfigurationTest
```

---

Thank you for your contribution!
