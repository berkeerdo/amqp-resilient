# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2025-01-11

### Added

- Initial release
- `ConnectionManager` with auto-reconnection and exponential backoff
- `BaseConsumer` with retry logic and dead letter queue support
- `BasePublisher` with publisher confirms and retry
- `CircuitBreaker` pattern for resilience
- `HealthService` for connection monitoring
- Full TypeScript support with type definitions
- ESM module support
- Comprehensive test suite (unit + integration)
