/**
 * amqp-resilient
 * Production-ready AMQP client with retry, reconnection, and resilience patterns
 */

// Connection
export { ConnectionManager } from './connection/ConnectionManager.js';

// Consumer
export { BaseConsumer } from './consumer/BaseConsumer.js';

// Publisher
export { BasePublisher } from './publisher/BasePublisher.js';

// Patterns
export { CircuitBreaker, CircuitBreakerOpenError } from './patterns/CircuitBreaker.js';

// Health
export { HealthService } from './health/HealthService.js';

// Types
export {
  // Logger
  type AmqpLogger,
  noopLogger,
  // Connection
  type ConnectionOptions,
  ConnectionStatus,
  // Consumer
  type ConsumerOptions,
  type MessageContext,
  // Publisher
  type PublisherOptions,
  type PublishOptions,
  type PublishResult,
  // Circuit Breaker
  type CircuitBreakerOptions,
  CircuitState,
} from './types.js';
