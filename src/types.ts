/**
 * Core types for amqp-resilient
 */

/**
 * Logger interface - inject your own logger implementation
 * Compatible with pino, winston, bunyan, console, etc.
 */
export interface AmqpLogger {
  info(obj: object, msg?: string): void;
  warn(obj: object, msg?: string): void;
  error(obj: object, msg?: string): void;
  debug(obj: object, msg?: string): void;
}

/**
 * Connection options
 */
export interface ConnectionOptions {
  /** Full AMQP URL (alternative to individual params) */
  url?: string;
  /** RabbitMQ host */
  host?: string;
  /** RabbitMQ port (default: 5672) */
  port?: number;
  /** RabbitMQ username */
  username?: string;
  /** RabbitMQ password */
  password?: string;
  /** RabbitMQ virtual host (default: /) */
  vhost?: string;
  /** Connection name for identification (default: default) */
  connectionName?: string;
  /** Prefetch count for channels (default: 10) */
  prefetch?: number;
  /** Connection heartbeat in seconds (default: 60) */
  heartbeat?: number;
  /** Maximum reconnection attempts (0 = unlimited, default: 0) */
  maxReconnectAttempts?: number;
  /** Initial reconnection delay in ms (default: 1000) */
  initialReconnectDelay?: number;
  /** Maximum reconnection delay in ms (default: 60000) */
  maxReconnectDelay?: number;
  /** Logger instance (optional) */
  logger?: AmqpLogger;
}

/**
 * Connection status
 */
export enum ConnectionStatus {
  DISCONNECTED = 'disconnected',
  CONNECTING = 'connecting',
  CONNECTED = 'connected',
  RECONNECTING = 'reconnecting',
  DEAD = 'dead',
}

/**
 * Consumer options
 */
export interface ConsumerOptions {
  /** Queue name to consume from */
  queue: string;
  /** Exchange to bind to */
  exchange: string;
  /** Routing keys for binding */
  routingKeys: string[];
  /** Prefetch count (default: 10) */
  prefetch?: number;
  /** Maximum number of retries (default: 3) */
  maxRetries?: number;
  /** Initial retry delay in ms (default: 1000) */
  initialRetryDelay?: number;
  /** Maximum retry delay in ms (default: 30000) */
  maxRetryDelay?: number;
  /** Whether to use circuit breaker (default: true) */
  useCircuitBreaker?: boolean;
  /** Circuit breaker failure threshold (default: 5) */
  circuitBreakerThreshold?: number;
  /** Exchange type (default: topic) */
  exchangeType?: 'direct' | 'topic' | 'fanout' | 'headers';
  /** Logger instance (optional, inherits from connection if not provided) */
  logger?: AmqpLogger;
}

/**
 * Message context passed to consumer handler
 */
export interface MessageContext {
  /** Routing key */
  routingKey: string;
  /** Message ID */
  messageId: string;
  /** Correlation ID for tracing */
  correlationId?: string;
  /** Current retry count */
  retryCount: number;
  /** Timestamp when message was received */
  receivedAt: Date;
  /** Raw message headers */
  headers: Record<string, unknown>;
}

/**
 * Publisher options
 */
export interface PublisherOptions {
  /** Exchange name to publish to */
  exchange: string;
  /** Exchange type (default: topic) */
  exchangeType?: 'direct' | 'topic' | 'fanout' | 'headers';
  /** Whether to use publisher confirms (default: true) */
  confirm?: boolean;
  /** Maximum number of publish retries (default: 3) */
  maxRetries?: number;
  /** Initial retry delay in ms (default: 100) */
  initialRetryDelay?: number;
  /** Maximum retry delay in ms (default: 5000) */
  maxRetryDelay?: number;
  /** Whether to use circuit breaker (default: true) */
  useCircuitBreaker?: boolean;
  /** Circuit breaker failure threshold (default: 5) */
  circuitBreakerThreshold?: number;
  /** Logger instance (optional, inherits from connection if not provided) */
  logger?: AmqpLogger;
}

/**
 * Publish options for individual messages
 */
export interface PublishOptions {
  /** Correlation ID for message tracing */
  correlationId?: string;
  /** Message ID (auto-generated if not provided) */
  messageId?: string;
  /** Message priority (0-9, higher = more important) */
  priority?: number;
  /** Message expiration in milliseconds */
  expiration?: string;
  /** Custom headers */
  headers?: Record<string, unknown>;
  /** Whether message should be persistent (default: true) */
  persistent?: boolean;
  /** Reply-to queue for RPC patterns */
  replyTo?: string;
  /** Content type (default: application/json) */
  contentType?: string;
}

/**
 * Publish result
 */
export interface PublishResult {
  /** Whether the publish was successful */
  success: boolean;
  /** Message ID */
  messageId: string;
  /** Correlation ID */
  correlationId: string;
  /** Number of retries needed */
  retries: number;
  /** Error if publish failed */
  error?: Error;
}

/**
 * Circuit breaker options
 */
export interface CircuitBreakerOptions {
  /** Name for logging purposes */
  name: string;
  /** Number of failures before opening circuit (default: 5) */
  failureThreshold?: number;
  /** Time in ms to wait before attempting recovery (default: 30000) */
  resetTimeout?: number;
  /** Number of successful calls in half-open state before closing (default: 3) */
  successThreshold?: number;
  /** Time window in ms to count failures (default: 60000) */
  failureWindow?: number;
  /** Logger instance (optional) */
  logger?: AmqpLogger;
}

/**
 * Circuit breaker state
 */
export enum CircuitState {
  CLOSED = 'CLOSED',
  OPEN = 'OPEN',
  HALF_OPEN = 'HALF_OPEN',
}

/**
 * No-op logger for when no logger is provided
 */
// eslint-disable-next-line @typescript-eslint/no-empty-function
const noop = () => {};
export const noopLogger: AmqpLogger = {
  info: noop,
  warn: noop,
  error: noop,
  debug: noop,
};
