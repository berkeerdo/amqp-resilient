/**
 * Base Publisher for AMQP
 * Production-ready implementation with:
 * - Publisher confirms for guaranteed delivery
 * - Automatic retry with exponential backoff
 * - Circuit breaker integration
 * - Proper message properties and headers
 *
 * Best Practices:
 * 1. Always use publisher confirms for critical messages
 * 2. Implement retry logic for transient failures
 * 3. Use circuit breaker to prevent cascading failures
 * 4. Set persistent delivery mode for durable messages
 * 5. Include correlation IDs for tracing
 */
import type { ConfirmChannel, Channel } from 'amqplib';
import { randomUUID } from 'crypto';
import type { ConnectionManager } from '../connection/ConnectionManager.js';
import { CircuitBreaker, CircuitBreakerOpenError } from '../patterns/CircuitBreaker.js';
import type { AmqpLogger, PublisherOptions, PublishOptions, PublishResult } from '../types.js';

/** Default retry configuration */
const DEFAULTS = {
  MAX_RETRIES: 3,
  INITIAL_RETRY_DELAY: 100,
  MAX_RETRY_DELAY: 5000,
  CIRCUIT_BREAKER_THRESHOLD: 5,
};

/**
 * BasePublisher - Base class for AMQP publishers
 * Use this class directly or extend it for specific publishers
 */
export class BasePublisher {
  private channel: Channel | ConfirmChannel | null = null;
  private isInitialized = false;
  private circuitBreaker: CircuitBreaker | null = null;
  protected readonly logger: AmqpLogger;

  private readonly exchange: string;
  private readonly exchangeType: 'direct' | 'topic' | 'fanout' | 'headers';
  private readonly confirm: boolean;
  private readonly maxRetries: number;
  private readonly initialRetryDelay: number;
  private readonly maxRetryDelay: number;
  private readonly useCircuitBreaker: boolean;

  constructor(
    protected readonly connection: ConnectionManager,
    options: PublisherOptions
  ) {
    this.exchange = options.exchange;
    this.exchangeType = options.exchangeType ?? 'topic';
    this.confirm = options.confirm ?? true;
    this.maxRetries = options.maxRetries ?? DEFAULTS.MAX_RETRIES;
    this.initialRetryDelay = options.initialRetryDelay ?? DEFAULTS.INITIAL_RETRY_DELAY;
    this.maxRetryDelay = options.maxRetryDelay ?? DEFAULTS.MAX_RETRY_DELAY;
    this.useCircuitBreaker = options.useCircuitBreaker ?? true;
    this.logger = options.logger ?? connection.getLogger();

    // Initialize circuit breaker if enabled
    if (this.useCircuitBreaker) {
      this.circuitBreaker = new CircuitBreaker({
        name: `publisher-${this.exchange}`,
        failureThreshold: options.circuitBreakerThreshold ?? DEFAULTS.CIRCUIT_BREAKER_THRESHOLD,
        resetTimeout: 30000,
        successThreshold: 3,
        logger: this.logger,
      });
    }
  }

  /**
   * Initialize the publisher - setup exchange
   */
  async initialize(): Promise<void> {
    if (this.isInitialized) return;

    try {
      // Get appropriate channel type
      if (this.confirm) {
        this.channel = await this.connection.getConfirmChannel();
      } else {
        this.channel = await this.connection.getChannel();
      }

      // Assert the exchange
      await this.channel.assertExchange(this.exchange, this.exchangeType, { durable: true });

      this.isInitialized = true;
      this.logger.info(
        { exchange: this.exchange, type: this.exchangeType, confirms: this.confirm },
        'Publisher initialized'
      );
    } catch (error) {
      this.logger.error({ err: error, exchange: this.exchange }, 'Failed to initialize publisher');
      throw error;
    }
  }

  /**
   * Execute single publish attempt
   */
  private async executePublishAttempt(
    routingKey: string,
    message: object,
    options: Required<Pick<PublishOptions, 'messageId' | 'correlationId'>> & PublishOptions
  ): Promise<void> {
    if (this.circuitBreaker) {
      await this.circuitBreaker.execute(() => this.doPublish(routingKey, message, options));
    } else {
      await this.doPublish(routingKey, message, options);
    }
  }

  /**
   * Handle publish retry logic
   */
  private async handlePublishRetry(
    routingKey: string,
    messageId: string,
    retries: number,
    lastError: Error
  ): Promise<boolean> {
    if (retries > this.maxRetries) return false;

    const delay = this.calculateRetryDelay(retries);
    this.logger.warn(
      {
        exchange: this.exchange,
        routingKey,
        messageId,
        retry: retries,
        maxRetries: this.maxRetries,
        delayMs: delay,
        error: lastError.message,
      },
      'Retrying publish after failure'
    );
    await this.sleep(delay);
    return true;
  }

  /**
   * Log publish failure after all retries exhausted
   */
  private logPublishFailure(
    routingKey: string,
    messageId: string,
    retries: number,
    error?: Error
  ): void {
    this.logger.error(
      {
        exchange: this.exchange,
        routingKey,
        messageId,
        retries,
        error: error?.message,
      },
      'Failed to publish message after all retries'
    );
  }

  /**
   * Handle circuit breaker error during publish
   */
  private handleCircuitBreakerError(
    error: CircuitBreakerOpenError,
    routingKey: string,
    messageId: string
  ): never {
    this.logger.warn(
      {
        exchange: this.exchange,
        routingKey,
        messageId,
        remainingResetTime: error.remainingResetTime,
      },
      'Publish blocked by circuit breaker'
    );
    throw error;
  }

  /**
   * Publish a message to the exchange
   */
  async publish(
    routingKey: string,
    message: object,
    options: PublishOptions = {}
  ): Promise<PublishResult> {
    const messageId = options.messageId ?? randomUUID();
    const correlationId = options.correlationId ?? randomUUID();
    const publishOptions = { ...options, messageId, correlationId };
    let retries = 0;
    let lastError: Error | undefined;

    while (retries <= this.maxRetries) {
      try {
        await this.executePublishAttempt(routingKey, message, publishOptions);
        if (retries > 0) {
          this.logger.info(
            { exchange: this.exchange, routingKey, messageId, retries },
            'Message published after retry'
          );
        }
        return { success: true, messageId, correlationId, retries };
      } catch (error) {
        lastError = error instanceof Error ? error : new Error(String(error));
        if (error instanceof CircuitBreakerOpenError) {
          this.handleCircuitBreakerError(error, routingKey, messageId);
        }
        retries++;
        if (!(await this.handlePublishRetry(routingKey, messageId, retries, lastError))) {
          break;
        }
      }
    }

    this.logPublishFailure(routingKey, messageId, retries, lastError);
    return { success: false, messageId, correlationId, retries, error: lastError };
  }

  /**
   * Publish a message and throw on failure (for critical messages)
   */
  async publishOrThrow(
    routingKey: string,
    message: object,
    options: PublishOptions = {}
  ): Promise<PublishResult> {
    const result = await this.publish(routingKey, message, options);
    if (!result.success) {
      throw result.error ?? new Error('Failed to publish message');
    }
    return result;
  }

  /**
   * Build publish options
   */
  private buildPublishOptions(
    options: Required<Pick<PublishOptions, 'messageId' | 'correlationId'>> & PublishOptions
  ): {
    messageId: string;
    correlationId: string;
    persistent: boolean;
    contentType: string;
    timestamp: number;
    priority: number | undefined;
    expiration: string | number | undefined;
    replyTo: string | undefined;
    headers: Record<string, unknown>;
  } {
    return {
      messageId: options.messageId,
      correlationId: options.correlationId,
      persistent: options.persistent ?? true,
      contentType: options.contentType ?? 'application/json',
      timestamp: Date.now(),
      priority: options.priority,
      expiration: options.expiration,
      replyTo: options.replyTo,
      headers: {
        ...options.headers,
        'x-published-at': new Date().toISOString(),
        'x-publisher': this.exchange,
      },
    };
  }

  /**
   * Publish with confirm channel (guaranteed delivery)
   */
  private async publishWithConfirm(
    routingKey: string,
    content: Buffer,
    publishOptions: object
  ): Promise<void> {
    await new Promise<void>((resolve, reject) => {
      (this.channel as ConfirmChannel).publish(
        this.exchange,
        routingKey,
        content,
        publishOptions,
        (err) => {
          if (err) {
            reject(err instanceof Error ? err : new Error(String(err)));
          } else {
            resolve();
          }
        }
      );
    });
  }

  /**
   * Publish without confirmation (fire and forget)
   */
  private async publishWithoutConfirm(
    routingKey: string,
    content: Buffer,
    publishOptions: object
  ): Promise<void> {
    if (!this.channel) {
      throw new Error('Channel not available for publishing');
    }
    const published = this.channel.publish(this.exchange, routingKey, content, publishOptions);
    if (!published) {
      await new Promise<void>((resolve) => {
        this.channel?.once('drain', resolve);
      });
    }
  }

  /**
   * Internal publish implementation
   */
  private async doPublish(
    routingKey: string,
    message: object,
    options: Required<Pick<PublishOptions, 'messageId' | 'correlationId'>> & PublishOptions
  ): Promise<void> {
    if (!this.isInitialized) {
      await this.initialize();
    }
    if (!this.channel) {
      throw new Error('Publisher not initialized');
    }

    const content = Buffer.from(JSON.stringify(message));
    const publishOptions = this.buildPublishOptions(options);

    if (this.confirm) {
      await this.publishWithConfirm(routingKey, content, publishOptions);
    } else {
      await this.publishWithoutConfirm(routingKey, content, publishOptions);
    }

    this.logger.debug(
      {
        exchange: this.exchange,
        routingKey,
        messageId: options.messageId,
        correlationId: options.correlationId,
        confirms: this.confirm,
      },
      'Message published'
    );
  }

  /**
   * Calculate retry delay with exponential backoff
   */
  private calculateRetryDelay(retryCount: number): number {
    const delay = Math.min(
      this.initialRetryDelay * Math.pow(2, retryCount - 1),
      this.maxRetryDelay
    );
    // Add jitter (0-25%)
    const jitter = delay * Math.random() * 0.25;
    return Math.floor(delay + jitter);
  }

  /**
   * Sleep for specified milliseconds
   */
  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  /**
   * Get publisher stats
   */
  getStats(): {
    exchange: string;
    isInitialized: boolean;
    confirm: boolean;
    circuitBreakerState?: string;
  } {
    return {
      exchange: this.exchange,
      isInitialized: this.isInitialized,
      confirm: this.confirm,
      circuitBreakerState: this.circuitBreaker?.getState(),
    };
  }

  /**
   * Reset circuit breaker (useful for testing or manual recovery)
   */
  resetCircuitBreaker(): void {
    this.circuitBreaker?.reset();
  }
}
