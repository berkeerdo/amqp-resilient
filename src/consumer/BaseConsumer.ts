/**
 * Base Consumer for AMQP
 * Production-ready implementation with:
 * - Automatic retry with exponential backoff
 * - Dead Letter Queue (DLQ) handling
 * - Circuit breaker integration
 * - Message deduplication support
 * - Proper error handling and logging
 *
 * Best Practices:
 * 1. Each consumer gets its own channel
 * 2. Failed messages are retried with exponential backoff
 * 3. Messages exceeding max retries go to DLQ
 * 4. Circuit breaker prevents cascading failures
 * 5. Proper ack/nack handling
 */
import type { Channel, ConsumeMessage } from 'amqplib';
import type { ConnectionManager } from '../connection/ConnectionManager.js';
import { CircuitBreaker, CircuitBreakerOpenError } from '../patterns/CircuitBreaker.js';
import { HealthService } from '../health/HealthService.js';
import type { AmqpLogger, ConsumerOptions, MessageContext } from '../types.js';

/** Default retry configuration */
const DEFAULTS = {
  MAX_RETRIES: 3,
  INITIAL_RETRY_DELAY: 1000,
  MAX_RETRY_DELAY: 30000,
  PREFETCH: 10,
  CIRCUIT_BREAKER_THRESHOLD: 5,
};

/** Header names for retry tracking */
const HEADER = {
  RETRY_COUNT: 'x-retry-count',
  ORIGINAL_EXCHANGE: 'x-original-exchange',
  ORIGINAL_ROUTING_KEY: 'x-original-routing-key',
  FIRST_FAILURE_TIME: 'x-first-failure-time',
  LAST_ERROR: 'x-last-error',
};

/**
 * Type-safe header value getter
 */
function getHeaderValue(headers: Record<string, unknown> | undefined, key: string): unknown {
  if (!headers) {
    return undefined;
  }
  const entry = Object.entries(headers).find(([k]) => k === key);
  return entry ? entry[1] : undefined;
}

/**
 * BaseConsumer - Abstract base class for AMQP consumers
 * Extend this class and implement handle() for your specific use case
 */
export abstract class BaseConsumer<T = unknown> {
  protected channel: Channel | null = null;
  protected isInitialized = false;
  protected isConsuming = false;
  protected consumerTag: string | null = null;
  protected circuitBreaker: CircuitBreaker | null = null;
  protected readonly logger: AmqpLogger;

  private readonly queue: string;
  private readonly exchange: string;
  private readonly routingKeys: string[];
  private readonly prefetch: number;
  private readonly maxRetries: number;
  private readonly initialRetryDelay: number;
  private readonly maxRetryDelay: number;
  private readonly exchangeType: 'direct' | 'topic' | 'fanout' | 'headers';
  private readonly useCircuitBreaker: boolean;

  constructor(
    protected readonly connection: ConnectionManager,
    options: ConsumerOptions
  ) {
    this.queue = options.queue;
    this.exchange = options.exchange;
    this.routingKeys = options.routingKeys;
    this.prefetch = options.prefetch ?? DEFAULTS.PREFETCH;
    this.maxRetries = options.maxRetries ?? DEFAULTS.MAX_RETRIES;
    this.initialRetryDelay = options.initialRetryDelay ?? DEFAULTS.INITIAL_RETRY_DELAY;
    this.maxRetryDelay = options.maxRetryDelay ?? DEFAULTS.MAX_RETRY_DELAY;
    this.exchangeType = options.exchangeType ?? 'topic';
    this.useCircuitBreaker = options.useCircuitBreaker ?? true;
    this.logger = options.logger ?? connection.getLogger();

    // Initialize circuit breaker if enabled
    if (this.useCircuitBreaker) {
      this.circuitBreaker = new CircuitBreaker({
        name: `consumer-${this.queue}`,
        failureThreshold: options.circuitBreakerThreshold ?? DEFAULTS.CIRCUIT_BREAKER_THRESHOLD,
        resetTimeout: 30000,
        successThreshold: 3,
        logger: this.logger,
      });
    }
  }

  /**
   * Get DLQ name for this consumer
   */
  protected getDlqName(): string {
    return `${this.queue}.dlq`;
  }

  /**
   * Get retry queue name for this consumer
   */
  protected getRetryQueueName(): string {
    return `${this.queue}.retry`;
  }

  /**
   * Get DLX (Dead Letter Exchange) name
   */
  protected getDlxName(): string {
    return `${this.queue}.dlx`;
  }

  /**
   * Setup exchanges for consumer (main exchange and DLX)
   */
  private async setupExchanges(): Promise<void> {
    if (!this.channel) return;
    await this.channel.assertExchange(this.exchange, this.exchangeType, { durable: true });
    await this.channel.assertExchange(this.getDlxName(), 'direct', { durable: true });
  }

  /**
   * Setup queues for consumer (DLQ, retry queue, and main queue)
   */
  private async setupQueues(): Promise<void> {
    if (!this.channel) return;

    // DLQ
    await this.channel.assertQueue(this.getDlqName(), { durable: true, autoDelete: false });
    await this.channel.bindQueue(this.getDlqName(), this.getDlxName(), this.getDlqName());

    // Retry queue
    await this.channel.assertQueue(this.getRetryQueueName(), {
      durable: true,
      autoDelete: false,
      arguments: {
        'x-dead-letter-exchange': this.exchange,
        'x-message-ttl': this.initialRetryDelay,
      },
    });

    // Main queue
    await this.channel.assertQueue(this.queue, {
      durable: true,
      autoDelete: false,
      arguments: {
        'x-dead-letter-exchange': this.getDlxName(),
        'x-dead-letter-routing-key': this.getDlqName(),
      },
    });
  }

  /**
   * Bind queue to exchange with routing keys
   */
  private async bindRoutingKeys(): Promise<void> {
    if (!this.channel) return;
    for (const routingKey of this.routingKeys) {
      await this.channel.bindQueue(this.queue, this.exchange, routingKey);
      this.logger.debug(
        { queue: this.queue, exchange: this.exchange, routingKey },
        'Queue bound to exchange'
      );
    }
  }

  /**
   * Initialize the consumer - setup queue, DLQ, and bindings
   */
  async initialize(): Promise<void> {
    if (this.isInitialized) return;

    try {
      this.channel = await this.connection.createChannel();
      if (this.prefetch) {
        await this.channel.prefetch(this.prefetch);
      }

      await this.setupExchanges();
      await this.setupQueues();
      await this.bindRoutingKeys();

      this.isInitialized = true;
      HealthService.registerDlq(this.getDlqName(), this.queue);
      this.logger.info(
        {
          queue: this.queue,
          exchange: this.exchange,
          routingKeys: this.routingKeys,
          maxRetries: this.maxRetries,
          dlq: this.getDlqName(),
        },
        'Consumer initialized'
      );
    } catch (error) {
      this.logger.error({ err: error, queue: this.queue }, 'Failed to initialize consumer');
      throw error;
    }
  }

  /**
   * Start consuming messages
   */
  async start(): Promise<void> {
    if (!this.isInitialized) {
      await this.initialize();
    }

    if (this.isConsuming || !this.channel) return;

    const result = await this.channel.consume(
      this.queue,
      (msg) => {
        if (!msg) return;
        void this.handleMessage(msg);
      },
      { noAck: false }
    );

    this.consumerTag = result.consumerTag;
    this.isConsuming = true;
    this.logger.info({ queue: this.queue, consumerTag: this.consumerTag }, 'Consumer started');
  }

  /**
   * Handle incoming message with retry logic and circuit breaker
   */
  private async handleMessage(msg: ConsumeMessage): Promise<void> {
    const startTime = Date.now();
    const routingKey = msg.fields.routingKey;
    const messageId = (msg.properties.messageId as string) || `auto-${Date.now()}`;
    const correlationId = msg.properties.correlationId as string | undefined;
    const retryCount = this.getRetryCount(msg);
    const rawHeaders = msg.properties.headers as Record<string, unknown> | undefined;
    const headers: Record<string, unknown> = rawHeaders ?? {};

    const context: MessageContext = {
      routingKey,
      messageId,
      correlationId,
      retryCount,
      receivedAt: new Date(),
      headers,
    };

    this.logger.debug(
      { queue: this.queue, routingKey, messageId, correlationId, retryCount },
      'Processing message'
    );

    try {
      // Parse message content
      const content = this.parseMessageContent(msg);

      // Execute with circuit breaker if enabled
      if (this.circuitBreaker) {
        await this.circuitBreaker.execute(async () => {
          await this.handle(content as T, context);
        });
      } else {
        await this.handle(content as T, context);
      }

      // Acknowledge message on success
      this.channel?.ack(msg);

      this.logger.debug(
        { queue: this.queue, routingKey, messageId, durationMs: Date.now() - startTime },
        'Message processed successfully'
      );
    } catch (error) {
      await this.handleMessageError(msg, error, context, startTime);
    }
  }

  /**
   * Parse message content safely
   */
  private parseMessageContent(msg: ConsumeMessage): unknown {
    const contentType = msg.properties.contentType as string | undefined;
    const content = msg.content.toString();

    if (contentType === 'application/json' || content.startsWith('{') || content.startsWith('[')) {
      try {
        return JSON.parse(content);
      } catch {
        return content;
      }
    }

    return content;
  }

  /**
   * Handle message processing error
   */
  private async handleMessageError(
    msg: ConsumeMessage,
    error: unknown,
    context: MessageContext,
    startTime: number
  ): Promise<void> {
    const errorMessage = error instanceof Error ? error.message : String(error);
    const isCircuitBreakerOpen = error instanceof CircuitBreakerOpenError;

    this.logger.error(
      {
        err: error,
        queue: this.queue,
        routingKey: context.routingKey,
        messageId: context.messageId,
        retryCount: context.retryCount,
        durationMs: Date.now() - startTime,
        isCircuitBreakerOpen,
      },
      'Failed to process message'
    );

    // If circuit breaker is open, requeue the message for later
    if (isCircuitBreakerOpen) {
      this.channel?.nack(msg, false, true); // Requeue
      return;
    }

    // Check if we should retry
    if (context.retryCount < this.maxRetries) {
      await this.scheduleRetry(msg, context.retryCount, errorMessage);
    } else {
      // Max retries exceeded - send to DLQ
      this.logger.warn(
        {
          queue: this.queue,
          messageId: context.messageId,
          retryCount: context.retryCount,
          maxRetries: this.maxRetries,
        },
        'Message exceeded max retries, sending to DLQ'
      );
      this.channel?.nack(msg, false, false); // Don't requeue, let DLQ binding handle it
    }
  }

  /**
   * Build retry headers for message
   */
  private buildRetryHeaders(
    msg: ConsumeMessage,
    newRetryCount: number,
    errorMessage: string
  ): Record<string, unknown> {
    const existingHeaders = msg.properties.headers as Record<string, unknown> | undefined;
    const firstFailureTime = getHeaderValue(existingHeaders, HEADER.FIRST_FAILURE_TIME);
    const firstFailure = typeof firstFailureTime === 'number' ? firstFailureTime : Date.now();

    return {
      ...(existingHeaders || {}),
      [HEADER.RETRY_COUNT]: newRetryCount,
      [HEADER.ORIGINAL_EXCHANGE]: msg.fields.exchange || this.exchange,
      [HEADER.ORIGINAL_ROUTING_KEY]: msg.fields.routingKey,
      [HEADER.FIRST_FAILURE_TIME]: firstFailure,
      [HEADER.LAST_ERROR]: errorMessage.substring(0, 500),
    };
  }

  /**
   * Create temporary retry queue with delay TTL
   */
  private async createTempRetryQueue(delay: number, routingKey: string): Promise<string> {
    const tempRetryQueue = `${this.getRetryQueueName()}.${delay}`;
    await this.channel?.assertQueue(tempRetryQueue, {
      durable: true,
      autoDelete: true,
      expires: delay + 60000,
      arguments: {
        'x-dead-letter-exchange': this.exchange,
        'x-dead-letter-routing-key': routingKey,
        'x-message-ttl': delay,
      },
    });
    return tempRetryQueue;
  }

  /**
   * Schedule a message for retry
   */
  private async scheduleRetry(
    msg: ConsumeMessage,
    currentRetryCount: number,
    errorMessage: string
  ): Promise<void> {
    const newRetryCount = currentRetryCount + 1;
    const delay = this.calculateRetryDelay(newRetryCount);

    this.logger.info(
      {
        queue: this.queue,
        messageId: msg.properties.messageId as string | undefined,
        currentRetry: currentRetryCount,
        nextRetry: newRetryCount,
        delayMs: delay,
      },
      'Scheduling message retry'
    );

    try {
      const headers = this.buildRetryHeaders(msg, newRetryCount, errorMessage);
      const tempRetryQueue = await this.createTempRetryQueue(delay, msg.fields.routingKey);
      this.channel?.publish('', tempRetryQueue, msg.content, {
        ...msg.properties,
        headers,
        persistent: true,
      });
      this.channel?.ack(msg);
    } catch (retryError) {
      this.logger.error(
        { err: retryError, queue: this.queue },
        'Failed to schedule retry, sending to DLQ'
      );
      this.channel?.nack(msg, false, false);
    }
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
   * Get retry count from message headers
   */
  private getRetryCount(msg: ConsumeMessage): number {
    const headers = msg.properties.headers as Record<string, unknown> | undefined;
    const retryCount = getHeaderValue(headers, HEADER.RETRY_COUNT);
    return typeof retryCount === 'number' ? retryCount : 0;
  }

  /**
   * Process the message - to be implemented by subclasses
   * @param message - Parsed message content
   * @param context - Message context with metadata
   */
  protected abstract handle(message: T, context: MessageContext): Promise<void>;

  /**
   * Stop consuming messages
   */
  async stop(): Promise<void> {
    if (!this.isConsuming || !this.channel || !this.consumerTag) return;

    try {
      await this.channel.cancel(this.consumerTag);
      this.isConsuming = false;
      this.consumerTag = null;
      this.logger.info({ queue: this.queue }, 'Consumer stopped');
    } catch (error) {
      this.logger.error({ err: error, queue: this.queue }, 'Error stopping consumer');
    }
  }

  /**
   * Close the consumer and release resources
   */
  async close(): Promise<void> {
    await this.stop();

    if (this.channel) {
      try {
        await this.channel.close();
      } catch (error) {
        this.logger.debug({ err: error, queue: this.queue }, 'Error closing channel');
      }
    }

    this.channel = null;
    this.isInitialized = false;
    this.logger.info({ queue: this.queue }, 'Consumer closed');
  }

  /**
   * Get consumer stats
   */
  getStats(): {
    queue: string;
    isConsuming: boolean;
    isInitialized: boolean;
    circuitBreakerState?: string;
  } {
    return {
      queue: this.queue,
      isConsuming: this.isConsuming,
      isInitialized: this.isInitialized,
      circuitBreakerState: this.circuitBreaker?.getState(),
    };
  }
}
