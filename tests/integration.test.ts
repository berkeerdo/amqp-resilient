/**
 * Integration tests for amqp-resilient
 * Requires RabbitMQ running on localhost:5672
 *
 * Run with: npm run test:integration
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import {
  ConnectionManager,
  BaseConsumer,
  BasePublisher,
  HealthService,
  ConnectionStatus,
  type MessageContext,
} from '../src/index.js';

const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://localhost:5672';

// Test message type
interface TestMessage {
  id: string;
  data: string;
  timestamp: number;
}

// Test consumer implementation
class TestConsumer extends BaseConsumer<TestMessage> {
  public receivedMessages: TestMessage[] = [];
  public processedCount = 0;

  constructor(connection: ConnectionManager) {
    super(connection, {
      queue: 'amqp-resilient.test.queue',
      exchange: 'amqp-resilient.test.exchange',
      routingKeys: ['test.message'],
      prefetch: 5,
      maxRetries: 2,
    });
  }

  protected async handle(message: TestMessage, context: MessageContext): Promise<void> {
    this.receivedMessages.push(message);
    this.processedCount++;
    console.log(`Received message: ${message.id}`, context.routingKey);
  }
}

// Test publisher implementation
class TestPublisher extends BasePublisher {
  constructor(connection: ConnectionManager) {
    super(connection, {
      exchange: 'amqp-resilient.test.exchange',
      confirm: true,
    });
  }
}

describe('Integration Tests', () => {
  let connection: ConnectionManager;
  let consumer: TestConsumer;
  let publisher: TestPublisher;

  beforeAll(async () => {
    // Create connection
    connection = new ConnectionManager({
      url: RABBITMQ_URL,
      connectionName: 'integration-test',
      prefetch: 10,
    });

    // Connect
    await connection.connect();

    // Create consumer and publisher
    consumer = new TestConsumer(connection);
    publisher = new TestPublisher(connection);

    // Initialize and start consumer
    await consumer.start();

    // Wait for consumer to be ready
    await new Promise((resolve) => setTimeout(resolve, 500));
  }, 30000);

  afterAll(async () => {
    // Stop consumer
    await consumer.close();

    // Close connection
    await connection.close();
  }, 10000);

  describe('ConnectionManager', () => {
    it('should connect to RabbitMQ', () => {
      expect(connection.isConnected()).toBe(true);
    });

    it('should register with HealthService', () => {
      const status = HealthService.getStatus('integration-test');
      expect(status).toBe(ConnectionStatus.CONNECTED);
    });

    it('should report healthy overall status', () => {
      const overall = HealthService.getOverallStatus();
      expect(overall).toBe('healthy');
    });

    it('should have correct stats', () => {
      const stats = connection.getStats();
      expect(stats.connected).toBe(true);
      expect(stats.reconnectAttempts).toBe(0);
      expect(stats.channelCount).toBeGreaterThan(0);
    });
  });

  describe('Publisher', () => {
    it('should publish a message successfully', async () => {
      const message: TestMessage = {
        id: 'test-1',
        data: 'Hello World',
        timestamp: Date.now(),
      };

      const result = await publisher.publish('test.message', message);

      expect(result.success).toBe(true);
      expect(result.messageId).toBeDefined();
      expect(result.correlationId).toBeDefined();
      expect(result.retries).toBe(0);
    });

    it('should publish multiple messages', async () => {
      const messages = Array.from({ length: 5 }, (_, i) => ({
        id: `batch-${i}`,
        data: `Message ${i}`,
        timestamp: Date.now(),
      }));

      const results = await Promise.all(
        messages.map((msg) => publisher.publish('test.message', msg))
      );

      expect(results.every((r) => r.success)).toBe(true);
    });

    it('should have correct stats', () => {
      const stats = publisher.getStats();
      expect(stats.exchange).toBe('amqp-resilient.test.exchange');
      expect(stats.isInitialized).toBe(true);
      expect(stats.confirm).toBe(true);
    });
  });

  describe('Consumer', () => {
    it('should receive published messages', async () => {
      // Clear previous messages
      consumer.receivedMessages = [];
      consumer.processedCount = 0;

      // Publish a new message
      const message: TestMessage = {
        id: 'consumer-test-1',
        data: 'Test for consumer',
        timestamp: Date.now(),
      };

      await publisher.publish('test.message', message);

      // Wait for message to be consumed
      await new Promise((resolve) => setTimeout(resolve, 1000));

      // Check if message was received
      const received = consumer.receivedMessages.find((m) => m.id === 'consumer-test-1');
      expect(received).toBeDefined();
      expect(received?.data).toBe('Test for consumer');
    });

    it('should have correct stats', () => {
      const stats = consumer.getStats();
      expect(stats.queue).toBe('amqp-resilient.test.queue');
      expect(stats.isConsuming).toBe(true);
      expect(stats.isInitialized).toBe(true);
    });
  });

  describe('End-to-End Flow', () => {
    it('should handle publish-consume cycle', async () => {
      const initialCount = consumer.processedCount;

      // Publish 10 messages
      const messages = Array.from({ length: 10 }, (_, i) => ({
        id: `e2e-${i}-${Date.now()}`,
        data: `E2E Message ${i}`,
        timestamp: Date.now(),
      }));

      await Promise.all(messages.map((msg) => publisher.publish('test.message', msg)));

      // Wait for all messages to be consumed
      await new Promise((resolve) => setTimeout(resolve, 2000));

      // Verify all messages were processed
      expect(consumer.processedCount).toBeGreaterThanOrEqual(initialCount + 10);
    });
  });
});
