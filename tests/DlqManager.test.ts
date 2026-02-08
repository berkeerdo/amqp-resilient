import { describe, it, expect, beforeEach, vi } from 'vitest';
import { DlqManager } from '../src/health/DlqManager.js';
import { HealthService } from '../src/health/HealthService.js';
import type { ConnectionManager } from '../src/connection/ConnectionManager.js';

function createMockChannel(overrides: Record<string, unknown> = {}) {
  return {
    checkQueue: vi.fn().mockResolvedValue({ messageCount: 0 }),
    get: vi.fn().mockResolvedValue(false),
    ack: vi.fn(),
    nack: vi.fn(),
    publish: vi.fn().mockReturnValue(true),
    purgeQueue: vi.fn().mockResolvedValue({ messageCount: 0 }),
    close: vi.fn().mockResolvedValue(undefined),
    ...overrides,
  };
}

function createMockConnection(channel: ReturnType<typeof createMockChannel>) {
  return {
    getChannel: vi.fn().mockResolvedValue(channel),
    createChannel: vi.fn().mockResolvedValue(channel),
  } as unknown as ConnectionManager;
}

function createMockMessage(
  content: unknown,
  headers: Record<string, unknown> = {},
  properties: Record<string, unknown> = {}
) {
  const buffer = Buffer.from(typeof content === 'string' ? content : JSON.stringify(content));
  return {
    content: buffer,
    properties: {
      headers,
      messageId: 'msg-1',
      ...properties,
    },
    fields: {
      routingKey: 'test.key',
      exchange: 'test-exchange',
    },
  };
}

describe('DlqManager', () => {
  let channel: ReturnType<typeof createMockChannel>;
  let connection: ConnectionManager;
  let dlqManager: DlqManager;

  beforeEach(() => {
    HealthService.clear();
    channel = createMockChannel();
    connection = createMockConnection(channel);
    dlqManager = new DlqManager(connection);
  });

  describe('checkAllDepths', () => {
    it('should return depths for all registered DLQs', async () => {
      HealthService.registerDlq('orders.dlq', 'orders');
      HealthService.registerDlq('payments.dlq', 'payments');

      channel.checkQueue
        .mockResolvedValueOnce({ messageCount: 5 })
        .mockResolvedValueOnce({ messageCount: 12 });

      const results = await dlqManager.checkAllDepths();

      expect(results).toEqual([
        { dlqName: 'orders.dlq', originalQueue: 'orders', depth: 5 },
        { dlqName: 'payments.dlq', originalQueue: 'payments', depth: 12 },
      ]);
    });

    it('should return empty array when no DLQs registered', async () => {
      const results = await dlqManager.checkAllDepths();
      expect(results).toEqual([]);
    });

    it('should return depth 0 for DLQs that fail to check', async () => {
      HealthService.registerDlq('orders.dlq', 'orders');
      channel.checkQueue.mockRejectedValueOnce(new Error('Queue not found'));

      const results = await dlqManager.checkAllDepths();
      expect(results).toEqual([{ dlqName: 'orders.dlq', originalQueue: 'orders', depth: 0 }]);
    });
  });

  describe('checkDepth', () => {
    it('should return message count for a queue', async () => {
      channel.checkQueue.mockResolvedValueOnce({ messageCount: 7 });

      const depth = await dlqManager.checkDepth('orders.dlq');

      expect(depth).toBe(7);
      expect(channel.checkQueue).toHaveBeenCalledWith('orders.dlq');
    });
  });

  describe('peek', () => {
    it('should read messages and requeue them', async () => {
      const mockMsg = createMockMessage(
        { orderId: 123 },
        {
          'x-retry-count': 3,
          'x-original-routing-key': 'order.created',
          'x-first-failure-time': 1700000000000,
          'x-last-error': 'Connection timeout',
        }
      );

      HealthService.registerDlq('orders.dlq', 'orders');
      channel.get.mockResolvedValueOnce(mockMsg).mockResolvedValueOnce(false);

      const messages = await dlqManager.peek('orders.dlq', 5);

      expect(messages).toHaveLength(1);
      expect(messages[0]).toEqual({
        content: { orderId: 123 },
        headers: mockMsg.properties.headers,
        messageId: 'msg-1',
        originalQueue: 'orders',
        originalRoutingKey: 'order.created',
        retryCount: 3,
        firstFailureTime: 1700000000000,
        lastError: 'Connection timeout',
      });

      expect(channel.nack).toHaveBeenCalledWith(mockMsg, false, true);
      expect(channel.close).toHaveBeenCalled();
    });

    it('should default to 10 messages', async () => {
      channel.get.mockResolvedValue(false);

      await dlqManager.peek('orders.dlq');

      // get is called once and returns false immediately
      expect(channel.get).toHaveBeenCalledTimes(1);
    });

    it('should handle non-JSON content', async () => {
      const mockMsg = createMockMessage('plain text message');
      // Override content to be plain text
      mockMsg.content = Buffer.from('plain text message');

      HealthService.registerDlq('orders.dlq', 'orders');
      channel.get.mockResolvedValueOnce(mockMsg).mockResolvedValueOnce(false);

      const messages = await dlqManager.peek('orders.dlq');

      expect(messages[0]!.content).toBe('plain text message');
    });

    it('should infer original queue from DLQ name if not registered', async () => {
      const mockMsg = createMockMessage({ data: 1 });
      channel.get.mockResolvedValueOnce(mockMsg).mockResolvedValueOnce(false);

      const messages = await dlqManager.peek('some-queue.dlq');

      expect(messages[0]!.originalQueue).toBe('some-queue');
    });
  });

  describe('purge', () => {
    it('should purge queue and return count', async () => {
      channel.purgeQueue.mockResolvedValueOnce({ messageCount: 15 });

      const count = await dlqManager.purge('orders.dlq');

      expect(count).toBe(15);
      expect(channel.purgeQueue).toHaveBeenCalledWith('orders.dlq');
    });
  });

  describe('replay', () => {
    it('should replay messages to original exchange', async () => {
      const mockMsg = createMockMessage(
        { orderId: 1 },
        {
          'x-retry-count': 3,
          'x-original-exchange': 'orders-exchange',
          'x-original-routing-key': 'order.created',
          'x-first-failure-time': 1700000000000,
          'x-last-error': 'Timeout',
          'x-custom-header': 'keep-me',
        }
      );

      channel.get.mockResolvedValueOnce(mockMsg).mockResolvedValueOnce(false);

      const replayed = await dlqManager.replay('orders.dlq');

      expect(replayed).toBe(1);
      expect(channel.publish).toHaveBeenCalledWith(
        'orders-exchange',
        'order.created',
        mockMsg.content,
        expect.objectContaining({
          headers: { 'x-custom-header': 'keep-me' },
          persistent: true,
        })
      );
      expect(channel.ack).toHaveBeenCalledWith(mockMsg);
      expect(channel.close).toHaveBeenCalled();
    });

    it('should respect count limit', async () => {
      const msg1 = createMockMessage({ id: 1 }, { 'x-original-exchange': 'ex' });
      const msg2 = createMockMessage({ id: 2 }, { 'x-original-exchange': 'ex' });

      channel.get.mockResolvedValueOnce(msg1).mockResolvedValueOnce(msg2);

      const replayed = await dlqManager.replay('orders.dlq', 1);

      expect(replayed).toBe(1);
      expect(channel.ack).toHaveBeenCalledTimes(1);
    });

    it('should replay all messages when no count specified', async () => {
      const msg1 = createMockMessage({ id: 1 }, { 'x-original-exchange': 'ex' });
      const msg2 = createMockMessage({ id: 2 }, { 'x-original-exchange': 'ex' });

      channel.get
        .mockResolvedValueOnce(msg1)
        .mockResolvedValueOnce(msg2)
        .mockResolvedValueOnce(false);

      const replayed = await dlqManager.replay('orders.dlq');

      expect(replayed).toBe(2);
    });

    it('should use routing key from message fields as fallback', async () => {
      const mockMsg = createMockMessage({ id: 1 }, {});

      channel.get.mockResolvedValueOnce(mockMsg).mockResolvedValueOnce(false);

      await dlqManager.replay('orders.dlq');

      expect(channel.publish).toHaveBeenCalledWith(
        '',
        'test.key',
        mockMsg.content,
        expect.objectContaining({ persistent: true })
      );
    });
  });

  describe('getRegisteredDlqs', () => {
    it('should return registered DLQ mappings', () => {
      HealthService.registerDlq('orders.dlq', 'orders');
      HealthService.registerDlq('payments.dlq', 'payments');

      const dlqs = dlqManager.getRegisteredDlqs();

      expect(dlqs.get('orders.dlq')).toBe('orders');
      expect(dlqs.get('payments.dlq')).toBe('payments');
      expect(dlqs.size).toBe(2);
    });
  });

  describe('Consumer DLQ registration', () => {
    it('should register DLQ via HealthService', () => {
      HealthService.registerDlq('my-queue.dlq', 'my-queue');

      const dlqs = HealthService.getRegisteredDlqs();
      expect(dlqs.get('my-queue.dlq')).toBe('my-queue');
    });
  });
});
