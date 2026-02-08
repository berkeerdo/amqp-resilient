/**
 * DLQ Manager for inspecting, replaying, and purging Dead Letter Queues
 * Works with DLQs automatically created by BaseConsumer
 */
import type { ConnectionManager } from '../connection/ConnectionManager.js';
import { HealthService } from './HealthService.js';

export interface DlqMessageInfo {
  content: unknown;
  headers: Record<string, unknown>;
  messageId?: string;
  originalQueue: string;
  originalRoutingKey?: string;
  retryCount: number;
  firstFailureTime?: number;
  lastError?: string;
}

export interface DlqDepthInfo {
  dlqName: string;
  originalQueue: string;
  depth: number;
}

export class DlqManager {
  constructor(private readonly connection: ConnectionManager) {}

  /**
   * Check depth of all registered DLQs
   */
  async checkAllDepths(): Promise<DlqDepthInfo[]> {
    const dlqs = HealthService.getRegisteredDlqs();
    const results: DlqDepthInfo[] = [];

    const channel = await this.connection.getChannel();

    for (const [dlqName, originalQueue] of dlqs) {
      try {
        const info = await channel.checkQueue(dlqName);
        results.push({
          dlqName,
          originalQueue,
          depth: info.messageCount,
        });
      } catch {
        results.push({
          dlqName,
          originalQueue,
          depth: 0,
        });
      }
    }

    return results;
  }

  /**
   * Check depth of a single DLQ
   */
  async checkDepth(dlqName: string): Promise<number> {
    const channel = await this.connection.getChannel();
    const info = await channel.checkQueue(dlqName);
    return info.messageCount;
  }

  /**
   * Peek at messages in a DLQ without consuming them
   * Messages are read and then requeued (nack with requeue=true)
   */
  async peek(dlqName: string, count = 10): Promise<DlqMessageInfo[]> {
    const dlqs = HealthService.getRegisteredDlqs();
    const originalQueue = dlqs.get(dlqName) ?? dlqName.replace(/\.dlq$/, '');
    const messages: DlqMessageInfo[] = [];

    const channel = await this.connection.createChannel();

    try {
      for (let i = 0; i < count; i++) {
        const msg = await channel.get(dlqName, { noAck: false });
        if (!msg) break;

        const rawHeaders = msg.properties.headers as Record<string, unknown> | undefined;
        const headers: Record<string, unknown> = rawHeaders ?? {};
        const content = this.parseContent(msg.content);

        messages.push({
          content,
          headers,
          messageId: msg.properties.messageId as string | undefined,
          originalQueue,
          originalRoutingKey: headers['x-original-routing-key'] as string | undefined,
          retryCount: typeof headers['x-retry-count'] === 'number' ? headers['x-retry-count'] : 0,
          firstFailureTime:
            typeof headers['x-first-failure-time'] === 'number'
              ? headers['x-first-failure-time']
              : undefined,
          lastError:
            typeof headers['x-last-error'] === 'string' ? headers['x-last-error'] : undefined,
        });

        channel.nack(msg, false, true);
      }
    } finally {
      await channel.close();
    }

    return messages;
  }

  /**
   * Purge all messages from a DLQ
   * Returns the number of purged messages
   */
  async purge(dlqName: string): Promise<number> {
    const channel = await this.connection.getChannel();
    const result = await channel.purgeQueue(dlqName);
    return result.messageCount;
  }

  /**
   * Replay messages from DLQ back to their original exchange
   * Messages are consumed from DLQ and republished to the original exchange
   * with the original routing key
   */
  async replay(dlqName: string, count?: number): Promise<number> {
    const channel = await this.connection.createChannel();
    let replayed = 0;

    try {
      const limit = count ?? Infinity;

      while (replayed < limit) {
        const msg = await channel.get(dlqName, { noAck: false });
        if (!msg) break;

        const rawHeaders = msg.properties.headers as Record<string, unknown> | undefined;
        const headers: Record<string, unknown> = rawHeaders ?? {};
        const originalExchange = (headers['x-original-exchange'] as string) || '';
        const originalRoutingKey =
          (headers['x-original-routing-key'] as string) || msg.fields.routingKey;

        // Remove retry-related headers for a clean retry
        const cleanHeaders = { ...headers };
        delete cleanHeaders['x-retry-count'];
        delete cleanHeaders['x-first-failure-time'];
        delete cleanHeaders['x-last-error'];
        delete cleanHeaders['x-original-exchange'];
        delete cleanHeaders['x-original-routing-key'];

        channel.publish(originalExchange, originalRoutingKey, msg.content, {
          ...msg.properties,
          headers: cleanHeaders,
          persistent: true,
        });

        channel.ack(msg);
        replayed++;
      }
    } finally {
      await channel.close();
    }

    return replayed;
  }

  /**
   * Get registered DLQ mappings
   */
  getRegisteredDlqs(): Map<string, string> {
    return HealthService.getRegisteredDlqs();
  }

  private parseContent(buffer: Buffer): unknown {
    const str = buffer.toString();
    try {
      return JSON.parse(str);
    } catch {
      return str;
    }
  }
}
