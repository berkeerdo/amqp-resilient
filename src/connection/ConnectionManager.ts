/**
 * AMQP Connection Manager
 * Production-ready implementation with:
 * - Automatic reconnection with exponential backoff and jitter
 * - Connection heartbeat for health monitoring
 * - Dedicated channels for consumers (one channel per consumer)
 * - Confirm channels for publishers (guaranteed delivery)
 * - Graceful shutdown handling
 *
 * Best Practices:
 * 1. Use separate channels for publishing and consuming
 * 2. Never share channels between consumers
 * 3. Use confirm channels for critical messages
 * 4. Implement proper error handling and reconnection
 * 5. Use heartbeats to detect dead connections
 */
import amqplib from 'amqplib';
import type { Channel, ConfirmChannel } from 'amqplib';
import { ConnectionStatus, noopLogger, type AmqpLogger, type ConnectionOptions } from '../types.js';
import { HealthService } from '../health/HealthService.js';

// amqplib connection type (from connect return type)
type AmqpConnection = Awaited<ReturnType<typeof amqplib.connect>>;

/** Default configuration values */
const DEFAULTS = {
  INITIAL_RECONNECT_DELAY: 1000,
  MAX_RECONNECT_DELAY: 60000,
  MAX_RECONNECT_ATTEMPTS: 0, // unlimited
  HEARTBEAT_SECONDS: 60,
  PREFETCH: 10,
  MANAGEMENT_PORT: 15672,
};

/**
 * ConnectionManager - Manages AMQP connection with auto-reconnect
 * Use one instance per logical connection purpose
 */
export class ConnectionManager {
  private connection: AmqpConnection | null = null;
  private sharedChannel: Channel | null = null;
  private confirmChannel: ConfirmChannel | null = null;
  private reconnectAttempts = 0;
  private isShuttingDown = false;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private readonly createdChannels = new Set<Channel>();
  private readonly logger: AmqpLogger;

  private readonly connectionUrl: string;
  private readonly connectionName: string;
  private readonly prefetch: number;
  private readonly heartbeat: number;
  private readonly maxReconnectAttempts: number;
  private readonly initialReconnectDelay: number;
  private readonly maxReconnectDelay: number;

  // Connection params for vhost management
  private readonly host?: string;
  private readonly managementPort: number;
  private readonly username: string;
  private readonly password: string;
  private readonly vhost: string;
  private readonly autoCreateVhost: boolean;

  constructor(options: ConnectionOptions) {
    this.connectionName = options.connectionName ?? 'default';
    this.prefetch = options.prefetch ?? DEFAULTS.PREFETCH;
    this.heartbeat = options.heartbeat ?? DEFAULTS.HEARTBEAT_SECONDS;
    this.maxReconnectAttempts = options.maxReconnectAttempts ?? DEFAULTS.MAX_RECONNECT_ATTEMPTS;
    this.initialReconnectDelay = options.initialReconnectDelay ?? DEFAULTS.INITIAL_RECONNECT_DELAY;
    this.maxReconnectDelay = options.maxReconnectDelay ?? DEFAULTS.MAX_RECONNECT_DELAY;
    this.logger = options.logger ?? noopLogger;

    // Store connection params for vhost management
    this.host = options.host;
    this.managementPort = options.managementPort ?? DEFAULTS.MANAGEMENT_PORT;
    this.username = options.username ?? 'guest';
    this.password = options.password ?? 'guest';
    this.vhost = options.vhost ?? '/';
    this.autoCreateVhost = options.autoCreateVhost ?? false;

    // Build connection URL from individual params or use provided URL
    if (options.url) {
      this.connectionUrl = options.url;
    } else if (options.host) {
      const username = encodeURIComponent(this.username);
      const password = encodeURIComponent(this.password);
      const host = options.host;
      const port = options.port ?? 5672;
      const vhost = encodeURIComponent(this.vhost);
      this.connectionUrl = `amqp://${username}:${password}@${host}:${port}/${vhost}`;
    } else {
      throw new Error('ConnectionManager requires either url or host parameter');
    }
  }

  /**
   * Setup connection event handlers
   */
  private setupConnectionHandlers(): void {
    if (!this.connection) {
      return;
    }

    this.connection.on('error', (err: Error) => {
      this.logger.error({ err, connectionName: this.connectionName }, 'AMQP connection error');
    });

    this.connection.on('close', () => {
      if (this.isShuttingDown) {
        return;
      }
      this.logger.warn(
        { connectionName: this.connectionName },
        'AMQP connection closed unexpectedly, scheduling reconnect...'
      );
      this.resetConnectionState();
      HealthService.registerStatus(this.connectionName, ConnectionStatus.RECONNECTING);
      this.scheduleReconnect();
    });

    this.connection.on('blocked', (reason: string) => {
      this.logger.warn(
        { connectionName: this.connectionName, reason },
        'AMQP connection blocked by broker'
      );
    });

    this.connection.on('unblocked', () => {
      this.logger.info({ connectionName: this.connectionName }, 'AMQP connection unblocked');
    });
  }

  /**
   * Reset connection state on disconnect
   */
  private resetConnectionState(): void {
    this.connection = null;
    this.sharedChannel = null;
    this.confirmChannel = null;
    this.createdChannels.clear();
  }

  /**
   * Ensure vhost exists (create if not)
   * Uses RabbitMQ Management HTTP API
   */
  private async ensureVhost(): Promise<void> {
    if (!this.autoCreateVhost || !this.host || this.vhost === '/') {
      return;
    }

    const encodedVhost = encodeURIComponent(this.vhost);
    const baseUrl = `http://${this.host}:${this.managementPort}/api`;
    const authHeader = `Basic ${Buffer.from(`${this.username}:${this.password}`).toString('base64')}`;

    try {
      // Check if vhost exists
      const checkResponse = await fetch(`${baseUrl}/vhosts/${encodedVhost}`, {
        method: 'GET',
        headers: { Authorization: authHeader },
      });

      if (checkResponse.status === 200) {
        this.logger.debug({ vhost: this.vhost }, 'Vhost already exists');
        return;
      }

      if (checkResponse.status !== 404) {
        this.logger.warn(
          { vhost: this.vhost, status: checkResponse.status },
          'Unexpected response checking vhost'
        );
        return;
      }

      // Create vhost
      this.logger.info({ vhost: this.vhost }, 'Creating vhost...');
      const createResponse = await fetch(`${baseUrl}/vhosts/${encodedVhost}`, {
        method: 'PUT',
        headers: {
          Authorization: authHeader,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ description: `Auto-created by ${this.connectionName}` }),
      });

      if (!createResponse.ok) {
        throw new Error(
          `Failed to create vhost: ${createResponse.status} ${createResponse.statusText}`
        );
      }

      // Set permissions for the user
      const encodedUsername = encodeURIComponent(this.username);
      const permResponse = await fetch(
        `${baseUrl}/permissions/${encodedVhost}/${encodedUsername}`,
        {
          method: 'PUT',
          headers: {
            Authorization: authHeader,
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({ configure: '.*', write: '.*', read: '.*' }),
        }
      );

      if (!permResponse.ok) {
        throw new Error(
          `Failed to set vhost permissions: ${permResponse.status} ${permResponse.statusText}`
        );
      }

      this.logger.info({ vhost: this.vhost }, 'Vhost created and permissions set');
    } catch (error) {
      this.logger.error({ err: error, vhost: this.vhost }, 'Failed to ensure vhost exists');
      // Don't throw - let the connection attempt proceed and fail with a clearer error
    }
  }

  /**
   * Connect to AMQP server
   */
  async connect(): Promise<void> {
    if (this.connection) {
      return;
    }

    HealthService.registerStatus(this.connectionName, ConnectionStatus.CONNECTING);

    try {
      // Ensure vhost exists before connecting
      await this.ensureVhost();

      this.logger.info({ connectionName: this.connectionName }, 'Connecting to AMQP server...');
      this.connection = await amqplib.connect(this.connectionUrl, { heartbeat: this.heartbeat });
      this.reconnectAttempts = 0;
      this.setupConnectionHandlers();
      this.sharedChannel = await this.createManagedChannel();
      HealthService.registerStatus(this.connectionName, ConnectionStatus.CONNECTED);
      this.logger.info({ connectionName: this.connectionName }, 'AMQP connected successfully');
    } catch (error) {
      this.logger.error(
        { err: error, connectionName: this.connectionName },
        'Failed to connect to AMQP server'
      );
      HealthService.registerStatus(this.connectionName, ConnectionStatus.DISCONNECTED);
      this.scheduleReconnect();
      throw error;
    }
  }

  /**
   * Create a managed channel with error handlers
   */
  private async createManagedChannel(): Promise<Channel> {
    if (!this.connection) {
      throw new Error('Not connected to AMQP server');
    }

    const channel = await this.connection.createChannel();
    await channel.prefetch(this.prefetch);

    channel.on('error', (err: Error) => {
      this.logger.error({ err, connectionName: this.connectionName }, 'AMQP channel error');
      this.createdChannels.delete(channel);
    });

    channel.on('close', () => {
      if (!this.isShuttingDown) {
        this.logger.warn({ connectionName: this.connectionName }, 'AMQP channel closed');
      }
      this.createdChannels.delete(channel);
    });

    this.createdChannels.add(channel);
    return channel;
  }

  /**
   * Schedule reconnection with exponential backoff and jitter
   */
  private scheduleReconnect(): void {
    if (this.isShuttingDown || this.reconnectTimer) {
      return;
    }

    this.reconnectAttempts++;

    // Check max attempts (0 = unlimited)
    if (this.maxReconnectAttempts > 0 && this.reconnectAttempts > this.maxReconnectAttempts) {
      this.logger.error(
        { connectionName: this.connectionName, attempts: this.reconnectAttempts },
        'Max reconnection attempts reached, marking connection as dead'
      );
      HealthService.registerStatus(this.connectionName, ConnectionStatus.DEAD);
      return;
    }

    // Calculate delay with exponential backoff and jitter
    const exponentialDelay = Math.min(
      this.initialReconnectDelay * Math.pow(2, this.reconnectAttempts - 1),
      this.maxReconnectDelay
    );
    // Add jitter (0-25% of delay) to prevent thundering herd
    const jitter = Math.random() * exponentialDelay * 0.25;
    const delay = Math.floor(exponentialDelay + jitter);

    this.logger.info(
      {
        connectionName: this.connectionName,
        attempt: this.reconnectAttempts,
        delayMs: delay,
      },
      'Scheduling AMQP reconnection...'
    );

    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.connect().catch(() => {
        // Error already logged in connect()
      });
    }, delay);
  }

  /**
   * Get the shared channel (for simple operations)
   * Note: For consumers, use createChannel() instead
   */
  async getChannel(): Promise<Channel> {
    if (!this.sharedChannel) {
      await this.connect();
    }
    if (!this.sharedChannel) {
      throw new Error('Failed to get AMQP channel');
    }
    return this.sharedChannel;
  }

  /**
   * Create a dedicated channel for a consumer
   * Best Practice: Each consumer should have its own channel
   */
  async createChannel(): Promise<Channel> {
    if (!this.connection) {
      await this.connect();
    }
    if (!this.connection) {
      throw new Error('Failed to get AMQP connection');
    }

    const channel = await this.createManagedChannel();
    this.logger.debug(
      { connectionName: this.connectionName, channelCount: this.createdChannels.size },
      'Created dedicated channel'
    );

    return channel;
  }

  /**
   * Get confirm channel for guaranteed delivery
   */
  async getConfirmChannel(): Promise<ConfirmChannel> {
    if (!this.confirmChannel) {
      if (!this.connection) {
        await this.connect();
      }
      if (!this.connection) {
        throw new Error('Failed to get AMQP connection');
      }

      this.confirmChannel = await this.connection.createConfirmChannel();
      await this.confirmChannel.prefetch(this.prefetch);

      this.confirmChannel.on('error', (err: Error) => {
        this.logger.error(
          { err, connectionName: this.connectionName },
          'AMQP confirm channel error'
        );
        this.confirmChannel = null;
      });

      this.confirmChannel.on('close', () => {
        if (!this.isShuttingDown) {
          this.logger.warn({ connectionName: this.connectionName }, 'AMQP confirm channel closed');
        }
        this.confirmChannel = null;
      });

      this.logger.debug({ connectionName: this.connectionName }, 'Created confirm channel');
    }

    return this.confirmChannel;
  }

  /**
   * Close connection gracefully
   */
  async close(): Promise<void> {
    this.isShuttingDown = true;

    // Cancel reconnection timer
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }

    try {
      // Close all created channels
      const closePromises: Promise<void>[] = [];

      for (const channel of this.createdChannels) {
        closePromises.push(
          channel.close().catch((err: unknown) => {
            this.logger.debug({ err }, 'Error closing channel');
          })
        );
      }

      if (this.sharedChannel && !this.createdChannels.has(this.sharedChannel)) {
        closePromises.push(
          this.sharedChannel.close().catch((err: unknown) => {
            this.logger.debug({ err }, 'Error closing shared channel');
          })
        );
      }

      if (this.confirmChannel) {
        closePromises.push(
          this.confirmChannel.close().catch((err: unknown) => {
            this.logger.debug({ err }, 'Error closing confirm channel');
          })
        );
      }

      await Promise.all(closePromises);

      if (this.connection) {
        await this.connection.close();
      }

      this.connection = null;
      this.sharedChannel = null;
      this.confirmChannel = null;
      this.createdChannels.clear();

      HealthService.unregisterConnection(this.connectionName);
      this.logger.info(
        { connectionName: this.connectionName },
        'AMQP connection closed gracefully'
      );
    } catch (error) {
      this.logger.error(
        { err: error, connectionName: this.connectionName },
        'Error closing AMQP connection'
      );
    }
  }

  /**
   * Check if connected
   */
  isConnected(): boolean {
    return this.connection !== null;
  }

  /**
   * Get connection name
   */
  getConnectionName(): string {
    return this.connectionName;
  }

  /**
   * Get logger instance (for consumers/publishers to inherit)
   */
  getLogger(): AmqpLogger {
    return this.logger;
  }

  /**
   * Get connection stats
   */
  getStats(): {
    connected: boolean;
    reconnectAttempts: number;
    channelCount: number;
    hasConfirmChannel: boolean;
  } {
    return {
      connected: this.isConnected(),
      reconnectAttempts: this.reconnectAttempts,
      channelCount: this.createdChannels.size,
      hasConfirmChannel: this.confirmChannel !== null,
    };
  }
}
