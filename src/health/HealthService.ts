/**
 * Health Service for tracking connection health
 * Provides global status monitoring for all AMQP connections
 */
import { ConnectionStatus } from '../types.js';

/**
 * HealthService - Singleton for tracking queue connection health
 */
class HealthServiceClass {
  private connectionStatuses = new Map<string, ConnectionStatus>();
  private registeredDlqs = new Map<string, string>(); // dlqName → originalQueue

  /**
   * Register or update a connection's status
   */
  registerStatus(connectionName: string, status: ConnectionStatus): void {
    this.connectionStatuses.set(connectionName, status);
  }

  /**
   * Get status of a specific connection
   */
  getStatus(connectionName: string): ConnectionStatus | undefined {
    return this.connectionStatuses.get(connectionName);
  }

  /**
   * Get all connection statuses
   */
  getAllStatuses(): Record<string, string> {
    return Object.fromEntries(this.connectionStatuses);
  }

  /**
   * Check if any connections are dead
   */
  hasDeadConnections(): boolean {
    for (const status of this.connectionStatuses.values()) {
      if (status === ConnectionStatus.DEAD) {
        return true;
      }
    }
    return false;
  }

  /**
   * Check if all connections are healthy
   */
  isHealthy(): boolean {
    if (this.connectionStatuses.size === 0) {
      return true; // No connections configured
    }

    for (const status of this.connectionStatuses.values()) {
      if (status === ConnectionStatus.DEAD || status === ConnectionStatus.DISCONNECTED) {
        return false;
      }
    }
    return true;
  }

  /**
   * Get overall status for health endpoint
   */
  getOverallStatus(): 'healthy' | 'degraded' | 'dead' | 'not_configured' {
    if (this.connectionStatuses.size === 0) {
      return 'not_configured';
    }

    if (this.hasDeadConnections()) {
      return 'dead';
    }

    for (const status of this.connectionStatuses.values()) {
      if (status === ConnectionStatus.DISCONNECTED) {
        return 'degraded';
      }
    }

    return 'healthy';
  }

  /**
   * Unregister a connection
   */
  unregisterConnection(connectionName: string): void {
    this.connectionStatuses.delete(connectionName);
  }

  /**
   * Register a DLQ and its original queue mapping
   */
  registerDlq(dlqName: string, originalQueue: string): void {
    this.registeredDlqs.set(dlqName, originalQueue);
  }

  /**
   * Get all registered DLQ mappings
   */
  getRegisteredDlqs(): Map<string, string> {
    return new Map(this.registeredDlqs);
  }

  /**
   * Clear all connections and DLQ registrations (useful for testing)
   */
  clear(): void {
    this.connectionStatuses.clear();
    this.registeredDlqs.clear();
  }
}

export const HealthService = new HealthServiceClass();
