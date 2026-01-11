import { describe, it, expect, beforeEach } from 'vitest';
import { HealthService } from '../src/health/HealthService.js';
import { ConnectionStatus } from '../src/types.js';

describe('HealthService', () => {
  beforeEach(() => {
    HealthService.clear();
  });

  describe('registerStatus', () => {
    it('should register connection status', () => {
      HealthService.registerStatus('conn1', ConnectionStatus.CONNECTED);
      expect(HealthService.getStatus('conn1')).toBe(ConnectionStatus.CONNECTED);
    });

    it('should update existing connection status', () => {
      HealthService.registerStatus('conn1', ConnectionStatus.CONNECTING);
      HealthService.registerStatus('conn1', ConnectionStatus.CONNECTED);
      expect(HealthService.getStatus('conn1')).toBe(ConnectionStatus.CONNECTED);
    });
  });

  describe('getAllStatuses', () => {
    it('should return all statuses', () => {
      HealthService.registerStatus('conn1', ConnectionStatus.CONNECTED);
      HealthService.registerStatus('conn2', ConnectionStatus.RECONNECTING);

      const statuses = HealthService.getAllStatuses();
      expect(statuses).toEqual({
        conn1: 'connected',
        conn2: 'reconnecting',
      });
    });
  });

  describe('isHealthy', () => {
    it('should return true when no connections', () => {
      expect(HealthService.isHealthy()).toBe(true);
    });

    it('should return true when all connected', () => {
      HealthService.registerStatus('conn1', ConnectionStatus.CONNECTED);
      HealthService.registerStatus('conn2', ConnectionStatus.CONNECTED);
      expect(HealthService.isHealthy()).toBe(true);
    });

    it('should return true when reconnecting', () => {
      HealthService.registerStatus('conn1', ConnectionStatus.CONNECTED);
      HealthService.registerStatus('conn2', ConnectionStatus.RECONNECTING);
      expect(HealthService.isHealthy()).toBe(true);
    });

    it('should return false when disconnected', () => {
      HealthService.registerStatus('conn1', ConnectionStatus.DISCONNECTED);
      expect(HealthService.isHealthy()).toBe(false);
    });

    it('should return false when dead', () => {
      HealthService.registerStatus('conn1', ConnectionStatus.DEAD);
      expect(HealthService.isHealthy()).toBe(false);
    });
  });

  describe('getOverallStatus', () => {
    it('should return not_configured when empty', () => {
      expect(HealthService.getOverallStatus()).toBe('not_configured');
    });

    it('should return healthy when all connected', () => {
      HealthService.registerStatus('conn1', ConnectionStatus.CONNECTED);
      expect(HealthService.getOverallStatus()).toBe('healthy');
    });

    it('should return degraded when disconnected', () => {
      HealthService.registerStatus('conn1', ConnectionStatus.CONNECTED);
      HealthService.registerStatus('conn2', ConnectionStatus.DISCONNECTED);
      expect(HealthService.getOverallStatus()).toBe('degraded');
    });

    it('should return dead when any dead', () => {
      HealthService.registerStatus('conn1', ConnectionStatus.CONNECTED);
      HealthService.registerStatus('conn2', ConnectionStatus.DEAD);
      expect(HealthService.getOverallStatus()).toBe('dead');
    });
  });

  describe('unregisterConnection', () => {
    it('should remove connection', () => {
      HealthService.registerStatus('conn1', ConnectionStatus.CONNECTED);
      HealthService.unregisterConnection('conn1');
      expect(HealthService.getStatus('conn1')).toBeUndefined();
    });
  });
});
