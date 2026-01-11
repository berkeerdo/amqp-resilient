import { describe, it, expect } from 'vitest';
import { noopLogger, ConnectionStatus, CircuitState } from '../src/types.js';

describe('types', () => {
  describe('noopLogger', () => {
    it('should not throw when calling methods', () => {
      expect(() => noopLogger.info({ test: true })).not.toThrow();
      expect(() => noopLogger.warn({ test: true })).not.toThrow();
      expect(() => noopLogger.error({ test: true })).not.toThrow();
      expect(() => noopLogger.debug({ test: true })).not.toThrow();
    });

    it('should accept message parameter', () => {
      expect(() => noopLogger.info({ test: true }, 'message')).not.toThrow();
    });
  });

  describe('ConnectionStatus', () => {
    it('should have expected values', () => {
      expect(ConnectionStatus.DISCONNECTED).toBe('disconnected');
      expect(ConnectionStatus.CONNECTING).toBe('connecting');
      expect(ConnectionStatus.CONNECTED).toBe('connected');
      expect(ConnectionStatus.RECONNECTING).toBe('reconnecting');
      expect(ConnectionStatus.DEAD).toBe('dead');
    });
  });

  describe('CircuitState', () => {
    it('should have expected values', () => {
      expect(CircuitState.CLOSED).toBe('CLOSED');
      expect(CircuitState.OPEN).toBe('OPEN');
      expect(CircuitState.HALF_OPEN).toBe('HALF_OPEN');
    });
  });
});
