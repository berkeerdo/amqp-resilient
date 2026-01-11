/**
 * Circuit Breaker Pattern
 * Prevents cascading failures by temporarily stopping operations when errors exceed threshold
 *
 * States:
 * - CLOSED: Normal operation, all requests pass through
 * - OPEN: Failure threshold exceeded, all requests fail immediately
 * - HALF_OPEN: Testing if service has recovered (allows limited requests)
 */
import { CircuitState, noopLogger, type AmqpLogger, type CircuitBreakerOptions } from '../types.js';

interface FailureRecord {
  timestamp: number;
  error: Error;
}

/**
 * Error thrown when circuit breaker is open
 */
export class CircuitBreakerOpenError extends Error {
  readonly remainingResetTime: number;

  constructor(message: string, remainingResetTime: number) {
    super(message);
    this.name = 'CircuitBreakerOpenError';
    this.remainingResetTime = remainingResetTime;
  }
}

/**
 * CircuitBreaker - Implements the circuit breaker pattern
 */
export class CircuitBreaker {
  private state: CircuitState = CircuitState.CLOSED;
  private failures: FailureRecord[] = [];
  private successCount = 0;
  private lastFailureTime = 0;
  private readonly logger: AmqpLogger;

  private readonly failureThreshold: number;
  private readonly resetTimeout: number;
  private readonly successThreshold: number;
  private readonly failureWindow: number;
  private readonly name: string;

  constructor(options: CircuitBreakerOptions) {
    this.name = options.name;
    this.failureThreshold = options.failureThreshold ?? 5;
    this.resetTimeout = options.resetTimeout ?? 30000;
    this.successThreshold = options.successThreshold ?? 3;
    this.failureWindow = options.failureWindow ?? 60000;
    this.logger = options.logger ?? noopLogger;
  }

  /**
   * Execute a function with circuit breaker protection
   */
  async execute<T>(fn: () => Promise<T>): Promise<T> {
    // Check if circuit should transition from OPEN to HALF_OPEN
    if (this.state === CircuitState.OPEN) {
      if (Date.now() - this.lastFailureTime >= this.resetTimeout) {
        this.transitionTo(CircuitState.HALF_OPEN);
      } else {
        throw new CircuitBreakerOpenError(
          `Circuit breaker is open for ${this.name}`,
          this.getRemainingResetTime()
        );
      }
    }

    try {
      const result = await fn();
      this.onSuccess();
      return result;
    } catch (error) {
      this.onFailure(error instanceof Error ? error : new Error(String(error)));
      throw error;
    }
  }

  /**
   * Record a success
   */
  private onSuccess(): void {
    if (this.state === CircuitState.HALF_OPEN) {
      this.successCount++;
      if (this.successCount >= this.successThreshold) {
        this.transitionTo(CircuitState.CLOSED);
      }
    } else if (this.state === CircuitState.CLOSED) {
      // Clear old failures outside the window
      this.cleanupOldFailures();
    }
  }

  /**
   * Record a failure
   */
  private onFailure(error: Error): void {
    this.lastFailureTime = Date.now();
    this.failures.push({ timestamp: Date.now(), error });
    this.cleanupOldFailures();

    if (this.state === CircuitState.HALF_OPEN) {
      // Any failure in half-open state reopens the circuit
      this.transitionTo(CircuitState.OPEN);
    } else if (this.state === CircuitState.CLOSED && this.failures.length >= this.failureThreshold) {
      this.transitionTo(CircuitState.OPEN);
    }
  }

  /**
   * Remove failures outside the failure window
   */
  private cleanupOldFailures(): void {
    const cutoff = Date.now() - this.failureWindow;
    this.failures = this.failures.filter((f) => f.timestamp > cutoff);
  }

  /**
   * Transition to a new state
   */
  private transitionTo(newState: CircuitState): void {
    const previousState = this.state;
    this.state = newState;

    if (newState === CircuitState.CLOSED) {
      this.failures = [];
      this.successCount = 0;
    } else if (newState === CircuitState.HALF_OPEN) {
      this.successCount = 0;
    }

    this.logger.info(
      {
        circuitBreaker: this.name,
        previousState,
        newState,
        failureCount: this.failures.length,
      },
      'Circuit breaker state changed'
    );
  }

  /**
   * Get remaining time before reset attempt
   */
  private getRemainingResetTime(): number {
    return Math.max(0, this.resetTimeout - (Date.now() - this.lastFailureTime));
  }

  /**
   * Get current state
   */
  getState(): CircuitState {
    return this.state;
  }

  /**
   * Get circuit breaker stats
   */
  getStats(): {
    state: CircuitState;
    failureCount: number;
    successCount: number;
    lastFailureTime: number;
    remainingResetTime: number;
  } {
    return {
      state: this.state,
      failureCount: this.failures.length,
      successCount: this.successCount,
      lastFailureTime: this.lastFailureTime,
      remainingResetTime: this.state === CircuitState.OPEN ? this.getRemainingResetTime() : 0,
    };
  }

  /**
   * Manually reset the circuit breaker
   */
  reset(): void {
    this.transitionTo(CircuitState.CLOSED);
    this.logger.info({ circuitBreaker: this.name }, 'Circuit breaker manually reset');
  }
}
