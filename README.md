# amqp-resilient

[![npm version](https://img.shields.io/npm/v/amqp-resilient.svg)](https://www.npmjs.com/package/amqp-resilient)
[![npm downloads](https://img.shields.io/npm/dm/amqp-resilient.svg)](https://www.npmjs.com/package/amqp-resilient)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.0+-blue.svg)](https://www.typescriptlang.org/)
[![Node.js](https://img.shields.io/badge/Node.js-18+-green.svg)](https://nodejs.org/)

Production-ready AMQP client for Node.js with built-in resilience patterns.

## Features

- **Auto-Reconnection** - Exponential backoff with jitter for stable reconnects
- **Circuit Breaker** - Prevent cascading failures with configurable thresholds
- **Retry with DLQ** - Failed messages retry with backoff, then route to Dead Letter Queue
- **Publisher Confirms** - Guaranteed message delivery with confirmation
- **Health Monitoring** - Built-in health checks for all connections
- **TypeScript** - Full type safety with generics support
- **ESM** - Native ES modules support
- **Zero Dependencies** - Only amqplib as peer dependency

## Installation

```bash
npm install amqp-resilient amqplib
```

## Quick Start

### Connection

```typescript
import { ConnectionManager } from 'amqp-resilient';

const connection = new ConnectionManager({
  url: 'amqp://localhost:5672',
  connectionName: 'my-service',
  logger: console, // optional
});

await connection.connect();
```

### Consumer

```typescript
import { BaseConsumer, type MessageContext } from 'amqp-resilient';

interface OrderMessage {
  orderId: string;
  amount: number;
}

class OrderConsumer extends BaseConsumer<OrderMessage> {
  constructor(connection: ConnectionManager) {
    super(connection, {
      queue: 'orders',
      exchange: 'orders.exchange',
      routingKeys: ['order.created', 'order.updated'],
      prefetch: 10,
      maxRetries: 3,
    });
  }

  protected async handle(message: OrderMessage, context: MessageContext): Promise<void> {
    console.log(`Processing order ${message.orderId}`);
    // Throw an error to trigger retry
  }
}

const consumer = new OrderConsumer(connection);
await consumer.start();
```

### Publisher

```typescript
import { BasePublisher } from 'amqp-resilient';

const publisher = new BasePublisher(connection, {
  exchange: 'orders.exchange',
  confirm: true,
});

const result = await publisher.publish('order.created', {
  orderId: '123',
  amount: 99.99,
});

if (result.success) {
  console.log(`Published message ${result.messageId}`);
}

// Or throw on failure
await publisher.publishOrThrow('order.created', { orderId: '456' });
```

## Configuration

### ConnectionManager

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `url` | string | - | Full AMQP URL (amqp://host:port) |
| `host` | string | - | RabbitMQ host (alternative to url) |
| `port` | number | 5672 | RabbitMQ port |
| `username` | string | guest | Username |
| `password` | string | guest | Password |
| `vhost` | string | / | Virtual host |
| `connectionName` | string | **required** | Name for logging and health checks |
| `prefetch` | number | 10 | Default channel prefetch count |
| `heartbeat` | number | 60 | Heartbeat interval in seconds |
| `maxReconnectAttempts` | number | 0 | Max reconnect attempts (0 = unlimited) |
| `initialReconnectDelay` | number | 1000 | Initial reconnect delay (ms) |
| `maxReconnectDelay` | number | 30000 | Max reconnect delay (ms) |
| `logger` | AmqpLogger | noop | Logger instance |

### BaseConsumer

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `queue` | string | **required** | Queue name |
| `exchange` | string | **required** | Exchange to bind |
| `routingKeys` | string[] | **required** | Routing keys to bind |
| `prefetch` | number | 10 | Consumer prefetch count |
| `maxRetries` | number | 3 | Max retries before DLQ |
| `initialRetryDelay` | number | 1000 | Initial retry delay (ms) |
| `maxRetryDelay` | number | 30000 | Max retry delay (ms) |
| `useCircuitBreaker` | boolean | true | Enable circuit breaker |
| `exchangeType` | ExchangeType | topic | Exchange type |

### BasePublisher

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `exchange` | string | **required** | Exchange name |
| `exchangeType` | ExchangeType | topic | Exchange type |
| `confirm` | boolean | true | Use publisher confirms |
| `maxRetries` | number | 3 | Max publish retries |
| `initialRetryDelay` | number | 100 | Initial retry delay (ms) |
| `maxRetryDelay` | number | 5000 | Max retry delay (ms) |
| `useCircuitBreaker` | boolean | true | Enable circuit breaker |

## Health & Metrics

```typescript
import { HealthService, ConnectionStatus } from 'amqp-resilient';

// Get overall health status
const status = HealthService.getOverallStatus();
// 'healthy' | 'degraded' | 'dead' | 'not_configured'

// Get specific connection status
const connStatus = HealthService.getStatus('my-connection');
// ConnectionStatus.CONNECTED | CONNECTING | DISCONNECTED | RECONNECTING | CLOSED

// Get all connection statuses
const allStatuses = HealthService.getAllStatuses();
// { 'my-connection': 'connected', 'other-connection': 'reconnecting' }

// Get connection stats
const stats = connection.getStats();
// {
//   connected: true,
//   reconnectAttempts: 0,
//   channelCount: 2,
//   lastConnectedAt: Date,
// }
```

## Events

```typescript
connection.on('connected', () => {
  console.log('AMQP connected');
});

connection.on('disconnected', () => {
  console.log('AMQP disconnected');
});

connection.on('reconnecting', (attempt) => {
  console.log(`Reconnecting attempt ${attempt}`);
});

connection.on('error', (error) => {
  console.error('Connection error:', error);
});
```

## Circuit Breaker

Standalone circuit breaker that can be used independently:

```typescript
import { CircuitBreaker, CircuitBreakerOpenError } from 'amqp-resilient';

const breaker = new CircuitBreaker({
  name: 'external-api',
  failureThreshold: 5,    // Open after 5 failures
  resetTimeout: 30000,    // Try half-open after 30s
  successThreshold: 3,    // Close after 3 successes in half-open
});

try {
  const result = await breaker.execute(async () => {
    return await externalApiCall();
  });
} catch (error) {
  if (error instanceof CircuitBreakerOpenError) {
    console.log(`Circuit open, retry in ${error.remainingResetTime}ms`);
  }
}

// Get circuit state
const state = breaker.getState();
// 'CLOSED' | 'OPEN' | 'HALF_OPEN'
```

## Logger Interface

Any logger that implements this interface works:

```typescript
interface AmqpLogger {
  info(obj: object, msg?: string): void;
  warn(obj: object, msg?: string): void;
  error(obj: object, msg?: string): void;
  debug(obj: object, msg?: string): void;
}

// Examples: pino, winston, bunyan, console
```

## Dead Letter Queue (DLQ)

Messages that fail after max retries are automatically sent to a DLQ:

```typescript
// Queue: orders
// DLQ: orders.dlq (auto-created)

// Message headers in DLQ:
// x-death-reason: 'max-retries-exceeded'
// x-death-count: 3
// x-original-routing-key: 'order.created'
// x-first-death-queue: 'orders'
```

## Requirements

- Node.js >= 18.0.0
- amqplib >= 0.10.0

## License

MIT © [berkeerdo](https://github.com/berkeerdo)
