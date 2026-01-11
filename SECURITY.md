# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 1.x.x   | :white_check_mark: |

## Reporting a Vulnerability

We take security seriously. If you discover a security vulnerability, please report it responsibly.

### How to Report

1. **Do NOT** open a public GitHub issue
2. Email security concerns to: berkeerdo@pm.me
3. Include:
   - Description of the vulnerability
   - Steps to reproduce
   - Potential impact
   - Suggested fix (if any)

### What to Expect

- **Acknowledgment**: Within 48 hours
- **Initial Assessment**: Within 1 week
- **Resolution Timeline**: Depends on severity
  - Critical: 24-48 hours
  - High: 1 week
  - Medium: 2 weeks
  - Low: Next release

### Disclosure Policy

- We will work with you to understand and resolve the issue
- We will credit you in the security advisory (unless you prefer anonymity)
- We ask that you give us reasonable time to fix the issue before public disclosure

## Security Best Practices

When using this library:

### Connection Security

```typescript
// Use TLS in production (RabbitMQ with TLS)
const connection = new ConnectionManager({
  url: 'amqps://user:pass@rabbitmq.example.com:5671',
  connectionName: 'my-service',
});
```

### Credential Handling

- Never log sensitive data (passwords, tokens)
- Use environment variables for connection URLs
- Rotate credentials regularly
- Use separate credentials per service

### Network Security

- Use internal networks when possible
- Implement proper firewall rules
- Use TLS for all production connections
- Consider using vhosts for isolation

## Known Security Considerations

### Message Security

Messages are not encrypted by default. Consider:

- Encrypting sensitive message payloads at application level
- Using TLS for transport encryption
- Not including secrets in message bodies

### Dead Letter Queue

Messages in DLQ may contain sensitive data:

- Implement proper access controls on DLQ
- Consider message expiration policies
- Monitor DLQ for sensitive data exposure

### Logging

The library logs connection and error information. Ensure your logger:

- Does not log sensitive message content
- Properly sanitizes output
- Has appropriate access controls

```typescript
// Use a logger that filters sensitive data
const connection = new ConnectionManager({
  url: process.env.RABBITMQ_URL,
  connectionName: 'my-service',
  logger: sanitizedLogger, // Filter passwords, tokens, etc.
});
```

## Dependencies

We regularly update dependencies to address security vulnerabilities. Run:

```bash
npm audit
```

To check for known vulnerabilities in dependencies.
