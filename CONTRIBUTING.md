# Contributing to amqp-resilient

Thank you for your interest in contributing! This document provides guidelines and instructions for contributing.

## Code of Conduct

Please read and follow our [Code of Conduct](CODE_OF_CONDUCT.md).

## How to Contribute

### Reporting Bugs

1. Check if the bug has already been reported in [Issues](https://github.com/berkeerdo/amqp-resilient/issues)
2. If not, create a new issue with:
   - Clear, descriptive title
   - Steps to reproduce
   - Expected vs actual behavior
   - Environment details (Node.js version, RabbitMQ version, OS, etc.)
   - Code samples if applicable

### Suggesting Features

1. Check existing issues for similar suggestions
2. Create a new issue with:
   - Clear description of the feature
   - Use case / motivation
   - Proposed API (if applicable)

### Pull Requests

1. Fork the repository
2. Create a feature branch: `git checkout -b feat/my-feature`
3. Make your changes
4. Write/update tests
5. Ensure all tests pass: `npm test`
6. Ensure linting passes: `npm run lint`
7. Commit using conventional commits
8. Push and create a Pull Request

## Development Setup

```bash
# Clone your fork
git clone https://github.com/YOUR_USERNAME/amqp-resilient.git
cd amqp-resilient

# Install dependencies
npm install

# Run unit tests
npm test

# Run integration tests (requires RabbitMQ)
npm run test:integration

# Run all tests
npm run test:all

# Run tests with coverage
npm run test:coverage

# Run linter
npm run lint

# Build
npm run build
```

### Running RabbitMQ for Integration Tests

```bash
# Using Docker
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management

# Or using Homebrew (macOS)
brew services start rabbitmq
```

## Commit Convention

We use [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<scope>): <description>

[optional body]

[optional footer(s)]
```

### Types

- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation only
- `style`: Formatting (no code change)
- `refactor`: Code refactoring
- `perf`: Performance improvement
- `test`: Adding/updating tests
- `build`: Build system changes
- `ci`: CI configuration
- `chore`: Other changes

### Examples

```
feat(consumer): add dead letter queue support
fix(connection): handle unexpected disconnects correctly
docs(readme): add circuit breaker example
test(publisher): add coverage for confirm mode
```

## Code Style

- Use TypeScript
- Follow ESLint rules
- Use Prettier for formatting
- Write descriptive variable/function names
- Add JSDoc comments for public APIs
- Keep functions small and focused

## Testing

- Write unit tests for new features
- Write integration tests for complex scenarios
- Maintain >80% code coverage
- Test edge cases and error scenarios

```bash
# Run unit tests
npm test

# Run integration tests
npm run test:integration

# Run with coverage
npm run test:coverage

# Run specific test file
npm test -- tests/CircuitBreaker.test.ts

# Watch mode
npm run test:watch
```

## Documentation

- Update README.md for user-facing changes
- Add JSDoc comments for new public APIs
- Include code examples where helpful

## Release Process

Releases are automated via GitHub Actions when changes are merged to `main`:

1. Version is checked against npm registry
2. If version is new, package is published
3. GitHub release is created automatically

To trigger a release, update the version in `package.json` and merge to main.

## Questions?

Feel free to open an issue for any questions or concerns.

Thank you for contributing!
