# Contributing to @platformatic/leader

Welcome! We're glad you're interested in contributing.

## Prerequisites

- **Node.js**: Version 22 or higher
- **PostgreSQL**: Required for running tests

## Getting Started

```bash
git clone https://github.com/platformatic/leader.git
cd leader
npm install
```

## Running Tests

Tests require a running PostgreSQL instance. Set the `CONNECTION_STRING` environment variable or use the default (`postgres://postgres:postgres@127.0.0.1:5433/scaler`).

```bash
npm test
```

## Code Style

This project uses [Standard](https://standardjs.com/) for linting:

```bash
npm run lint
```

## Pull Request Process

1. Ensure all tests pass: `npm test`
2. Run linting: `npm run lint`
3. Open a pull request with a clear description of the changes

## Developer Certificate of Origin

All contributions must include a Developer Certificate of Origin (DCO) sign-off:

```bash
git commit -s -m "Your commit message"
```

## Getting Help

- Open an issue on GitHub
- Join the [Platformatic Discord](https://discord.gg/platformatic)

Thank you for contributing!
