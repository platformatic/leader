# CLAUDE.md

## Project Overview

`@platformatic/leader` is a PostgreSQL advisory lock-based leader election module with notification channels. It uses `@databases/pg` for database access.

## Commands

- Install: `npm install`
- Run tests: `node --test test/*.test.js`
- Lint: `npx standard --fix`

## Code Style

- Standard JS (2-space indentation, no semicolons, single quotes)
- CommonJS modules (require/module.exports)
- `'use strict'` at the top of every file

## Architecture

- `index.js` - Exports `createLeaderElector()` factory function
- Uses PostgreSQL `pg_try_advisory_lock` for leader election
- Uses PostgreSQL `LISTEN`/`NOTIFY` for notification channels
- Polls at configurable intervals, retries on errors

## Testing

- Tests use `node:test` and require a running PostgreSQL instance
- Set `CONNECTION_STRING` env var or use default (`postgres://postgres:postgres@127.0.0.1:5433/scaler`)
