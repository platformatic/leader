# @platformatic/leader

PostgreSQL advisory lock-based leader election with notification channels.

Ensures only one instance in a cluster acts as the leader, using PostgreSQL advisory locks for distributed coordination and `LISTEN`/`NOTIFY` for real-time event routing.

## Install

```bash
npm install @platformatic/leader
```

## Usage

### Election only

```js
'use strict'

const pg = require('pg')
const createLeaderElector = require('@platformatic/leader')

const pool = new pg.Pool({
  connectionString: 'postgres://localhost/mydb'
})

const leader = createLeaderElector({
  pool,
  lock: 4242,
  onLeadershipChange: (isLeader) => {
    if (isLeader) {
      console.log('I am the leader — starting work')
    } else {
      console.log('No longer the leader — stopping work')
    }
  }
})

leader.start()

console.log(leader.isLeader()) // true or false

await leader.stop()
await pool.end()
```

### With notification channels

```js
'use strict'

const pg = require('pg')
const createLeaderElector = require('@platformatic/leader')

const pool = new pg.Pool({
  connectionString: 'postgres://localhost/mydb'
})

const leader = createLeaderElector({
  pool,
  lock: 4242,
  poll: 10000,
  channels: [
    {
      channel: 'my_events',
      onNotification: (payload) => {
        console.log('Received:', payload)
      }
    }
  ],
  onLeadershipChange: (isLeader) => {
    console.log('Leadership changed:', isLeader)
  }
})

leader.start()

// Send a notification to the leader
await leader.notify({ action: 'refresh' }, 'my_events')

// Check if this instance is the leader
console.log(leader.isLeader())

// Stop the leader elector
await leader.stop()
await pool.end()
```

## API

### `createLeaderElector(options)`

Returns an object with `{ start, stop, notify, isLeader }`.

#### Options

| Option | Type | Required | Default | Description |
|---|---|---|---|---|
| `pool` | `pg.Pool` | Yes | - | A [node-postgres](https://node-postgres.com/) pool instance |
| `lock` | `number` | Yes | - | PostgreSQL advisory lock ID |
| `poll` | `number` | No | `10000` | Polling interval in milliseconds |
| `channels` | `Array` | Yes | - | Notification channel configurations |
| `log` | `object` | No | `pino()` | Logger with `info`, `debug`, `warn`, `error` methods |
| `onLeadershipChange` | `function` | No | `null` | Callback invoked with `(isLeader: boolean)` when leadership status changes |

Each channel in the `channels` array must have:
- `channel` (string) - PostgreSQL notification channel name
- `onNotification` (function) - Handler called with the parsed payload when a notification arrives

#### Methods

- **`start()`** - Begins the leader election loop. Returns a promise.
- **`stop()`** - Stops the leader election loop and releases resources.
- **`notify(payload, channelName)`** - Sends a notification on the given channel. The payload is JSON-serialized.
- **`isLeader()`** - Returns `true` if this instance is currently the leader.

## How It Works

1. Each instance periodically attempts to acquire a PostgreSQL advisory lock using `pg_try_advisory_lock`.
2. The instance that acquires the lock becomes the leader and starts listening for notifications via `LISTEN`.
3. Only the leader processes incoming notifications and routes them to the appropriate channel handler.
4. If the leader stops or loses the lock, another instance acquires it and takes over.
5. The `onLeadershipChange` callback fires whenever the leadership status transitions.

## Development

Start PostgreSQL:

```
docker compose up -d
```

Run tests:

```
node --test test/*.test.js
```

## License

Apache-2.0
