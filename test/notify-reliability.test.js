'use strict'

const { test } = require('node:test')
const assert = require('node:assert')
const { setTimeout: sleep } = require('node:timers/promises')
const pg = require('pg')
const createLeaderElector = require('../index')

const connectionString = process.env.CONNECTION_STRING || 'postgres://postgres:postgres@127.0.0.1:5433/leader'

const silentLogger = {
  info: () => {},
  debug: () => {},
  warn: () => {},
  error: () => {}
}

function createPool () {
  return new pg.Pool({ connectionString })
}

test('leader receives notifications sent via pg_notify from a different pool connection', async (t) => {
  const pool = createPool()
  const receivedNotifications = []

  t.after(async () => {
    await leaderElector.stop()
    await pool.end()
  })

  const leaderElector = createLeaderElector({
    pool,
    lock: 50001,
    poll: 10000,
    channels: [
      {
        channel: 'test_pg_notify',
        onNotification: (payload) => {
          receivedNotifications.push(payload)
        }
      }
    ],
    log: silentLogger
  })

  leaderElector.start()
  await sleep(500)
  assert.ok(leaderElector.isLeader())

  // Send notification using pg_notify from a pool connection (like the workflow queue plugin does)
  await pool.query("SELECT pg_notify('test_pg_notify', '{\"msg\":\"hello\"}')")

  await sleep(500)

  assert.strictEqual(receivedNotifications.length, 1)
  assert.deepStrictEqual(receivedNotifications[0], { msg: 'hello' })
})

test('leader receives rapid-fire notifications from separate pool connections', async (t) => {
  const pool = createPool()
  const receivedNotifications = []
  const NOTIFICATION_COUNT = 20

  t.after(async () => {
    await leaderElector.stop()
    await pool.end()
  })

  const leaderElector = createLeaderElector({
    pool,
    lock: 50002,
    poll: 10000,
    channels: [
      {
        channel: 'test_rapid_notify',
        onNotification: (payload) => {
          receivedNotifications.push(payload)
        }
      }
    ],
    log: silentLogger
  })

  leaderElector.start()
  await sleep(500)

  // Send many notifications rapidly from pool connections
  for (let i = 0; i < NOTIFICATION_COUNT; i++) {
    await pool.query(`SELECT pg_notify('test_rapid_notify', '{"i":${i}}')`)
  }

  await sleep(2000)

  assert.strictEqual(receivedNotifications.length, NOTIFICATION_COUNT,
    `Expected ${NOTIFICATION_COUNT} notifications but received ${receivedNotifications.length}`)
})

test('notifications are received promptly (within 100ms), not delayed by poll interval', async (t) => {
  const pool = createPool()
  const timestamps = []

  t.after(async () => {
    await leaderElector.stop()
    await pool.end()
  })

  const leaderElector = createLeaderElector({
    pool,
    lock: 50003,
    poll: 10000, // 10 second poll - notifications should NOT wait for this
    channels: [
      {
        channel: 'test_prompt_notify',
        onNotification: () => {
          timestamps.push(Date.now())
        }
      }
    ],
    log: silentLogger
  })

  leaderElector.start()
  await sleep(500)

  const sendTime = Date.now()
  await pool.query("SELECT pg_notify('test_prompt_notify', '{}')")

  await sleep(1000)

  assert.strictEqual(timestamps.length, 1, 'Should have received exactly 1 notification')
  const latency = timestamps[0] - sendTime
  assert.ok(latency < 100, `Notification latency was ${latency}ms, expected < 100ms`)
})

test('notifications sent during leader advisory lock re-check are not lost', async (t) => {
  const pool = createPool()
  const receivedNotifications = []

  t.after(async () => {
    await leaderElector.stop()
    await pool.end()
  })

  // Use a very short poll interval so the leader frequently re-checks the advisory lock
  // This means the shared client is frequently busy with pg_try_advisory_lock queries
  const leaderElector = createLeaderElector({
    pool,
    lock: 50004,
    poll: 50, // Very frequent re-checks — maximizes chance of notification during a query
    channels: [
      {
        channel: 'test_during_recheck',
        onNotification: (payload) => {
          receivedNotifications.push(payload)
        }
      }
    ],
    log: silentLogger
  })

  leaderElector.start()
  await sleep(300)

  // Send 50 notifications with small delays, overlapping with advisory lock re-checks
  const TOTAL = 50
  for (let i = 0; i < TOTAL; i++) {
    await pool.query(`SELECT pg_notify('test_during_recheck', '{"i":${i}}')`)
    await sleep(10) // Small delay to spread notifications across multiple poll cycles
  }

  await sleep(1000)

  assert.strictEqual(receivedNotifications.length, TOTAL,
    `Expected ${TOTAL} notifications but received ${receivedNotifications.length}. ` +
    `Missing: ${TOTAL - receivedNotifications.length}`)
})

test('onNotification that fires-and-forgets an async function does not block subsequent notifications', async (t) => {
  const pool = createPool()
  const receivedNotifications = []

  t.after(async () => {
    await leaderElector.stop()
    await pool.end()
  })

  // Simulate what the workflow poller does: onNotification calls an async function
  // but does NOT return the promise (fire-and-forget)
  async function execute () {
    // Simulate some async work
    await sleep(50)
  }

  const leaderElector = createLeaderElector({
    pool,
    lock: 50005,
    poll: 10000,
    channels: [
      {
        channel: 'test_fire_forget',
        onNotification: () => {
          // Fire-and-forget — does NOT return the promise
          // This is exactly what the workflow poller does:
          // onNotification: () => { execute() }
          execute()
          receivedNotifications.push(Date.now())
        }
      }
    ],
    log: silentLogger
  })

  leaderElector.start()
  await sleep(500)

  // Send 5 notifications rapidly
  for (let i = 0; i < 5; i++) {
    await pool.query("SELECT pg_notify('test_fire_forget', '{}')")
  }

  await sleep(2000)

  assert.strictEqual(receivedNotifications.length, 5,
    `Expected 5 notifications but received ${receivedNotifications.length}`)

  // All notifications should arrive promptly, not delayed by the async work
  const maxGap = Math.max(...receivedNotifications.slice(1).map((ts, i) => ts - receivedNotifications[i]))
  assert.ok(maxGap < 200, `Max gap between notifications was ${maxGap}ms, expected < 200ms`)
})
