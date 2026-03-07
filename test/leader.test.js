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

test('should notify through PostgreSQL notification', async (t) => {
  const listenClient = new pg.Client(connectionString)
  await listenClient.connect()

  const pool = createPool()

  t.after(async () => {
    await listenClient.end()
    await pool.end()
  })

  let notificationReceived = false
  let receivedPayload = null

  listenClient.on('notification', (msg) => {
    if (msg.channel === 'test_channel') {
      notificationReceived = true
      receivedPayload = msg.payload
    }
  })

  await listenClient.query('LISTEN "test_channel"')

  const leaderElection = createLeaderElector({
    pool,
    lock: 9999,
    channels: [
      {
        channel: 'test_channel',
        onNotification: () => {}
      }
    ],
    log: silentLogger
  })

  const testPayload = 'test-payload-123'
  await leaderElection.notify(testPayload, 'test_channel')

  await sleep(1000)

  assert.ok(notificationReceived)
  assert.strictEqual(JSON.parse(receivedPayload), testPayload)
})

test('leaderElector notifies through PostgreSQL notification with an object', async (t) => {
  const listenClient = new pg.Client(connectionString)
  await listenClient.connect()

  const pool = createPool()

  t.after(async () => {
    await listenClient.end()
    await pool.end()
  })

  let notificationReceived = false
  let receivedPayload = null

  listenClient.on('notification', (msg) => {
    if (msg.channel === 'test_channel') {
      notificationReceived = true
      receivedPayload = msg.payload
    }
  })

  await listenClient.query('LISTEN "test_channel"')

  const leaderElection = createLeaderElector({
    pool,
    lock: 9999,
    channels: [
      {
        channel: 'test_channel',
        onNotification: () => {}
      }
    ],
    log: silentLogger
  })

  const testPayload = { test: 'payload-123' }
  await leaderElection.notify(testPayload, 'test_channel')

  await sleep(1000)

  assert.ok(notificationReceived)
  assert.deepStrictEqual(JSON.parse(receivedPayload), testPayload)
})

test('leaderElector properly passes payload to callback', async (t) => {
  let callbackCount = 0
  let callbackPayload = null

  const pool = createPool()

  t.after(async () => {
    await leaderElector.stop()
    await pool.end()
  })

  const leaderElector = createLeaderElector({
    pool,
    lock: 9999,
    poll: 200,
    channels: [
      {
        channel: 'test_callback_channel',
        onNotification: (payload) => {
          callbackCount++
          callbackPayload = payload
        }
      }
    ],
    log: silentLogger
  })

  leaderElector.start()

  await sleep(500)

  const testPayload = 'test-callback-payload'
  await leaderElector.notify(testPayload, 'test_callback_channel')

  await sleep(1000)

  assert.strictEqual(callbackCount, 1)
  assert.strictEqual(callbackPayload, testPayload)
})

test('if one instance is shut down, the other is elected', async (t) => {
  const pool1 = createPool()
  const pool2 = createPool()

  const lockId = Math.floor(Math.random() * 1000) + 7000

  await pool1.query('SELECT pg_advisory_unlock_all()')
  await pool2.query('SELECT pg_advisory_unlock_all()')

  const leaderElector1 = createLeaderElector({
    pool: pool1,
    lock: lockId,
    poll: 200,
    channels: [
      {
        channel: 'test_re_election_channel',
        onNotification: () => {}
      }
    ],
    log: silentLogger
  })

  leaderElector1.start()
  await sleep(500)
  assert.ok(leaderElector1.isLeader())

  const leaderElector2 = createLeaderElector({
    pool: pool2,
    lock: lockId,
    poll: 200,
    channels: [
      {
        channel: 'test_re_election_channel',
        onNotification: () => {}
      }
    ],
    log: silentLogger
  })

  leaderElector2.start()
  await sleep(500)

  assert.ok(leaderElector1.isLeader())
  assert.ok(!leaderElector2.isLeader())

  await leaderElector1.stop()
  await sleep(500)
  await pool1.end()

  await sleep(500)

  assert.ok(leaderElector2.isLeader())

  await leaderElector2.stop()
  await pool2.end()
})

test('only the leader instance executes notification callbacks and leadership transfers properly', async (t) => {
  const pool1 = createPool()
  const pool2 = createPool()

  const lockId = Math.floor(Math.random() * 1000) + 7000
  const testChannel = `test_leader_notifications_${lockId}`

  t.after(async () => {
    try {
      await pool1.end().catch(() => {})
      await pool2.end().catch(() => {})
    } catch {}
  })

  await pool1.query('SELECT pg_advisory_unlock_all()')
  await pool2.query('SELECT pg_advisory_unlock_all()')

  let instance1Notifications = 0
  let instance2Notifications = 0

  const leaderElector1 = createLeaderElector({
    pool: pool1,
    lock: lockId,
    poll: 100,
    channels: [
      {
        channel: testChannel,
        onNotification: () => {
          instance1Notifications++
        }
      }
    ],
    log: silentLogger
  })

  const leaderElector2 = createLeaderElector({
    pool: pool2,
    lock: lockId,
    poll: 100,
    channels: [
      {
        channel: testChannel,
        onNotification: () => {
          instance2Notifications++
        }
      }
    ],
    log: silentLogger
  })

  leaderElector1.start()
  await sleep(300)
  leaderElector2.start()
  await sleep(300)

  assert.ok(leaderElector1.isLeader())
  assert.ok(!leaderElector2.isLeader())

  const testPayload1 = 'test-notification-payload-1'
  await leaderElector1.notify(testPayload1, testChannel)

  await sleep(300)

  assert.strictEqual(instance1Notifications, 1)
  assert.strictEqual(instance2Notifications, 0)

  await leaderElector1.stop()
  await sleep(500)
  await pool1.end()

  await sleep(500)
  assert.ok(leaderElector2.isLeader())

  const testPayload2 = 'test-notification-payload-2'
  await leaderElector2.notify(testPayload2, testChannel)

  await sleep(300)

  assert.strictEqual(instance1Notifications, 1)
  assert.strictEqual(instance2Notifications, 1)

  await leaderElector2.stop()
})

test('should throw error when required parameters are missing', async () => {
  assert.throws(() => {
    createLeaderElector({ lock: 123 })
  }, { message: 'pool is required' })

  assert.throws(() => {
    createLeaderElector({ pool: {} })
  }, { message: 'lock is required' })

  assert.throws(() => {
    createLeaderElector({ pool: {}, lock: 123 })
  }, { message: 'channels array is required' })
})

test('should trigger onLeadershipChange callback when leadership changes', async (t) => {
  const pool = createPool()

  const leadershipChanges = []

  const leaderElector = createLeaderElector({
    pool,
    lock: 8888,
    poll: 200,
    channels: [
      {
        channel: 'test_leadership_change',
        onNotification: () => {}
      }
    ],
    log: silentLogger,
    onLeadershipChange: (isLeader) => {
      leadershipChanges.push(isLeader)
    }
  })

  leaderElector.start()
  await sleep(500)

  assert.ok(leaderElector.isLeader())
  assert.strictEqual(leadershipChanges.length, 1)
  assert.strictEqual(leadershipChanges[0], true)

  await leaderElector.stop()
  await pool.end()
})

test('should handle errors in onNotification callback', async (t) => {
  const pool = createPool()

  const logMessages = []
  const leaderElector = createLeaderElector({
    pool,
    lock: 7777,
    poll: 200,
    channels: [
      {
        channel: 'test_notification_error',
        onNotification: () => {
          throw new Error('Test notification error')
        }
      }
    ],
    log: {
      info: () => {},
      debug: () => {},
      warn: (data, msg) => logMessages.push({ level: 'warn', data, msg }),
      error: () => {}
    }
  })

  leaderElector.start()
  await sleep(500)

  await leaderElector.notify('test-error-payload', 'test_notification_error')
  await sleep(500)

  await leaderElector.stop()
  await pool.end()

  const warnLog = logMessages.find(log => log.level === 'warn' && log.data.err)
  assert.ok(warnLog)
  assert.strictEqual(warnLog.data.err.message, 'Test notification error')
})

test('should support multiple notification channels', async (t) => {
  const pool = createPool()

  const channel1Notifications = []
  const channel2Notifications = []

  const leaderElector = createLeaderElector({
    pool,
    lock: 9999,
    poll: 200,
    channels: [
      {
        channel: 'test_channel_1',
        onNotification: (payload) => {
          channel1Notifications.push(payload)
        }
      },
      {
        channel: 'test_channel_2',
        onNotification: (payload) => {
          channel2Notifications.push(payload)
        }
      }
    ],
    log: silentLogger
  })

  leaderElector.start()
  await sleep(500)

  await leaderElector.notify({ message: 'hello from channel 1' }, 'test_channel_1')
  await sleep(300)

  await leaderElector.notify({ message: 'hello from channel 2' }, 'test_channel_2')
  await sleep(300)

  await leaderElector.notify({ message: 'second message on channel 1' }, 'test_channel_1')
  await sleep(300)

  await leaderElector.stop()
  await pool.end()

  assert.strictEqual(channel1Notifications.length, 2)
  assert.strictEqual(channel1Notifications[0].message, 'hello from channel 1')
  assert.strictEqual(channel1Notifications[1].message, 'second message on channel 1')

  assert.strictEqual(channel2Notifications.length, 1)
  assert.strictEqual(channel2Notifications[0].message, 'hello from channel 2')
})

test('should route notifications to correct channel handler', async (t) => {
  const pool = createPool()

  const receivedNotifications = []

  const leaderElector = createLeaderElector({
    pool,
    lock: 10000,
    poll: 200,
    channels: [
      {
        channel: 'channel_a',
        onNotification: (payload) => {
          receivedNotifications.push({ channel: 'a', payload })
        }
      },
      {
        channel: 'channel_b',
        onNotification: (payload) => {
          receivedNotifications.push({ channel: 'b', payload })
        }
      }
    ],
    log: silentLogger
  })

  leaderElector.start()
  await sleep(500)

  await leaderElector.notify({ id: 1 }, 'channel_a')
  await sleep(300)
  await leaderElector.notify({ id: 2 }, 'channel_b')
  await sleep(300)
  await leaderElector.notify({ id: 3 }, 'channel_a')
  await sleep(300)

  await leaderElector.stop()
  await pool.end()

  assert.strictEqual(receivedNotifications.length, 3)
  assert.strictEqual(receivedNotifications[0].channel, 'a')
  assert.strictEqual(receivedNotifications[0].payload.id, 1)
  assert.strictEqual(receivedNotifications[1].channel, 'b')
  assert.strictEqual(receivedNotifications[1].payload.id, 2)
  assert.strictEqual(receivedNotifications[2].channel, 'a')
  assert.strictEqual(receivedNotifications[2].payload.id, 3)
})

test('should throw error when channels array has invalid entries', async () => {
  assert.throws(() => {
    createLeaderElector({
      pool: {},
      lock: 123,
      log: { info: () => {} },
      channels: [
        { channel: 'test', onNotification: () => {} },
        { channel: 'test2' }
      ]
    })
  }, { message: 'onNotification is required for each notification channel' })

  assert.throws(() => {
    createLeaderElector({
      pool: {},
      lock: 123,
      log: { info: () => {} },
      channels: [
        { onNotification: () => {} }
      ]
    })
  }, { message: 'channel is required for each notification channel' })
})
