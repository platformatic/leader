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

// Simulates the workflow poller pattern exactly as used in platformatic-world
test('poller pattern: deferred timer wakes execute after notification-triggered cycle', async (t) => {
  const pool = createPool()
  const dispatches = []

  t.after(async () => {
    stopped = true
    if (deferredTimer) clearTimeout(deferredTimer)
    await leader.stop()
    await pool.end()
  })

  let stopped = false
  let executing = false
  let pendingNotify = false
  let deferredTimer = null

  async function execute () {
    if (stopped || !leader.isLeader()) return

    if (executing) {
      pendingNotify = true
      return
    }
    executing = true

    try {
      await executeOnce()
    } finally {
      executing = false
      if (pendingNotify && !stopped && leader.isLeader()) {
        pendingNotify = false
        setImmediate(() => execute())
      }
    }
  }

  async function executeOnce () {
    // Simulate: check for deferred messages, dispatch them
    dispatches.push(Date.now())

    // Simulate: dispatch creates a deferred message that should fire in 200ms
    // This is the key pattern: scheduleNextWakeup is called WITHOUT await
    scheduleNextWakeup(200)
  }

  // Simulate scheduleNextWakeup — fire-and-forget async function
  async function scheduleNextWakeup (ms) {
    if (deferredTimer) {
      clearTimeout(deferredTimer)
      deferredTimer = null
    }

    // Simulate the pool.query to find deferred messages
    await pool.query('SELECT 1')

    if (!stopped) {
      deferredTimer = setTimeout(() => {
        deferredTimer = null
        execute()
      }, ms)
    }
  }

  const leader = createLeaderElector({
    pool,
    lock: 60001,
    poll: 10000,
    channels: [{
      channel: 'poller_test_1',
      onNotification: () => { execute() }
    }],
    log: silentLogger,
    onLeadershipChange: (isLeader) => {
      if (isLeader) execute()
    }
  })

  leader.start()
  await sleep(300)
  assert.ok(leader.isLeader())

  // Initial execute from onLeadershipChange plus deferred chain (200ms each)
  assert.ok(dispatches.length >= 1, 'Should have at least 1 dispatch')

  const countAfterInit = dispatches.length
  await sleep(500)

  // The deferred timer chain should have continued
  assert.ok(dispatches.length > countAfterInit,
    `Expected dispatches to increase from ${countAfterInit} but got ${dispatches.length}`)
})

test('poller pattern: notification sent during executeOnce triggers re-execution', async (t) => {
  const pool = createPool()
  const dispatches = []

  t.after(async () => {
    stopped = true
    if (deferredTimer) clearTimeout(deferredTimer)
    await leader.stop()
    await pool.end()
  })

  let stopped = false
  let executing = false
  let pendingNotify = false
  const deferredTimer = null

  async function execute () {
    if (stopped || !leader.isLeader()) return

    if (executing) {
      pendingNotify = true
      return
    }
    executing = true

    try {
      await executeOnce()
    } finally {
      executing = false
      if (pendingNotify && !stopped && leader.isLeader()) {
        pendingNotify = false
        setImmediate(() => execute())
      }
    }
  }

  async function executeOnce () {
    dispatches.push(Date.now())
    await sleep(50)
  }

  const leader = createLeaderElector({
    pool,
    lock: 60002,
    poll: 10000,
    channels: [{
      channel: 'poller_test_2',
      onNotification: () => { execute() }
    }],
    log: silentLogger,
    onLeadershipChange: (isLeader) => {
      if (isLeader) execute()
    }
  })

  leader.start()
  await sleep(500)

  const countBefore = dispatches.length

  // Send a notification — should trigger execute
  await pool.query("SELECT pg_notify('poller_test_2', '{}')")

  await sleep(500)

  assert.ok(dispatches.length > countBefore,
    `Expected dispatches to increase from ${countBefore} but got ${dispatches.length}`)
})

test('poller pattern: simulates exact workflow failure scenario', async (t) => {
  // This test simulates the exact scenario from the CI failure:
  // 1. Initial batch dispatches messages
  // 2. One dispatch returns timeoutSeconds=1, creating a "deferred" wakeup
  // 3. No more notifications arrive (tests are polling, not queuing)
  // 4. The deferred timer must fire to continue processing

  const pool = createPool()
  const dispatches = []

  t.after(async () => {
    stopped = true
    if (deferredTimer) clearTimeout(deferredTimer)
    await leader.stop()
    await pool.end()
  })

  let stopped = false
  let executing = false
  let pendingNotify = false
  let deferredTimer = null
  let cycle = 0

  async function execute () {
    if (stopped || !leader.isLeader()) return

    if (executing) {
      pendingNotify = true
      return
    }
    executing = true

    try {
      await executeOnce()
    } finally {
      executing = false
      if (pendingNotify && !stopped && leader.isLeader()) {
        pendingNotify = false
        setImmediate(() => execute())
      }
    }
  }

  async function executeOnce () {
    cycle++
    const currentCycle = cycle
    dispatches.push({ cycle: currentCycle, time: Date.now() })

    // Simulate dispatch that takes some time
    await sleep(20)

    // On every cycle, simulate creating a deferred message (like timeoutSeconds=1)
    // scheduleNextWakeup is called WITHOUT await — this is the exact pattern from the poller
    if (currentCycle <= 3) {
      scheduleNextWakeup(200) // 200ms deferred
    }
  }

  async function scheduleNextWakeup (ms) {
    if (deferredTimer) {
      clearTimeout(deferredTimer)
      deferredTimer = null
    }

    // Simulate pool.query to check for deferred messages
    await pool.query('SELECT 1')

    if (!stopped) {
      deferredTimer = setTimeout(() => {
        deferredTimer = null
        execute()
      }, ms)
    }
  }

  const leader = createLeaderElector({
    pool,
    lock: 60003,
    poll: 10000,
    channels: [{
      channel: 'poller_test_3',
      onNotification: () => { execute() }
    }],
    log: silentLogger,
    onLeadershipChange: (isLeader) => {
      if (isLeader) execute()
    }
  })

  leader.start()
  await sleep(500)

  // By now, the chain should be:
  // cycle 1 (from onLeadershipChange) → sets deferred timer 200ms
  // cycle 2 (from deferred timer) → sets deferred timer 200ms
  // cycle 3 (from deferred timer) → sets deferred timer 200ms
  // cycle 4 (from deferred timer) → does NOT set timer (cycle > 3)

  // Wait enough time for all cycles
  await sleep(1500)

  assert.ok(dispatches.length >= 4,
    `Expected at least 4 dispatch cycles but got ${dispatches.length}: ` +
    JSON.stringify(dispatches.map(d => ({ cycle: d.cycle, elapsed: d.time - dispatches[0].time }))))

  // Verify timing: each cycle should be ~200ms apart (not 10 seconds)
  for (let i = 1; i < Math.min(dispatches.length, 4); i++) {
    const gap = dispatches[i].time - dispatches[i - 1].time
    assert.ok(gap < 1000,
      `Gap between cycle ${dispatches[i - 1].cycle} and ${dispatches[i].cycle} was ${gap}ms, expected < 1000ms`)
  }
})

test('poller pattern: race between scheduleNextWakeup and pendingNotify re-execution', async (t) => {
  // This tests the race condition where:
  // 1. executeOnce calls scheduleNextWakeup (fire-and-forget)
  // 2. A NOTIFY arrives during executeOnce, setting pendingNotify
  // 3. executeOnce finishes, pendingNotify triggers re-execution
  // 4. The re-execution's scheduleNextWakeup might clear the timer from step 1

  const pool = createPool()
  const dispatches = []

  t.after(async () => {
    stopped = true
    if (deferredTimer) clearTimeout(deferredTimer)
    await leader.stop()
    await pool.end()
  })

  let stopped = false
  let executing = false
  let pendingNotify = false
  let deferredTimer = null

  async function execute () {
    if (stopped || !leader.isLeader()) return

    if (executing) {
      pendingNotify = true
      return
    }
    executing = true

    try {
      await executeOnce()
    } finally {
      executing = false
      if (pendingNotify && !stopped && leader.isLeader()) {
        pendingNotify = false
        setImmediate(() => execute())
      }
    }
  }

  let shouldCreateDeferred = false

  async function executeOnce () {
    dispatches.push({ time: Date.now(), reason: shouldCreateDeferred ? 'deferred' : 'trigger' })

    // Simulate dispatch work
    await sleep(30)

    if (shouldCreateDeferred) {
      shouldCreateDeferred = false
      // Fire-and-forget scheduleNextWakeup
      scheduleNextWakeup(300)
    }
  }

  async function scheduleNextWakeup (ms) {
    if (deferredTimer) {
      clearTimeout(deferredTimer)
      deferredTimer = null
    }
    await pool.query('SELECT 1')
    if (!stopped) {
      deferredTimer = setTimeout(() => {
        deferredTimer = null
        shouldCreateDeferred = true
        execute()
      }, ms)
    }
  }

  const leader = createLeaderElector({
    pool,
    lock: 60004,
    poll: 10000,
    channels: [{
      channel: 'poller_test_4',
      onNotification: () => { execute() }
    }],
    log: silentLogger,
    onLeadershipChange: (isLeader) => {
      if (isLeader) {
        shouldCreateDeferred = true
        execute()
      }
    }
  })

  leader.start()
  await sleep(300)

  // At this point, initial execute ran, set deferred timer for 300ms
  // Send a NOTIFY while the timer is pending — this creates the race condition
  await pool.query("SELECT pg_notify('poller_test_4', '{}')")

  // The NOTIFY triggers execute() (either directly or via pendingNotify)
  // Then the deferred timer also fires
  // Both should result in execute() calls

  await sleep(2000)

  // We should have at least 3 dispatches:
  // 1. Initial (from onLeadershipChange) - creates deferred
  // 2. From NOTIFY
  // 3. From deferred timer - creates deferred
  // 4. From second deferred timer
  assert.ok(dispatches.length >= 3,
    `Expected at least 3 dispatches but got ${dispatches.length}: ` +
    JSON.stringify(dispatches.map(d => ({ ...d, elapsed: d.time - dispatches[0].time }))))
})
