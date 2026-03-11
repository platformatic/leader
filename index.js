'use strict'

const { scheduler } = require('timers/promises')
const { on } = require('events')
const pino = require('pino')

function createLeaderElector (options) {
  const {
    pool,
    lock,
    poll = 10000,
    channels,
    log = pino(),
    onLeadershipChange = null
  } = options

  if (!pool) {
    throw new Error('pool is required')
  }

  if (!lock) {
    throw new Error('lock is required')
  }

  log.info('Acquiring advisory lock %d', lock)

  if (channels) {
    if (!Array.isArray(channels)) {
      throw new Error('channels must be an array')
    }
    for (const ch of channels) {
      if (!ch.channel) {
        throw new Error('channel is required for each notification channel')
      }
      if (!ch.onNotification) {
        throw new Error('onNotification is required for each notification channel')
      }
    }
  }

  const notificationChannels = channels || []

  let elected = false
  const abortController = new AbortController()
  let leaderLoop

  function updateLeadershipStatus (newStatus) {
    if (elected !== newStatus) {
      elected = newStatus
      if (typeof onLeadershipChange === 'function') {
        onLeadershipChange(elected)
      }
    }
  }

  async function amITheLeader () {
    const client = await pool.connect()
    try {
      while (!abortController.signal.aborted) {
        const { rows: [{ leader }] } = await client.query(
          'SELECT pg_try_advisory_lock($1) as leader',
          [lock]
        )
        if (leader && !elected) {
          log.info('This instance is the leader')
          updateLeadershipStatus(true)
          if (notificationChannels.length > 0) {
            ;(async () => {
              for (const ch of notificationChannels) {
                await client.query(`LISTEN "${ch.channel}"`)
                log.info({ channel: ch.channel }, 'Listening to notification channel')
              }

              for await (const notification of on(client, 'notification', { signal: abortController.signal })) {
                log.debug({ notification }, 'Received notification')
                try {
                  const msg = notification[0]
                  const payload = JSON.parse(msg.payload)
                  const channelName = msg.channel

                  const channelConfig = notificationChannels.find(ch => ch.channel === channelName)
                  if (channelConfig) {
                    await channelConfig.onNotification(payload)
                  } else {
                    log.warn({ channel: channelName }, 'No handler found for notification channel')
                  }
                } catch (err) {
                  log.warn({ err }, 'error while processing notification')
                }
              }
            })()
              .catch((err) => {
                if (err.name !== 'AbortError') {
                  log.error({ err }, 'Error in notification')
                } else {
                  abortController.abort()
                }
              })
          }
        } else if (leader && elected) {
          log.debug('This instance is still the leader')
        } else if (!leader && elected) {
          log.warn('This instance was the leader but is not anymore')
          updateLeadershipStatus(false)
        } else {
          log.debug('This instance is not the leader')
        }
        try {
          await scheduler.wait(poll, { signal: abortController.signal })
        } catch {
          break
        }
      }
    } finally {
      client.release()
    }
    log.debug('leader loop stopped')
  }

  function start () {
    leaderLoop = amITheLeader()
    retryLeaderLoop(leaderLoop)
    return leaderLoop
  }

  function retryLeaderLoop (loop) {
    loop.catch((err) => {
      log.error({ err }, 'Error in leader loop')
      elected = false
      return scheduler.wait(1000)
    }).then(() => {
      if (!abortController.signal.aborted) {
        leaderLoop = amITheLeader()
        retryLeaderLoop(leaderLoop)
      }
    })
  }

  async function notify (payload, channelName) {
    if (!channelName) {
      throw new Error('channelName is required')
    }

    payload = JSON.stringify(payload)

    await pool.query(`NOTIFY "${channelName}", '${payload}'`)
  }

  async function stop () {
    abortController.abort()
    if (leaderLoop) {
      await leaderLoop
    }
  }

  function isLeader () {
    return elected
  }

  return {
    start,
    stop,
    notify,
    isLeader
  }
}

module.exports = createLeaderElector
