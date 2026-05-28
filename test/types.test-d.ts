import createLeaderElector = require('../index')

const fakePool: createLeaderElector.Pool = {
  connect: async () => ({}),
  query: async () => ({})
}

const elector: createLeaderElector.LeaderElector = createLeaderElector({
  pool: fakePool,
  lock: 42,
  poll: 1000,
  channels: [
    {
      channel: 'deferred_messages',
      onNotification: async (payload: unknown) => { void payload }
    }
  ],
  log: {
    info: () => {},
    debug: () => {},
    warn: () => {},
    error: () => {}
  },
  onLeadershipChange: (isLeader: boolean) => { void isLeader }
})

const _minimal: createLeaderElector.LeaderElector = createLeaderElector({
  pool: fakePool,
  lock: 1
})

const _start: Promise<void> = elector.start()
const _stop: Promise<void> = elector.stop()
const _notify: Promise<void> = elector.notify({ id: 1 }, 'channel')
const _isLeader: boolean = elector.isLeader()
void _minimal; void _start; void _stop; void _notify; void _isLeader
