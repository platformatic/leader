declare function createLeaderElector (
  options: createLeaderElector.LeaderElectorOptions
): createLeaderElector.LeaderElector

declare namespace createLeaderElector {
  export interface Pool {
    connect (): Promise<unknown>
    query (text: string, ...args: unknown[]): Promise<unknown>
  }

  export interface Logger {
    info: (...args: unknown[]) => void
    debug: (...args: unknown[]) => void
    warn: (...args: unknown[]) => void
    error: (...args: unknown[]) => void
  }

  export interface NotificationChannel<TPayload = unknown> {
    channel: string
    onNotification: (payload: TPayload) => void | Promise<void>
  }

  export interface LeaderElectorOptions {
    pool: Pool
    lock: number
    poll?: number
    channels?: NotificationChannel[]
    log?: Logger
    onLeadershipChange?: ((isLeader: boolean) => void) | null
  }

  export interface LeaderElector {
    start (): Promise<void>
    stop (): Promise<void>
    notify (payload: unknown, channelName: string): Promise<void>
    isLeader (): boolean
  }
}

export = createLeaderElector
