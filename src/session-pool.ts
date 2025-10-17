import { QueryServiceDefinition } from '@ydbjs/api/query'
import { StatusIds_StatusCode } from '@ydbjs/api/operation'
import { YDBError } from '@ydbjs/error'
import type { Driver } from '@ydbjs/core'

export type SessionContext = {
  sessionId: string
  nodeId: bigint
}

export type SessionPoolOptions = {
  maxSize?: number
  idleTimeoutMs?: number
}

type ManagedSession = SessionContext & {
  lastUsed: number
}

type ReleaseFn = (error?: unknown) => Promise<void>

class YdbSessionManager {
  constructor(private readonly driver: Driver) {}

  async create(): Promise<ManagedSession> {
    const client = this.driver.createClient(QueryServiceDefinition)

    const sessionResponse = await client.createSession({})
    if (sessionResponse.status !== StatusIds_StatusCode.SUCCESS) {
      throw new YDBError(sessionResponse.status, sessionResponse.issues)
    }

    const sessionId = sessionResponse.sessionId
    const nodeId = sessionResponse.nodeId

    const attachClient = this.driver.createClient(QueryServiceDefinition, nodeId)
    const iterator = attachClient.attachSession({ sessionId }, {})[Symbol.asyncIterator]()
    const attachResult = await iterator.next()
    if (attachResult.value.status !== StatusIds_StatusCode.SUCCESS) {
      throw new YDBError(attachResult.value.status, attachResult.value.issues)
    }

    return { sessionId, nodeId, lastUsed: Date.now() }
  }

  async delete(session: SessionContext): Promise<void> {
    try {
      const client = this.driver.createClient(QueryServiceDefinition, session.nodeId)
      await client.deleteSession({ sessionId: session.sessionId })
    } catch (error) {
      // ignore errors related to already closed sessions
    }
  }
}

export class YdbSessionPool {
  private readonly options: Required<SessionPoolOptions>
  private readonly manager: YdbSessionManager
  private idleSessions: ManagedSession[] = []

  constructor(driver: Driver, options: SessionPoolOptions = {}) {
    this.options = {
      maxSize: options.maxSize ?? 20,
      idleTimeoutMs: options.idleTimeoutMs ?? 30_000,
    }
    this.manager = new YdbSessionManager(driver)
  }

  async acquire(): Promise<{ session: SessionContext; release: ReleaseFn }> {
    await this.evictExpired()

    const session = (await this.takeIdle()) ?? (await this.manager.create())

    return {
      session,
      release: this.createRelease(session),
    }
  }

  async destroy(session: SessionContext): Promise<void> {
    await this.manager.delete(session)
  }

  async drain(): Promise<void> {
    const idle = this.idleSessions
    this.idleSessions = []
    await Promise.all(idle.map((session) => this.manager.delete(session)))
  }

  private async takeIdle(): Promise<ManagedSession | undefined> {
    const session = this.idleSessions.pop()
    if (session) {
      session.lastUsed = Date.now()
    }
    return session
  }

  private createRelease(session: ManagedSession): ReleaseFn {
    let released = false
    return async (error?: unknown) => {
      if (released) return
      released = true

      if (error) {
        await this.manager.delete(session)
        return
      }

      session.lastUsed = Date.now()
      this.idleSessions.push(session)
      await this.trimExcess()
    }
  }

  private async trimExcess(): Promise<void> {
    const { maxSize } = this.options
    if (this.idleSessions.length <= maxSize) {
      return
    }

    const overflow = this.idleSessions.splice(0, this.idleSessions.length - maxSize)
    await Promise.all(overflow.map((session) => this.manager.delete(session)))
  }

  private async evictExpired(): Promise<void> {
    const now = Date.now()
    const { idleTimeoutMs } = this.options

    if (this.idleSessions.length === 0) return

    const active: ManagedSession[] = []
    const expired: ManagedSession[] = []

    for (const session of this.idleSessions) {
      if (now - session.lastUsed > idleTimeoutMs) {
        expired.push(session)
      } else {
        active.push(session)
      }
    }

    this.idleSessions = active

    if (expired.length > 0) {
      await Promise.all(expired.map((session) => this.manager.delete(session)))
    }
  }
}
