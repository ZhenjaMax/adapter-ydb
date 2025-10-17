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
  private totalSessions = 0
  private pendingAcquires: Array<(session: ManagedSession) => void> = []

  constructor(driver: Driver, options: SessionPoolOptions = {}) {
    this.options = {
      maxSize: options.maxSize ?? 20,
      idleTimeoutMs: options.idleTimeoutMs ?? 30_000,
    }
    this.manager = new YdbSessionManager(driver)
  }

  async acquire(): Promise<{ session: SessionContext; release: ReleaseFn }> {
    const session = await this.obtainSession()

    return {
      session,
      release: this.createRelease(session),
    }
  }

  async destroy(session: SessionContext): Promise<void> {
    this.totalSessions = Math.max(0, this.totalSessions - 1)
    await this.manager.delete(session)
    await this.fulfillPending()
  }

  async drain(): Promise<void> {
    const idle = this.idleSessions
    this.idleSessions = []
    this.totalSessions = Math.max(0, this.totalSessions - idle.length)
    await Promise.all(idle.map((session) => this.manager.delete(session)))
  }

  private async obtainSession(): Promise<ManagedSession> {
    await this.evictExpired()

    const idle = this.idleSessions.pop()
    if (idle) {
      idle.lastUsed = Date.now()
      return idle
    }

    if (this.totalSessions < this.options.maxSize) {
      const created = await this.manager.create()
      this.totalSessions += 1
      return created
    }

    return this.waitForSession()
  }

  private async waitForSession(): Promise<ManagedSession> {
    return new Promise<ManagedSession>((resolve) => {
      this.pendingAcquires.push((session) => {
        session.lastUsed = Date.now()
        resolve(session)
      })
    })
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
        this.totalSessions = Math.max(0, this.totalSessions - 1)
        await this.fulfillPending()
        return
      }

      session.lastUsed = Date.now()
      const pending = this.pendingAcquires.shift()
      if (pending) {
        pending(session)
        return
      }

      this.idleSessions.push(session)
      await this.trimExcess()
    }
  }

  private async trimExcess(): Promise<void> {
    const { maxSize } = this.options
    if (this.totalSessions <= maxSize && this.idleSessions.length <= maxSize) {
      return
    }

    const overflowCount = Math.max(0, this.idleSessions.length - maxSize)
    if (overflowCount === 0) {
      return
    }

    const overflow = this.idleSessions.splice(0, overflowCount)
    this.totalSessions = Math.max(0, this.totalSessions - overflow.length)
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
      this.totalSessions = Math.max(0, this.totalSessions - expired.length)
      await Promise.all(expired.map((session) => this.manager.delete(session)))
      await this.fulfillPending()
    }
  }

  private async fulfillPending(): Promise<void> {
    while (this.pendingAcquires.length > 0) {
      const session = await this.takeIdle()
      if (session) {
        const resolve = this.pendingAcquires.shift()
        if (resolve) {
          resolve(session)
          continue
        }
        this.idleSessions.push(session)
        break
      }

      if (this.totalSessions >= this.options.maxSize) {
        break
      }

      const created = await this.manager.create()
      this.totalSessions += 1
      const resolve = this.pendingAcquires.shift()
      if (resolve) {
        created.lastUsed = Date.now()
        resolve(created)
      } else {
        this.idleSessions.push(created)
        break
      }
    }
  }
}
