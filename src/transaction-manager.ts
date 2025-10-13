import { QueryServiceDefinition } from '@ydbjs/api/query'
import { StatusIds_StatusCode } from '@ydbjs/api/operation'
import type { Driver } from '@ydbjs/core'
import { YDBError } from '@ydbjs/error'
import type { SessionContext } from './session-pool.js'
import { YdbSessionPool } from './session-pool.js'
import type { YdbTransactionIsolation, YdbTransactionMeta } from './types.js'

export type TransactionContext = YdbTransactionMeta & {
  session: SessionContext
  release: (error?: unknown) => Promise<void>
}

export class YdbTransactionManager {
  private readonly transactions = new Map<string, TransactionContext>()

  constructor(private readonly driver: Driver, private readonly sessionPool: YdbSessionPool) {}

  async begin(isolation: YdbTransactionIsolation): Promise<TransactionContext> {
    const { session, release } = await this.sessionPool.acquire()

    try {
      const client = this.createClient(session.nodeId)
      const beginResult = await client.beginTransaction({
        sessionId: session.sessionId,
        txSettings: { txMode: { case: isolation, value: {} } },
      })

      if (beginResult.status !== StatusIds_StatusCode.SUCCESS || !beginResult.txMeta?.id) {
        throw new YDBError(beginResult.status, beginResult.issues)
      }

      const context: TransactionContext = {
        txId: beginResult.txMeta.id,
        sessionId: session.sessionId,
        nodeId: session.nodeId,
        isolation,
        session,
        release,
      }

      this.transactions.set(context.txId, context)
      return context
    } catch (error) {
      await release(error)
      throw error
    }
  }

  require(txId: string): TransactionContext {
    const tx = this.transactions.get(txId)
    if (!tx) {
      throw new Error(`Transaction ${txId} was not found or already finished.`)
    }
    return tx
  }

  async commit(txId: string): Promise<void> {
    const tx = this.require(txId)
    const client = this.createClient(tx.nodeId)

    try {
      const result = await client.commitTransaction({
        sessionId: tx.sessionId,
        txId: tx.txId,
      })

      if (result.status !== StatusIds_StatusCode.SUCCESS) {
        throw new YDBError(result.status, result.issues)
      }
    } finally {
      await this.finish(txId)
    }
  }

  async rollback(txId: string): Promise<void> {
    const tx = this.require(txId)
    const client = this.createClient(tx.nodeId)

    try {
      await client.rollbackTransaction({
        sessionId: tx.sessionId,
        txId: tx.txId,
      })
    } finally {
      await this.finish(txId)
    }
  }

  async dispose(): Promise<void> {
    const active = Array.from(this.transactions.keys())
    await Promise.all(active.map((txId) => this.finish(txId, true)))
  }

  private async finish(txId: string, forceError = false): Promise<void> {
    const tx = this.transactions.get(txId)
    if (!tx) {
      return
    }

    this.transactions.delete(txId)
    await tx.release(forceError ? new Error('force dispose') : undefined)
  }

  private createClient(nodeId?: bigint) {
    return this.driver.createClient(QueryServiceDefinition, nodeId)
  }
}
