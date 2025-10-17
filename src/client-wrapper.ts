import type { SqlQuery } from '@prisma/driver-adapter-utils'
import { AccessTokenCredentialsProvider } from '@ydbjs/auth/access-token'
import { Driver, type DriverOptions } from '@ydbjs/core'
import { QueryServiceDefinition, ExecMode, Syntax, StatsMode } from '@ydbjs/api/query'
import { StatusIds_StatusCode } from '@ydbjs/api/operation'
import { YDBError } from '@ydbjs/error'
import { YqlQueryPreparer } from './yql-query-preparer.js'
import { YdbResultNormalizer } from './ydb-result-normalizer.js'
import { YdbSessionPool, type SessionPoolOptions, type SessionContext } from './session-pool.js'
import { YdbTransactionManager } from './transaction-manager.js'
import type { YdbConnectionConfig, YdbQueryResult, YdbTransactionIsolation, YdbTransactionMeta } from './types.js'

export class YdbClientWrapper {
  private driver: Driver | null = null
  private connected = false
  private sessionPool: YdbSessionPool | null = null
  private transactionManager: YdbTransactionManager | null = null
  private readonly queryPreparer = new YqlQueryPreparer()
  private readonly resultNormalizer = new YdbResultNormalizer()

  constructor(private readonly config: YdbConnectionConfig, private readonly poolOptions: SessionPoolOptions = {}) {}

  async connect(): Promise<void> {
    if (this.connected) return

    const connectionString = this.buildConnectionString()
    const driverOptions = this.createDriverOptions()
    const driver = new Driver(connectionString, driverOptions)
    await driver.ready()

    this.driver = driver
    this.sessionPool = new YdbSessionPool(driver, this.poolOptions)
    this.transactionManager = new YdbTransactionManager(driver, this.sessionPool)
    this.connected = true
  }

  async executeQuery(query: SqlQuery, txId?: string): Promise<YdbQueryResult> {
    const driver = this.ensureDriver()
    const sessionPool = this.ensureSessionPool()
    const transactionManager = this.ensureTransactionManager()

    const prepared = this.queryPreparer.prepare(query)
    const encodedParams = this.queryPreparer.encodeParameters(prepared.parameters)

    let session: SessionContext
    let release: ((error?: unknown) => Promise<void>) | undefined

    if (txId) {
      session = transactionManager.require(txId).session
    } else {
      const acquired = await sessionPool.acquire()
      session = acquired.session
      release = acquired.release
    }

    const client = driver.createClient(QueryServiceDefinition, session.nodeId)

    const request: Record<string, unknown> = {
      sessionId: session.sessionId,
      execMode: ExecMode.EXECUTE,
      query: {
        case: 'queryContent',
        value: {
          syntax: Syntax.YQL_V1,
          text: prepared.text,
        },
      },
      parameters: encodedParams,
      statsMode: StatsMode.BASIC,
    }

    const txControl = this.createTxControl(txId, prepared.text)
    if (txControl) {
      request.txControl = txControl
    }

    const stream = client.executeQuery(request)

    try {
      return await this.resultNormalizer.collect(stream)
    } catch (error) {
      if (release) await release(error)
      throw error
    } finally {
      if (release) await release()
    }
  }

  async executeScript(script: string): Promise<void> {
    const driver = this.ensureDriver()
    const sessionPool = this.ensureSessionPool()

    const { session, release } = await sessionPool.acquire()
    const client = driver.createClient(QueryServiceDefinition, session.nodeId)

    try {
      const stream = client.executeQuery({
        sessionId: session.sessionId,
        execMode: ExecMode.EXECUTE,
        query: {
          case: 'queryContent',
          value: {
            syntax: Syntax.YQL_V1,
            text: script,
          },
        },
        statsMode: StatsMode.NONE,
      })

      for await (const part of stream) {
        if (part.status !== StatusIds_StatusCode.SUCCESS) {
          throw new YDBError(part.status, part.issues)
        }
      }
    } catch (error) {
      await release(error)
      throw error
    } finally {
      await release()
    }
  }

  async beginTransaction(isolation: YdbTransactionIsolation): Promise<YdbTransactionMeta> {
    const manager = this.ensureTransactionManager()
    const context = await manager.begin(isolation)
    const { session: _session, release: _release, ...meta } = context
    return meta
  }

  async commitTransaction(txId: string): Promise<void> {
    const manager = this.ensureTransactionManager()
    await manager.commit(txId)
  }

  async rollbackTransaction(txId: string): Promise<void> {
    const manager = this.ensureTransactionManager()
    await manager.rollback(txId)
  }

  getDatabasePath(): string {
    const driver = this.driver
    if (driver) {
      return driver.database
    }
    return this.config.database
  }

  async close(): Promise<void> {
    await this.transactionManager?.dispose()
    await this.sessionPool?.drain()

    if (this.driver) {
      this.driver.close()
    }

    this.driver = null
    this.sessionPool = null
    this.transactionManager = null
    this.connected = false
  }

  private buildConnectionString(): string {
    const { endpoint, database } = this.config
    const normalizedDatabase = database.startsWith('/') ? database : `/${database}`
    return `${endpoint}${normalizedDatabase}`
  }

  private createDriverOptions(): DriverOptions | undefined {
    const options: DriverOptions = {}

    if (this.config.authToken) {
      options.credentialsProvider = new AccessTokenCredentialsProvider({
        token: this.config.authToken,
      })
    }

    return Object.keys(options).length > 0 ? options : undefined
  }

  private ensureDriver(): Driver {
    const driver = this.driver
    if (!driver || !this.connected) {
      throw new Error('YDB client is not connected. Call connect() first.')
    }
    return driver
  }

  private ensureSessionPool(): YdbSessionPool {
    if (!this.sessionPool) {
      throw new Error('Session pool is not initialized. Call connect() first.')
    }
    return this.sessionPool
  }

  private ensureTransactionManager(): YdbTransactionManager {
    if (!this.transactionManager) {
      throw new Error('Transaction manager is not initialized. Call connect() first.')
    }
    return this.transactionManager
  }

  private createTxControl(txId: string | undefined, queryText: string) {
    if (txId) {
      return {
        txSelector: {
          case: 'txId',
          value: txId,
        },
        commitTx: false,
      }
    }

    if (this.isSchemaOperation(queryText)) {
      return undefined
    }

    return {
      txSelector: {
        case: 'beginTx',
        value: {
          txMode: {
            case: 'serializableReadWrite',
            value: {},
          },
        },
      },
      commitTx: true,
    }
  }

  private isSchemaOperation(queryText: string): boolean {
    const normalized = queryText.trim().toUpperCase()
    if (!normalized) return false

    const firstToken = normalized.split(/\s+/, 1)[0]

    switch (firstToken) {
      case 'CREATE':
      case 'DROP':
      case 'ALTER':
      case 'RENAME':
      case 'TRUNCATE':
      case 'GRANT':
      case 'REVOKE':
      case 'PRAGMA':
      case 'USE':
        return true
      default:
        return false
    }
  }
}
