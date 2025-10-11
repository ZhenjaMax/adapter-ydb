import type {
  ConnectionInfo,
  IsolationLevel,
  Provider,
  SqlDriverAdapter,
  Transaction,
} from '@prisma/driver-adapter-utils'
import { DriverAdapterError } from '@prisma/driver-adapter-utils'
import { YdbClientWrapper } from './client-wrapper'
import { YdbQueryable } from './queryable'
import { PostgresCompatibilityTransaction, YdbTransaction } from './transaction'
import type { YdbTransactionIsolation, YdbTransactionMeta } from './types'

export class PrismaYdbAdapter extends YdbQueryable {
  constructor(protected ydbClient: YdbClientWrapper) {
    super(ydbClient)
  }

  getConnectionInfo(): ConnectionInfo {
    const databasePath = this.ydbClient.getDatabasePath().trim()
    const normalizedPath = databasePath.replace(/\/+/g, '/').replace(/\/+$/, '')
    const pathSegments = normalizedPath.split('/').filter(Boolean)
    const schemaName = pathSegments[pathSegments.length - 1] ?? 'root'

    const info: ConnectionInfo = {
      supportsRelationJoins: true,
    }

    info.schemaName = schemaName
    return info
  }

  async dispose(): Promise<void> {
    await this.ydbClient.close()
  }

  async executeScript(script: string): Promise<void> {
    await this.ydbClient.executeScript(script)
  }

  protected instantiateTransaction(meta: YdbTransactionMeta): YdbTransaction {
    return new YdbTransaction(meta.txId, this.ydbClient, { usePhantomQuery: true })
  }

  protected async beginTransactionMeta(isolationLevel?: IsolationLevel): Promise<YdbTransactionMeta> {
    const isolation = this.mapIsolationLevel(isolationLevel)
    return this.ydbClient.beginTransaction(isolation)
  }

  protected mapIsolationLevel(level?: IsolationLevel): YdbTransactionIsolation {
    if (!level) {
      return 'serializableReadWrite'
    }

    switch (level) {
      case 'SERIALIZABLE':
        return 'serializableReadWrite'
      case 'SNAPSHOT':
        return 'snapshotReadOnly'
      default:
        throw new DriverAdapterError({ kind: 'InvalidIsolationLevel', level })
    }
  }
}

export class PostgresCompatibilityAdapter
  extends PrismaYdbAdapter
  implements SqlDriverAdapter
{
  readonly provider: Provider = 'postgres'
  readonly adapterName = '@prisma/adapter-ydb'

  async startTransaction(isolationLevel?: IsolationLevel): Promise<Transaction> {
    const meta = await this.beginTransactionMeta(isolationLevel)
    return this.instantiateTransaction(meta) as Transaction
  }

  protected override instantiateTransaction(meta: YdbTransactionMeta): YdbTransaction {
    return new PostgresCompatibilityTransaction(meta.txId, this.ydbClient, {
      usePhantomQuery: true,
    })
  }
}
