import type { ConnectionInfo, IsolationLevel, SqlDriverAdapter, Transaction } from '@prisma/driver-adapter-utils'
import { DriverAdapterError } from '@prisma/driver-adapter-utils'
import { YdbClientWrapper } from './client-wrapper'
import { YdbQueryable } from './queryable'
import { YdbTransaction } from './transaction'
import type { YdbTransactionIsolation } from './types'

/**
 * PrismaYdbAdapter — основной адаптер между Prisma и YDB.
 * Реализует интерфейс SqlDriverAdapter и использует клиент YDB для
 * выполнения запросов, транзакций и управления соединением.
 */
export class PrismaYdbAdapter extends YdbQueryable implements SqlDriverAdapter {
  constructor(private ydbClient: YdbClientWrapper) {
    super(ydbClient)
  }

  /**
   * Запускает новую транзакцию в YDB.
   * Возвращает объект YdbTransaction, совместимый с Prisma Transaction API.
   */
  async startTransaction(isolationLevel?: IsolationLevel): Promise<Transaction> {
    const isolation = this.mapIsolationLevel(isolationLevel)
    const meta = await this.ydbClient.beginTransaction(isolation)
    return new YdbTransaction(meta.txId, this.ydbClient, { usePhantomQuery: true })
  }

  /**
   * Возвращает базовую информацию о подключении, которую запрашивает Prisma.
   */
  getConnectionInfo(): ConnectionInfo {
    const databasePath = this.ydbClient.getDatabasePath().trim()
    const normalizedPath = databasePath.replace(/\/+/g, '/').replace(/\/+$/, '')
    const pathSegments = normalizedPath.split('/').filter(Boolean)
    const schemaName = pathSegments[pathSegments.length - 1] ?? 'root';

    const info: ConnectionInfo = {
      supportsRelationJoins: true,
    }
    
    info.schemaName = schemaName
    return info
  }

  /**
   * Освобождает ресурсы клиента и закрывает соединение с YDB.
   */
  async dispose(): Promise<void> {
    await this.ydbClient.close()
  }

  /**
   * Выполняет многооператорный скрипт, разделённый ';' (грубая реализация для mock-клиента).
   */
  async executeScript(script: string): Promise<void> {
    await this.ydbClient.executeScript(script)
  }

  private mapIsolationLevel(level?: IsolationLevel): YdbTransactionIsolation {
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
