import type { ConnectionInfo, IsolationLevel, SqlDriverAdapter } from '@prisma/driver-adapter-utils'
import { YdbClientWrapper } from './client-wrapper'
import { YdbQueryable } from './queryable'
import { YdbTransaction } from './transaction'

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
  async startTransaction(_isolationLevel?: IsolationLevel): Promise<YdbTransaction> {
    const txId = await this.ydbClient.beginTransaction()
    return new YdbTransaction(txId, this.ydbClient)
  }

  /**
   * Возвращает базовую информацию о подключении, которую запрашивает Prisma.
   */
  getConnectionInfo(): ConnectionInfo {
    return {
      schemaName: 'public', // логическое пространство схемы, Prisma требует это поле
      supportsRelationJoins: true // Prisma может использовать JOIN между таблицами
    }
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
    const statements = script
      .split(';')
      .map((s) => s.trim())
      .filter(Boolean)

    for (const stmt of statements) {
      await this.ydbClient.executeQuery(stmt)
    }
  }
}
