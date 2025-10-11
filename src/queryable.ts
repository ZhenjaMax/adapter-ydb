import type { SqlQuery, SqlResultSet } from '@prisma/driver-adapter-utils'
import type { YdbQueryResult } from './types'
import { YdbClientWrapper } from './client-wrapper'
import { YdbErrorMapper } from './error-mapper'

/**
 * YdbQueryable — базовый слой выполнения запросов YQL.
 * Выполняет запросы через YdbClientWrapper и конвертирует результаты
 * в формат, ожидаемый Prisma (SqlResultSet).
 */
export class YdbQueryable {
  readonly provider: string = 'ydb'
  readonly adapterName = '@prisma/adapter-ydb'

  constructor(protected readonly client: YdbClientWrapper, protected readonly txId?: string) {}

  /**
   * Выполняет запрос (SELECT / FETCH и т.д.), возвращающий данные.
   * Возвращает результат в формате SqlResultSet.
  */
  async queryRaw(query: SqlQuery): Promise<SqlResultSet> {
    try {
      const result: YdbQueryResult = await this.client.executeQuery(query, this.txId)

      return {
        columnNames: result.columns.map((c) => c.name),
        columnTypes: result.columnTypes,
        rows: result.rows,
      }
    } catch (err) {
      throw YdbErrorMapper.toPrismaError(err)
    }
  }

  /**
   * Выполняет команду, не возвращающую данных (INSERT / UPDATE / DELETE).
   * Возвращает количество затронутых строк, если известно.
  */
  async executeRaw(query: SqlQuery): Promise<number> {
    try {
      const result: YdbQueryResult = await this.client.executeQuery(query, this.txId)

      // В YDB явного rowsAffected нет, но можно попытаться взять из статистики.
      // Для MVP просто возвращаем 0 или количество строк в result.rows (если есть).
      if (typeof result.rowsAffected === 'number') return result.rowsAffected
      if (Array.isArray(result.rows) && result.rows.length > 0) return result.rows.length
      return 0
    } catch (err) {
      throw YdbErrorMapper.toPrismaError(err)
    }
  }
}
