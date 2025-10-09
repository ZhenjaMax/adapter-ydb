import type { SqlQuery, SqlResultSet } from '@prisma/driver-adapter-utils'
import type { YdbQueryResult, YdbColumn } from './types'
import { YdbClientWrapper } from './client-wrapper'
import { YqlTypeMapper } from './yql-conversion'
import { YdbErrorMapper } from './error-mapper'

/**
 * YdbQueryable — базовый слой выполнения запросов YQL.
 * Выполняет запросы через YdbClientWrapper и конвертирует результаты
 * в формат, ожидаемый Prisma (SqlResultSet).
 */
export class YdbQueryable {
  readonly provider = 'ydb'
  readonly adapterName = '@prisma/adapter-ydb'

  constructor(protected client: YdbClientWrapper, protected txId?: string) {}

  /**
   * Выполняет запрос (SELECT / FETCH и т.д.), возвращающий данные.
   * Возвращает результат в формате SqlResultSet.
   */
  async queryRaw(query: SqlQuery): Promise<SqlResultSet> {
    try {
      const sql = this.compileSql(query)
      const result: YdbQueryResult = await this.client.executeQuery(sql, undefined, this.txId)

      return {
        columnNames: result.columns.map((c: YdbColumn) => c.name),
        columnTypes: result.columns.map((c: YdbColumn) => YqlTypeMapper.toPrismaColumnType(c.type)),
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
      const sql = this.compileSql(query)
      const result: YdbQueryResult = await this.client.executeQuery(sql, undefined, this.txId)

      // В YDB явного rowsAffected нет, но можно попытаться взять из статистики.
      // Для MVP просто возвращаем 0 или количество строк в result.rows (если есть).
      if (typeof result.rowsAffected === 'number') return result.rowsAffected
      if (Array.isArray(result.rows) && result.rows.length > 0) return result.rows.length
      return 0
    } catch (err) {
      throw YdbErrorMapper.toPrismaError(err)
    }
  }

  /**
   * Компилирует SqlQuery от Prisma в плоскую строку YQL с инлайном параметров.
   * Для упрощённого in-memory клиента поддерживаем плейсхолдеры вида $1, $2, ... и '?'.
   */
  protected compileSql(query: SqlQuery): string {
    let text = query.sql
    // Предпочтительно: $1, $2 ...
    for (let i = 0; i < query.args.length; i++) {
      const placeholder = new RegExp(`\\$${i + 1}(?!\\d)`, 'g')
      const lit = this.toLiteral(query.args[i])
      text = text.replace(placeholder, lit)
    }
    // Резервно: '?' — заменяем по порядку
    if (text.includes('?') && query.args.length > 0) {
      let idx = 0
      text = text.replace(/\?/g, () => {
        const lit = this.toLiteral(query.args[idx])
        idx = Math.min(idx + 1, query.args.length - 1)
        return lit
      })
    }
    return text
  }

  private toLiteral(value: unknown): string {
    if (value === null || value === undefined) return 'NULL'
    if (typeof value === 'string') {
      // Упростим: удалим двойные кавычки, чтобы не ломать парсер CSV в моке
      const safe = value.replace(/"/g, '')
      return `"${safe}"`
    }
    if (typeof value === 'number') return String(value)
    if (typeof value === 'boolean') return value ? 'TRUE' : 'FALSE'
    if (value instanceof Date) return `"${value.toISOString()}"`
    if (value instanceof Uint8Array || (typeof Buffer !== 'undefined' && Buffer.isBuffer(value))) {
      return `"${Array.from(value as any).join(',')}"`
    }
    // Объекты сериализуем в JSON-строку
    try {
      const json = JSON.stringify(value)
      return `"${(json ?? '').replace(/"/g, '')}"`
    } catch {
      return `"${String(value).replace(/"/g, '')}"`
    }
  }
}
