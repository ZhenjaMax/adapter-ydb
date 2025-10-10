// Note: We intentionally avoid importing prisma adapter types here
// to keep local YDB types independent from the adapter-utils package.

export interface YdbQueryOptions {
  query: string
  params?: Record<string, any>
  txId?: string
}

export interface YdbConnectionConfig {
  endpoint: string
  database: string
  authToken?: string
}

export type YdbResultSet = {
  columns: YdbColumn[]
  rows: any[][]
  rowsAffected?: number
}

/**
 * Общие типы для YDB адаптера.
 * Описывают результат выполнения запросов, структуру колонок и статистику.
 */

/**
 * Метаданные одной колонки результата.
 * Поле `type` — строковое обозначение типа YQL (например, "Int64", "Utf8", "JsonDocument").
 */
export interface YdbColumn {
  name: string
  type: string
}

/**
 * Результат выполнения YQL-запроса.
 * Обычно представляет собой десериализованный `Ydb.ResultSet` из API YDB.
 */
export interface YdbQueryResult {
  /** Список колонок с именами и типами */
  columns: YdbColumn[]
  /** Массив строк, каждая строка — массив значений */
  rows: any[][]
  /**
   * Количество затронутых строк (для DML-команд),
   * не является частью официального протокола, но может быть вычислено из статистики.
   */
  rowsAffected?: number
  /**
   * Дополнительная статистика (время выполнения, количество сканов и т.д.)
   * — по возможности копируется из `QueryStats` API YDB.
   */
  stats?: Record<string, any>
}
