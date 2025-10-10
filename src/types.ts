import type { ColumnType } from '@prisma/driver-adapter-utils'
import type * as Ydb from '@ydbjs/api/dist/value.js'

export interface YdbQueryOptions {
  query: string
  params?: Record<string, unknown>
  txId?: string
}

export interface YdbConnectionConfig {
  endpoint: string
  database: string
  authToken?: string | undefined
}

export interface YdbColumn {
  name: string
  type: Ydb.Type
}

export interface YdbQueryResult {
  columns: YdbColumn[]
  columnTypes: ColumnType[]
  rows: unknown[][]
  rowsAffected?: number
  stats?: Record<string, unknown>
}

export type YdbResultSet = YdbQueryResult

export type YdbTransactionIsolation = 'serializableReadWrite' | 'snapshotReadOnly'

export interface YdbTransactionMeta {
  txId: string
  sessionId: string
  nodeId: bigint
  isolation: YdbTransactionIsolation
}
