import type { SqlQuery, SqlResultSet } from '@prisma/driver-adapter-utils'
import type { YdbQueryResult } from './types.js'
import { YdbClientWrapper } from './client-wrapper.js'
import { YdbErrorMapper } from './error-mapper.js'

export class YdbQueryable {
  readonly provider: string = 'ydb'
  readonly adapterName = '@prisma/adapter-ydb'

  constructor(protected readonly client: YdbClientWrapper, protected readonly txId?: string) {}

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

  async executeRaw(query: SqlQuery): Promise<number> {
    try {
      const result: YdbQueryResult = await this.client.executeQuery(query, this.txId)

      if (typeof result.rowsAffected === 'number') return result.rowsAffected
      if (Array.isArray(result.rows) && result.rows.length > 0) return result.rows.length
      return 0
    } catch (err) {
      throw YdbErrorMapper.toPrismaError(err)
    }
  }
}
