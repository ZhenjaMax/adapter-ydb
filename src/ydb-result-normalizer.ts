import { StatusIds_StatusCode } from '@ydbjs/api/operation'
import type { QueryStats, TableAccessStats } from '@ydbjs/api/query'
import { fromYdb, toJs } from '@ydbjs/value'
import type { ColumnType } from '@prisma/driver-adapter-utils'
import { YqlTypeMapper } from './yql-conversion.js'
import type { YdbColumn, YdbQueryResult } from './types.js'
import { YDBError } from '@ydbjs/error'

export class YdbResultNormalizer {
  async collect(stream: AsyncIterable<any>): Promise<YdbQueryResult> {
    const columns: YdbColumn[] = []
    const columnTypes: ColumnType[] = []
    const rows: unknown[][] = []
    let rowsAffected: number | undefined
    let stats: Record<string, unknown> | undefined

    for await (const part of stream) {
      if (part.status !== StatusIds_StatusCode.SUCCESS) {
        throw new YDBError(part.status, part.issues)
      }

      if (!part.resultSet) {
        if (part.execStats) {
          rowsAffected = this.calculateRowsAffected(part.execStats)
          stats = this.normalizeStats(part.execStats)
        }
        continue
      }

      if (columns.length === 0) {
        for (const column of part.resultSet.columns) {
          if (!column.type) {
            throw new Error(`YDB returned column without type metadata: ${column.name}`)
          }
          const columnMeta: YdbColumn = {
            name: column.name,
            type: column.type,
          }
          columns.push(columnMeta)
          columnTypes.push(YqlTypeMapper.toPrismaColumnType(column.type))
        }
      }

      const columnSchemas = part.resultSet.columns

      for (const row of part.resultSet.rows) {
        const normalizedRow: unknown[] = []

        row.items.forEach((value: unknown, index: number) => {
          const columnSchema = columnSchemas[index]
          if (!columnSchema) {
            throw new Error(`Missing column metadata for index ${index}`)
          }
          const ydbType = columnSchema.type
          if (!ydbType) {
            throw new Error(`Missing type for column index ${index}`)
          }

          const ydbValue = fromYdb(value as any, ydbType)
          const columnType = columnTypes[index] ?? YqlTypeMapper.toPrismaColumnType(ydbType)
          const jsValue = toJs(ydbValue)
          normalizedRow.push(YqlTypeMapper.normalizeValue(jsValue, columnType))
        })

        rows.push(normalizedRow)
      }

      if (part.execStats) {
        rowsAffected = this.calculateRowsAffected(part.execStats)
        stats = this.normalizeStats(part.execStats)
      }
    }

    const result: YdbQueryResult = {
      columns,
      columnTypes,
      rows,
    }

    if (rowsAffected !== undefined) {
      result.rowsAffected = rowsAffected
    }

    if (stats) {
      result.stats = stats
    }

    return result
  }

  private calculateRowsAffected(execStats: QueryStats): number | undefined {
    let total = 0n

    for (const phase of execStats.queryPhases ?? []) {
      for (const table of phase.tableAccess ?? []) {
        total += this.getOperationRows(table.updates)
        total += this.getOperationRows(table.deletes)
      }
    }

    if (total === 0n) {
      return 0
    }

    if (total > BigInt(Number.MAX_SAFE_INTEGER)) {
      return Number.MAX_SAFE_INTEGER
    }

    return Number(total)
  }

  private getOperationRows(operation?: TableAccessStats['updates']): bigint {
    if (!operation?.rows) {
      return 0n
    }
    return BigInt(operation.rows)
  }

  private normalizeStats(execStats: QueryStats): Record<string, unknown> {
    return this.normalizeValue(execStats) as Record<string, unknown>
  }

  private normalizeValue(value: unknown): unknown {
    if (typeof value === 'bigint') {
      return value.toString()
    }

    if (Array.isArray(value)) {
      return value.map((item) => this.normalizeValue(item))
    }

    if (value && typeof value === 'object') {
      const normalized: Record<string, unknown> = {}
      for (const [key, nested] of Object.entries(value as Record<string, unknown>)) {
        if (typeof nested === 'function') continue
        normalized[key] = this.normalizeValue(nested)
      }
      return normalized
    }

    return value
  }
}
