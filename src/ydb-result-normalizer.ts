import { StatusIds_StatusCode } from '@ydbjs/api/operation'
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

    for await (const part of stream) {
      if (part.status !== StatusIds_StatusCode.SUCCESS) {
        throw new YDBError(part.status, part.issues)
      }

      if (!part.resultSet) {
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
    }

    return {
      columns,
      columnTypes,
      rows,
      rowsAffected: 0,
    }
  }
}
