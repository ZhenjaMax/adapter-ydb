import { ColumnTypeEnum } from '@prisma/driver-adapter-utils'

/**
 * YqlTypeMapper — преобразование типов YQL ↔ Prisma ↔ JS
 */
export class YqlTypeMapper {
  /**
   * Преобразует имя типа YDB в ColumnTypeEnum Prisma.
   */
  static toPrismaColumnType(ydbType: string): (typeof ColumnTypeEnum)[keyof typeof ColumnTypeEnum] {
    switch (ydbType) {
      case 'Utf8':
      case 'String':
      case 'Text':
        return ColumnTypeEnum.Text

      case 'Int8':
      case 'Int16':
      case 'Int32':
      case 'Uint8':
      case 'Uint16':
      case 'Uint32':
        return ColumnTypeEnum.Int32

      case 'Int64':
      case 'Uint64':
        return ColumnTypeEnum.Int64

      case 'Bool':
        return ColumnTypeEnum.Boolean

      case 'Float':
        return ColumnTypeEnum.Float

      case 'Double':
        return ColumnTypeEnum.Double

      case 'Decimal':
        return ColumnTypeEnum.Numeric

      case 'Date':
        return ColumnTypeEnum.Date

      case 'Datetime':
      case 'Timestamp':
        return ColumnTypeEnum.DateTime

      case 'Json':
      case 'JsonDocument':
        return ColumnTypeEnum.Json

      case 'Bytes':
      case 'DyNumber':
        return ColumnTypeEnum.Bytes

      default:
        return ColumnTypeEnum.Text
    }
  }

  /**
   * JS → YQL-параметры (для DECLARE $param AS Type)
   */
  static toYdbParameter(value: any): any {
    if (value === null || value === undefined) return null

    const t = typeof value
    switch (t) {
      case 'string':
      case 'boolean':
        return value
      case 'number':
        return Number.isSafeInteger(value) ? value : String(value)
      case 'bigint':
        return value.toString()
      case 'object':
        if (value instanceof Date) return value.toISOString()
        if (Array.isArray(value)) return value.map(v => this.toYdbParameter(v))
        if (value instanceof Uint8Array || Buffer.isBuffer(value)) return Array.from(value)
        return JSON.stringify(value)
      default:
        return value
    }
  }

  /**
   * YQL → JS (для возврата Prisma)
   */
  static fromYdbValue(value: any, ydbType?: string): any {
    if (value === null || value === undefined) return null

    switch (ydbType) {
      case 'Int64':
      case 'Uint64':
        const num = Number(value)
        return Number.isSafeInteger(num) ? num : String(value)
      case 'Bool':
        return Boolean(value)
      case 'Json':
      case 'JsonDocument':
        return typeof value === 'string' ? value : JSON.stringify(value)
      case 'Timestamp':
      case 'Datetime':
      case 'Date':
        return typeof value === 'string' ? value : new Date(value).toISOString()
      case 'Bytes':
      case 'String':
        if (typeof value === 'string') return value
        if (Array.isArray(value)) return new Uint8Array(value)
        return value
      default:
        return value
    }
  }
}
