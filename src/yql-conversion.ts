import type * as Ydb from '@ydbjs/api/value'
import { Type_PrimitiveTypeId } from '@ydbjs/api/value'
import { ColumnTypeEnum, type ColumnType } from '@prisma/driver-adapter-utils'

type PrimitiveTypeId = Type_PrimitiveTypeId

const ARRAY_TYPE_MAP = new Map<ColumnType, ColumnType>([
  [ColumnTypeEnum.Int32, ColumnTypeEnum.Int32Array],
  [ColumnTypeEnum.Int64, ColumnTypeEnum.Int64Array],
  [ColumnTypeEnum.Float, ColumnTypeEnum.FloatArray],
  [ColumnTypeEnum.Double, ColumnTypeEnum.DoubleArray],
  [ColumnTypeEnum.Numeric, ColumnTypeEnum.NumericArray],
  [ColumnTypeEnum.Boolean, ColumnTypeEnum.BooleanArray],
  [ColumnTypeEnum.Character, ColumnTypeEnum.CharacterArray],
  [ColumnTypeEnum.Text, ColumnTypeEnum.TextArray],
  [ColumnTypeEnum.Date, ColumnTypeEnum.DateArray],
  [ColumnTypeEnum.Time, ColumnTypeEnum.TimeArray],
  [ColumnTypeEnum.DateTime, ColumnTypeEnum.DateTimeArray],
  [ColumnTypeEnum.Json, ColumnTypeEnum.JsonArray],
  [ColumnTypeEnum.Bytes, ColumnTypeEnum.BytesArray],
  [ColumnTypeEnum.Uuid, ColumnTypeEnum.UuidArray],
])

function unwrapOptional(type: Ydb.Type): Ydb.Type {
  let current = type
  while (current.type?.case === 'optionalType') {
    current = current.type.value.item as Ydb.Type
  }
  return current
}

function mapPrimitive(typeId: PrimitiveTypeId): ColumnType {
  switch (typeId) {
    case Type_PrimitiveTypeId.BOOL:
      return ColumnTypeEnum.Boolean
    case Type_PrimitiveTypeId.INT8:
    case Type_PrimitiveTypeId.INT16:
    case Type_PrimitiveTypeId.INT32:
    case Type_PrimitiveTypeId.UINT8:
    case Type_PrimitiveTypeId.UINT16:
    case Type_PrimitiveTypeId.UINT32:
      return ColumnTypeEnum.Int32
    case Type_PrimitiveTypeId.INT64:
    case Type_PrimitiveTypeId.UINT64:
      return ColumnTypeEnum.Int64
    case Type_PrimitiveTypeId.FLOAT:
      return ColumnTypeEnum.Float
    case Type_PrimitiveTypeId.DOUBLE:
      return ColumnTypeEnum.Double
    case Type_PrimitiveTypeId.DATE:
      return ColumnTypeEnum.Date
    case Type_PrimitiveTypeId.DATETIME:
    case Type_PrimitiveTypeId.TIMESTAMP:
    case Type_PrimitiveTypeId.TZ_DATETIME:
    case Type_PrimitiveTypeId.TZ_TIMESTAMP:
      return ColumnTypeEnum.DateTime
    case Type_PrimitiveTypeId.STRING:
    case Type_PrimitiveTypeId.YSON:
      return ColumnTypeEnum.Bytes
    case Type_PrimitiveTypeId.UTF8:
      return ColumnTypeEnum.Text
    case Type_PrimitiveTypeId.JSON:
    case Type_PrimitiveTypeId.JSON_DOCUMENT:
      return ColumnTypeEnum.Json
    case Type_PrimitiveTypeId.UUID:
      return ColumnTypeEnum.Uuid
    case Type_PrimitiveTypeId.DYNUMBER:
      return ColumnTypeEnum.Numeric
    default:
      return ColumnTypeEnum.Text
  }
}

function promoteToArray(baseType: ColumnType): ColumnType {
  return ARRAY_TYPE_MAP.get(baseType) ?? ColumnTypeEnum.TextArray
}

function isArrayColumn(columnType: ColumnType): boolean {
  return ARRAY_TYPE_MAP.has(columnType)
}

function normalizeNumberLike(value: unknown): number | string | unknown {
  if (typeof value === 'bigint') {
    const coerced = Number(value)
    return Number.isSafeInteger(coerced) ? coerced : value.toString()
  }
  if (typeof value === 'string') {
    const numeric = Number(value)
    return Number.isFinite(numeric) ? numeric : value
  }
  return value
}

export class YqlTypeMapper {
  static toPrismaColumnType(type: Ydb.Type): ColumnType {
    const withoutOptional = unwrapOptional(type)

    switch (withoutOptional.type?.case) {
      case 'typeId':
        return mapPrimitive(withoutOptional.type.value)
      case 'listType': {
        const inner = withoutOptional.type.value.item as Ydb.Type
        const innerColumn = this.toPrismaColumnType(inner)
        return promoteToArray(innerColumn)
      }
      default:
        return ColumnTypeEnum.Text
    }
  }

  static normalizeValue(value: unknown, columnType: ColumnType): unknown {
    if (value === null || value === undefined) {
      return null
    }

    if (isArrayColumn(columnType) && Array.isArray(value)) {
      const baseType = [...ARRAY_TYPE_MAP.entries()].find(([, arrayType]) => arrayType === columnType)?.[0]
      const effectiveBase = baseType ?? ColumnTypeEnum.Text
      return value.map((item) => this.normalizeValue(item, effectiveBase))
    }

    switch (columnType) {
      case ColumnTypeEnum.Int32:
      case ColumnTypeEnum.Float:
      case ColumnTypeEnum.Double:
      case ColumnTypeEnum.Numeric:
        return normalizeNumberLike(value)
      case ColumnTypeEnum.Int64: {
        if (typeof value === 'bigint') {
          const coerced = Number(value)
          return Number.isSafeInteger(coerced) ? coerced : value.toString()
        }
        return value
      }
      case ColumnTypeEnum.Boolean:
        return Boolean(value)
      case ColumnTypeEnum.Bytes:
        if (value instanceof Uint8Array) {
          return Buffer.from(value)
        }
        if (Array.isArray(value) && value.every((v) => typeof v === 'number')) {
          return Buffer.from(value)
        }
        return value
      case ColumnTypeEnum.Json:
        if (typeof value === 'string') return value
        try {
          return JSON.stringify(value)
        } catch {
          return String(value)
        }
      case ColumnTypeEnum.Date:
        if (value instanceof Date) {
          return value.toISOString().split('T')[0] ?? value.toISOString()
        }
        return value
      case ColumnTypeEnum.DateTime:
        if (value instanceof Date) {
          return value.toISOString()
        }
        return value
      case ColumnTypeEnum.Time:
        return value
      case ColumnTypeEnum.Uuid:
        return typeof value === 'string' ? value : String(value)
      default:
        return value
    }
  }
}
