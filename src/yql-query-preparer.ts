import type { ArgType, SqlQuery } from '@prisma/driver-adapter-utils'
import type { TypedValue, Value as YdbProtoValue } from '@ydbjs/api/value'
import type { Type as YdbValueType, Value as YdbValue } from '@ydbjs/value'
import { Optional, OptionalType } from '@ydbjs/value/optional'
import { List, ListType } from '@ydbjs/value/list'
import {
  Bool,
  BoolType,
  Bytes,
  BytesType,
  Double,
  DoubleType,
  Float,
  FloatType,
  Int8,
  Int8Type,
  Int16,
  Int16Type,
  Int32,
  Int32Type,
  Int64,
  Int64Type,
  Uint8,
  Uint8Type,
  Uint16,
  Uint16Type,
  Uint32,
  Uint32Type,
  Uint64,
  Uint64Type,
  JsonDocument,
  JsonDocumentType,
  Timestamp,
  TimestampType,
  Utf8,
  Utf8Type,
  Uuid,
  UuidType,
} from '@ydbjs/value/primitive'

export type PreparedQuery = {
  text: string
  parameters: Record<string, YdbValue>
}

type ScalarMeta = {
  typeFactory(): YdbValueType
  create(value: unknown): YdbValue
  optional: boolean
}

class StaticListValue implements YdbValue {
  readonly type: ListType

  constructor(itemType: YdbValueType) {
    this.type = new ListType(itemType)
  }

  encode(): YdbProtoValue {
    return { items: [] } as unknown as YdbProtoValue
  }
}

export class YqlQueryPreparer {
  prepare(query: SqlQuery): PreparedQuery {
    if (!query.args.length) {
      return { text: query.sql, parameters: {} }
    }

    const segmentsInfo = this.extractSegments(query)
    const metas = query.argTypes?.map((arg) => this.getScalarMeta(arg)) ?? []

    const values: YdbValue[] = segmentsInfo.order.map((argIndex) => {
      const argValue = query.args[argIndex]
      const argType = query.argTypes?.[argIndex]
      const meta = metas[argIndex] ?? this.getScalarMeta(argType)

      if (argType?.arity === 'list') {
        return this.createListValue(argValue, meta)
      }

      return this.createScalarValue(argValue, meta)
    })

    const textParts = segmentsInfo.segments
    let text = ''
    const params: Record<string, YdbValue> = {}

    textParts.forEach((part, index) => {
      text += part
      if (index < values.length) {
        const paramName = `$p${index}`
        const paramValue = values[index]!
        params[paramName] = paramValue
        text += paramName
      }
    })

    return { text, parameters: params }
  }

  encodeParameters(parameters: Record<string, YdbValue>): Record<string, TypedValue> {
    const encoded: Record<string, TypedValue> = {}

    for (const [name, value] of Object.entries(parameters)) {
      encoded[name] = {
        type: value.type.encode(),
        value: value.encode(),
      } as unknown as TypedValue
    }

    return encoded
  }

  private extractSegments(query: SqlQuery): { segments: string[]; order: number[] } {
    const segments: string[] = []
    const order: number[] = []
    const dollarRegex = /\$(\d+)/g
    const questionRegex = /\?/g

    let lastIndex = 0
    let match: RegExpExecArray | null = null

    if (dollarRegex.test(query.sql)) {
      dollarRegex.lastIndex = 0
      while ((match = dollarRegex.exec(query.sql))) {
        const index = Number(match[1]) - 1
        if (index < 0 || index >= query.args.length) {
          throw new Error(`Placeholder $${match[1]} does not match any argument.`)
        }

        segments.push(query.sql.slice(lastIndex, match.index))
        order.push(index)
        lastIndex = match.index + match[0].length
      }
    } else if (questionRegex.test(query.sql)) {
      questionRegex.lastIndex = 0
      let sequentialIndex = 0

      while ((match = questionRegex.exec(query.sql))) {
        if (sequentialIndex >= query.args.length) {
          throw new Error('Too many placeholders compared to provided arguments.')
        }

        segments.push(query.sql.slice(lastIndex, match.index))
        order.push(sequentialIndex)
        lastIndex = match.index + match[0].length
        sequentialIndex += 1
      }
    }

    segments.push(query.sql.slice(lastIndex))

    if (order.length === 0) {
      return { segments: [query.sql], order: [] }
    }

    return { segments, order }
  }

  private getScalarMeta(argType?: ArgType): ScalarMeta {
    const scalar = argType?.scalarType ?? 'string'
    const { normalized: dbType, optional } = this.normalizeDbType(argType?.dbType)

    switch (scalar) {
      case 'boolean':
        return {
          typeFactory: () => new BoolType(),
          create: (value) => new Bool(Boolean(value)),
          optional,
        }
      case 'int':
        return this.getIntegerMeta(dbType, optional)
      case 'bigint':
        return this.getBigIntMeta(dbType, optional)
      case 'float':
        return {
          typeFactory: () => new FloatType(),
          create: (value) => new Float(this.ensureNumber(value)),
          optional,
        }
      case 'decimal':
        return {
          typeFactory: () => new DoubleType(),
          create: (value) => new Double(this.ensureNumber(value)),
          optional,
        }
      case 'uuid':
        return {
          typeFactory: () => new UuidType(),
          create: (value) => new Uuid(String(value)),
          optional,
        }
      case 'json':
        return {
          typeFactory: () => new JsonDocumentType(),
          create: (value) => new JsonDocument(typeof value === 'string' ? value : JSON.stringify(value)),
          optional,
        }
      case 'datetime':
        return {
          typeFactory: () => new TimestampType(),
          create: (value) => new Timestamp(this.ensureDate(value)),
          optional,
        }
      case 'bytes':
        return {
          typeFactory: () => new BytesType(),
          create: (value) => new Bytes(this.ensureUint8Array(value)),
          optional,
        }
      case 'enum':
      case 'string':
        return {
          typeFactory: () => new Utf8Type(),
          create: (value) => new Utf8(String(value)),
          optional,
        }
      case 'unknown':
        return {
          typeFactory: () => new Utf8Type(),
          create: (value) => new Utf8(String(value)),
          optional,
        }
      default:
        return {
          typeFactory: () => new Utf8Type(),
          create: (value) => new Utf8(String(value)),
          optional,
        }
    }
  }

  private createScalarValue(value: unknown, meta: ScalarMeta): YdbValue {
    if (this.isPreboundYdbValue(value)) {
      return value as YdbValue
    }

    if (value === null || value === undefined) {
      return new Optional(null, meta.typeFactory())
    }

    const created = meta.create(value)
    if (created instanceof Optional) {
      return created
    }

    if (meta.optional) {
      return new Optional(created, meta.typeFactory())
    }

    return created
  }

  private createListValue(value: unknown, meta: ScalarMeta): YdbValue {
    const source = Array.isArray(value) ? value : []

    if (source.length === 0) {
      const itemType = meta.optional ? new OptionalType(meta.typeFactory()) : meta.typeFactory()
      return new StaticListValue(itemType)
    }

    const items = source.map((item) => this.createScalarValue(item, meta))
    return new List(...items)
  }

  private normalizeDbType(dbType?: string | null): { normalized?: string; optional: boolean } {
    if (!dbType) {
      return { optional: false }
    }

    let normalized = dbType.toLowerCase().replace(/\s+/g, '')
    let optional = false

    const optionalWrapper = /^optional<(.*)>$/
    let match = optionalWrapper.exec(normalized)
    while (match) {
      optional = true
      normalized = match[1]!
      match = optionalWrapper.exec(normalized)
    }

    if (normalized.includes('optional<')) {
      optional = true
      normalized = normalized.replace(/optional<([^>]+)>/g, '$1')
    }

    if (normalized.endsWith('?')) {
      optional = true
      normalized = normalized.slice(0, -1)
    }

    return { normalized, optional }
  }

  private ensureDate(value: unknown): Date {
    if (value instanceof Date) {
      return value
    }
    if (typeof value === 'number') {
      return new Date(value)
    }
    if (typeof value === 'string') {
      return new Date(value)
    }
    throw new Error('Unsupported date value for YDB parameter.')
  }

  private ensureUint8Array(value: unknown): Uint8Array {
    if (value instanceof Uint8Array) {
      return value
    }
    if (typeof Buffer !== 'undefined' && Buffer.isBuffer(value)) {
      return new Uint8Array(value)
    }
    if (Array.isArray(value)) {
      return new Uint8Array(value)
    }
    if (value instanceof ArrayBuffer) {
      return new Uint8Array(value)
    }
    if (ArrayBuffer.isView(value)) {
      const view = value as ArrayBufferView
      return new Uint8Array(view.buffer, view.byteOffset, view.byteLength)
    }
    throw new Error('Unsupported binary parameter type for YDB.')
  }

  private ensureNumber(value: unknown): number {
    if (typeof value === 'number') {
      return value
    }
    if (typeof value === 'string') {
      const parsed = Number(value)
      if (!Number.isNaN(parsed)) {
        return parsed
      }
    }
    if (typeof value === 'bigint') {
      return Number(value)
    }
    throw new Error('Cannot coerce value to number for YDB parameter binding.')
  }

  private ensureBigInt(value: unknown): bigint {
    if (typeof value === 'bigint') {
      return value
    }
    if (typeof value === 'number') {
      return BigInt(Math.trunc(value))
    }
    if (typeof value === 'string' && value.trim().length > 0) {
      return BigInt(value)
    }
    throw new Error('Cannot coerce value to bigint for YDB parameter binding.')
  }

  private isPreboundYdbValue(value: unknown): value is YdbValue {
    if (!value || typeof value !== 'object') {
      return false
    }

    const candidate = value as Partial<YdbValue>
    return typeof candidate.encode === 'function' && typeof candidate.type?.encode === 'function'
  }

  private getIntegerMeta(dbType?: string | null, optional = false): ScalarMeta {
    if (dbType) {
      if (dbType.includes('uint8')) {
        return {
          typeFactory: () => new Uint8Type(),
          create: (value) => new Uint8(this.ensureNumber(value)),
          optional,
        }
      }
      if (dbType.includes('uint16')) {
        return {
          typeFactory: () => new Uint16Type(),
          create: (value) => new Uint16(this.ensureNumber(value)),
          optional,
        }
      }
      if (dbType.includes('uint32')) {
        return {
          typeFactory: () => new Uint32Type(),
          create: (value) => new Uint32(this.ensureNumber(value)),
          optional,
        }
      }
      if (dbType.includes('uint64')) {
        return {
          typeFactory: () => new Uint64Type(),
          create: (value) => new Uint64(this.ensureBigInt(value)),
          optional,
        }
      }
      if (dbType.includes('int8')) {
        return {
          typeFactory: () => new Int8Type(),
          create: (value) => new Int8(this.ensureNumber(value)),
          optional,
        }
      }
      if (dbType.includes('int16')) {
        return {
          typeFactory: () => new Int16Type(),
          create: (value) => new Int16(this.ensureNumber(value)),
          optional,
        }
      }
      if (dbType.includes('int32')) {
        return {
          typeFactory: () => new Int32Type(),
          create: (value) => new Int32(this.ensureNumber(value)),
          optional,
        }
      }
      if (dbType.includes('int64')) {
        return {
          typeFactory: () => new Int64Type(),
          create: (value) => new Int64(this.ensureBigInt(value)),
          optional,
        }
      }
    }

    return {
      typeFactory: () => new Int32Type(),
      create: (value) => new Int32(this.ensureNumber(value)),
      optional,
    }
  }

  private getBigIntMeta(dbType?: string | null, optional = false): ScalarMeta {
    if (dbType) {
      if (dbType.includes('uint64')) {
        return {
          typeFactory: () => new Uint64Type(),
          create: (value) => new Uint64(this.ensureBigInt(value)),
          optional,
        }
      }
      if (dbType.includes('int64')) {
        return {
          typeFactory: () => new Int64Type(),
          create: (value) => new Int64(this.ensureBigInt(value)),
          optional,
        }
      }
    }

    return {
      typeFactory: () => new Int64Type(),
      create: (value) => new Int64(this.ensureBigInt(value)),
      optional,
    }
  }
}
