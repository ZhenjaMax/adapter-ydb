import type { ArgType, SqlQuery } from '@prisma/driver-adapter-utils'
import type { TypedValue } from '@ydbjs/api/dist/gen/protos/ydb_value_pb.js'
import type { Type as YdbValueType } from '@ydbjs/value/dist/type.js'
import type { Value as YdbValue } from '@ydbjs/value/dist/value.js'
import { Optional } from '@ydbjs/value/dist/optional.js'
import { List, ListType } from '@ydbjs/value/dist/list.js'
import {
  Bool,
  BoolType,
  Bytes,
  BytesType,
  Double,
  DoubleType,
  Float,
  FloatType,
  Int32,
  Int32Type,
  Int64,
  Int64Type,
  JsonDocument,
  JsonDocumentType,
  Timestamp,
  TimestampType,
  Utf8,
  Utf8Type,
  Uuid,
  UuidType,
} from '@ydbjs/value/dist/primitive.js'

export type PreparedQuery = {
  text: string
  parameters: Record<string, YdbValue>
}

type ScalarMeta = {
  typeFactory(): YdbValueType
  create(value: unknown): YdbValue
}

class StaticListValue implements YdbValue {
  readonly type: ListType

  constructor(itemType: YdbValueType) {
    this.type = new ListType(itemType)
  }

  encode() {
    return { items: [] } as unknown as TypedValue['value']
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

    switch (scalar) {
      case 'boolean':
        return {
          typeFactory: () => new BoolType(),
          create: (value) => new Bool(Boolean(value)),
        }
      case 'int':
        return {
          typeFactory: () => new Int32Type(),
          create: (value) => new Int32(this.ensureNumber(value)),
        }
      case 'bigint':
        return {
          typeFactory: () => new Int64Type(),
          create: (value) => new Int64(this.ensureBigInt(value)),
        }
      case 'float':
        return {
          typeFactory: () => new FloatType(),
          create: (value) => new Float(this.ensureNumber(value)),
        }
      case 'decimal':
        return {
          typeFactory: () => new DoubleType(),
          create: (value) => new Double(this.ensureNumber(value)),
        }
      case 'uuid':
        return {
          typeFactory: () => new UuidType(),
          create: (value) => new Uuid(String(value)),
        }
      case 'json':
        return {
          typeFactory: () => new JsonDocumentType(),
          create: (value) => new JsonDocument(typeof value === 'string' ? value : JSON.stringify(value)),
        }
      case 'datetime':
        return {
          typeFactory: () => new TimestampType(),
          create: (value) => new Timestamp(this.ensureDate(value)),
        }
      case 'bytes':
        return {
          typeFactory: () => new BytesType(),
          create: (value) => new Bytes(this.ensureUint8Array(value)),
        }
      case 'enum':
      case 'string':
        return {
          typeFactory: () => new Utf8Type(),
          create: (value) => new Utf8(String(value)),
        }
      case 'unknown':
        return {
          typeFactory: () => new Utf8Type(),
          create: (value) => new Utf8(String(value)),
        }
      default:
        return {
          typeFactory: () => new Utf8Type(),
          create: (value) => new Utf8(String(value)),
        }
    }
  }

  private createScalarValue(value: unknown, meta: ScalarMeta): YdbValue {
    if (value === null || value === undefined) {
      return new Optional(null, meta.typeFactory())
    }

    const created = meta.create(value)
    if (created instanceof Optional) {
      return created
    }
    return created
  }

  private createListValue(value: unknown, meta: ScalarMeta): YdbValue {
    const source = Array.isArray(value) ? value : []

    if (source.length === 0) {
      return new StaticListValue(meta.typeFactory())
    }

    const items = source.map((item) => this.createScalarValue(item, meta))
    return new List(...items)
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
}
