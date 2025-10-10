import type { SqlQuery, ArgType, ColumnType } from '@prisma/driver-adapter-utils'
import { Driver } from '@ydbjs/core'
import { QueryServiceDefinition, ExecMode, Syntax, StatsMode } from '@ydbjs/api/dist/query.js'
import { StatusIds_StatusCode } from '@ydbjs/api/dist/operation.js'
import { YDBError } from '@ydbjs/error'
import { fromYdb, toJs } from '@ydbjs/value/dist/index.js'
import type { Type as YdbValueType } from '@ydbjs/value/dist/type.js'
import type { Value as YdbValue } from '@ydbjs/value/dist/value.js'
import type { Value as YdbProtoValue, TypedValue } from '@ydbjs/api/dist/gen/protos/ydb_value_pb.js'
import type { TransactionControl } from '@ydbjs/api/dist/gen/protos/ydb_query_pb.js'
import { Optional } from '@ydbjs/value/dist/optional.js'
import { List, ListType } from '@ydbjs/value/dist/list.js'
import {
  Bool,
  BoolType,
  Int32,
  Int32Type,
  Int64,
  Int64Type,
  Float,
  FloatType,
  Double,
  DoubleType,
  Utf8,
  Utf8Type,
  Bytes,
  BytesType,
  JsonDocument,
  JsonDocumentType,
  Timestamp,
  TimestampType,
  Uuid,
  UuidType,
} from '@ydbjs/value/dist/primitive.js'

import { YqlTypeMapper } from './yql-conversion'
import type {
  YdbColumn,
  YdbConnectionConfig,
  YdbQueryResult,
  YdbTransactionIsolation,
  YdbTransactionMeta,
} from './types'

type SessionContext = {
  sessionId: string
  nodeId: bigint
}

type TransactionContext = YdbTransactionMeta & {
  attached: boolean
}

type PreparedQuery = {
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
    return { items: [] } as unknown as YdbProtoValue
  }
}

export class YdbClientWrapper {
  private driver: Driver | null = null
  private connected = false
  private readonly transactions = new Map<string, TransactionContext>()

  constructor(private readonly config: YdbConnectionConfig) {}

  async connect(): Promise<void> {
    if (this.connected) return

    const connectionString = this.buildConnectionString()
    this.driver = new Driver(connectionString)
    await this.driver.ready()
    this.connected = true
  }

  async executeQuery(query: SqlQuery, txId?: string): Promise<YdbQueryResult> {
    const driver = this.ensureDriver()

    const prepared = this.prepareQuery(query)
    const encodedParams = this.encodeParameters(prepared.parameters)

    const session = txId ? this.requireTransaction(txId) : await this.createSession()
    const client = driver.createClient(QueryServiceDefinition, session.nodeId)

    const request: Record<string, unknown> = {
      sessionId: session.sessionId,
      execMode: ExecMode.EXECUTE,
      query: {
        case: 'queryContent',
        value: {
          syntax: Syntax.YQL_V1,
          text: prepared.text,
        },
      },
      parameters: encodedParams,
      statsMode: StatsMode.NONE,
    }

    if (txId) {
      const txControl = {
        txSelector: {
          case: 'txId',
          value: txId,
        },
      } as unknown as TransactionControl
      request.txControl = txControl
    }

    const stream = client.executeQuery(request)

    const columns: YdbColumn[] = []
    const columnTypes: ColumnType[] = []
    const rows: unknown[][] = []

    try {
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
    } finally {
      if (!txId) {
        await this.deleteSession(session)
      }
    }

    return {
      columns,
      columnTypes,
      rows,
      rowsAffected: 0,
    }
  }

  async executeScript(script: string): Promise<void> {
    const driver = this.ensureDriver()
    const session = await this.createSession()
    const client = driver.createClient(QueryServiceDefinition, session.nodeId)

    try {
      const stream = client.executeQuery({
        sessionId: session.sessionId,
        execMode: ExecMode.EXECUTE,
        query: {
          case: 'queryContent',
          value: {
            syntax: Syntax.YQL_V1,
            text: script,
          },
        },
        statsMode: StatsMode.NONE,
      })

      for await (const part of stream) {
        if (part.status !== StatusIds_StatusCode.SUCCESS) {
          throw new YDBError(part.status, part.issues)
        }
      }
    } finally {
      await this.deleteSession(session)
    }
  }

  async beginTransaction(isolation: YdbTransactionIsolation): Promise<YdbTransactionMeta> {
    const driver = this.ensureDriver()
    const session = await this.createSession()
    const client = driver.createClient(QueryServiceDefinition, session.nodeId)

    const beginResult = await client.beginTransaction({
      sessionId: session.sessionId,
      txSettings: { txMode: { case: isolation, value: {} } },
    })

    if (beginResult.status !== StatusIds_StatusCode.SUCCESS || !beginResult.txMeta?.id) {
      await this.deleteSession(session)
      throw new YDBError(beginResult.status, beginResult.issues)
    }

    const txMeta: TransactionContext = {
      txId: beginResult.txMeta.id,
      sessionId: session.sessionId,
      nodeId: session.nodeId,
      isolation,
      attached: true,
    }

    this.transactions.set(txMeta.txId, txMeta)
    return txMeta
  }

  async commitTransaction(txId: string): Promise<void> {
    const tx = this.requireTransaction(txId)
    const driver = this.ensureDriver()
    const client = driver.createClient(QueryServiceDefinition, tx.nodeId)

    const result = await client.commitTransaction({
      sessionId: tx.sessionId,
      txId: tx.txId,
    })

    if (result.status !== StatusIds_StatusCode.SUCCESS) {
      throw new YDBError(result.status, result.issues)
    }

    await this.deleteSession({ sessionId: tx.sessionId, nodeId: tx.nodeId })
    this.transactions.delete(txId)
  }

  async rollbackTransaction(txId: string): Promise<void> {
    const tx = this.requireTransaction(txId)
    const driver = this.ensureDriver()
    const client = driver.createClient(QueryServiceDefinition, tx.nodeId)

    await client.rollbackTransaction({
      sessionId: tx.sessionId,
      txId: tx.txId,
    })

    await this.deleteSession({ sessionId: tx.sessionId, nodeId: tx.nodeId })
    this.transactions.delete(txId)
  }

  getDatabasePath(): string {
    const driver = this.driver
    if (driver) {
      return driver.database
    }
    return this.config.database
  }

  async close(): Promise<void> {
    this.transactions.clear()
    if (this.driver) {
      this.driver.close()
    }
    this.driver = null
    this.connected = false
  }

  private buildConnectionString(): string {
    const { endpoint, database } = this.config
    const normalizedDatabase = database.startsWith('/') ? database : `/${database}`
    return `${endpoint}${normalizedDatabase}`
  }

  private ensureDriver(): Driver {
    const driver = this.driver
    if (!driver || !this.connected) {
      throw new Error('YDB client is not connected. Call connect() first.')
    }
    return driver
  }

  private requireTransaction(txId: string): TransactionContext {
    const tx = this.transactions.get(txId)
    if (!tx) {
      throw new Error(`Transaction ${txId} was not found or already finished.`)
    }
    return tx
  }

  private async createSession(): Promise<SessionContext> {
    const driver = this.ensureDriver()
    const client = driver.createClient(QueryServiceDefinition)

    const sessionResponse = await client.createSession({})
    if (sessionResponse.status !== StatusIds_StatusCode.SUCCESS) {
      throw new YDBError(sessionResponse.status, sessionResponse.issues)
    }

    const sessionId = sessionResponse.sessionId
    const nodeId = sessionResponse.nodeId

    const attachClient = driver.createClient(QueryServiceDefinition, nodeId)
    const iterator = attachClient.attachSession({ sessionId }, {})[Symbol.asyncIterator]()
    const attachResult = await iterator.next()
    if (attachResult.value.status !== StatusIds_StatusCode.SUCCESS) {
      throw new YDBError(attachResult.value.status, attachResult.value.issues)
    }

    return { sessionId, nodeId }
  }

  private async deleteSession(session: SessionContext): Promise<void> {
    try {
      const driver = this.ensureDriver()
      const client = driver.createClient(QueryServiceDefinition, session.nodeId)
      await client.deleteSession({ sessionId: session.sessionId })
    } catch (error) {
      // Ignore errors related to already closed sessions
    }
  }

  private prepareQuery(query: SqlQuery): PreparedQuery {
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

  private encodeParameters(parameters: Record<string, YdbValue>) {
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
