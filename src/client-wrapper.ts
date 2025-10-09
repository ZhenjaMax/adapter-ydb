import type { YdbConnectionConfig, YdbResultSet } from './types'
import { YqlTypeMapper } from './yql-conversion'

/**
 * YdbClientWrapper — тонкая прослойка между адаптером и транспортом YDB.
 *
 * В этом репозитории реализована минимальная in-memory имитация поведения YDB,
 * достаточная для локальной отладки и playground. При интеграции с реальным YDB
 * замените содержимое executeQuery/beginTransaction/... на вызовы SDK.
 */
export class YdbClientWrapper {
  private connected = false

  private tables = new Map<
    string,
    { columns: { name: string; type: string }[]; primaryKey?: string }
  >()
  private data = new Map<string, Map<string | number, Record<string, any>>>()
  private txSet = new Set<string>()

  constructor(private config: YdbConnectionConfig) {}

  async connect(): Promise<void> {
    this.connected = true
  }

  async executeQuery(
    sql: string,
    params?: Record<string, any>,
    txId?: string,
  ): Promise<YdbResultSet & { rowsAffected?: number }> {
    if (!this.connected) throw new Error('YDB client is not connected')

    const normParams: Record<string, any> | undefined = params
      ? Object.fromEntries(
          Object.entries(params).map(([k, v]) => [k, YqlTypeMapper.toYdbParameter(v)]),
        )
      : undefined

    const text = sql.trim()
    const upper = text.toUpperCase()

    if (upper.startsWith('CREATE TABLE')) return this.handleCreateTable(text)
    if (upper.startsWith('UPSERT INTO')) return this.handleUpsert(text)
    if (/^SELECT\s+COUNT\(\*\)\s+AS\s+\w+\s+FROM\s+\w+/i.test(text)) return this.handleSelectCount(text)
    if (/^SELECT\s+\*\s+FROM\s+\w+/i.test(text)) return this.handleSelectAll(text)

    throw new Error('Unsupported YQL in mock YdbClientWrapper: ' + sql)
  }

  async beginTransaction(): Promise<string> {
    const id = 'tx_' + Date.now().toString(36) + '_' + Math.random().toString(36).slice(2)
    this.txSet.add(id)
    return id
  }

  async commitTransaction(txId: string): Promise<void> {
    if (!this.txSet.has(txId)) throw new Error('Unknown transaction id: ' + txId)
    this.txSet.delete(txId)
  }

  async rollbackTransaction(txId: string): Promise<void> {
    if (!this.txSet.has(txId)) throw new Error('Unknown transaction id: ' + txId)
    this.txSet.delete(txId)
  }

  async close(): Promise<void> {
    this.connected = false
    this.tables.clear()
    this.data.clear()
    this.txSet.clear()
  }

  private handleCreateTable(sql: string): YdbResultSet & { rowsAffected?: number } {
    const table = this.expectMatchGroup(
      /CREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?\s+(\w+)/i.exec(sql),
      1,
      'Invalid CREATE TABLE statement',
    )

    const parenStart = sql.indexOf('(')
    const parenEnd = sql.lastIndexOf(')')
    if (parenStart === -1 || parenEnd === -1 || parenEnd <= parenStart) {
      throw new Error('Invalid column definition in CREATE TABLE')
    }
    const inside = sql.slice(parenStart + 1, parenEnd)

    let primaryKey: string | undefined
    const pkMatch = /PRIMARY\s+KEY\s*\(([^)]+)\)/i.exec(inside)
    if (pkMatch) {
      const pkGroup = this.expectMatchGroup(pkMatch, 1, 'Invalid PRIMARY KEY definition')
      // Проверяем, что pkGroup не undefined перед вызовом split()
      if (pkGroup !== undefined) {
        // @ts-ignore
        primaryKey = pkGroup!.split(',')[0].trim().replace(/"/g, '');
      }
    }

    const colsPart = inside.replace(/,?\s*PRIMARY\s+KEY\s*\([^)]+\)/i, '')
    const colDefs = colsPart.split(',').map((s) => s.trim()).filter(Boolean)

    const columns = colDefs.map((def) => {
      const match = /^(\w+)\s+(\w+)/.exec(def)
      const name = this.expectMatchGroup(match, 1, 'Invalid column definition: ' + def)
      const type = this.expectMatchGroup(match, 2, 'Invalid column definition: ' + def)
      return { name, type }
    })

    const meta: { columns: { name: string; type: string }[]; primaryKey?: string } = {
      columns,
    }
    if (primaryKey !== undefined) meta.primaryKey = primaryKey
    this.tables.set(table, meta)
    if (!this.data.has(table)) this.data.set(table, new Map())

    return { columns: [], rows: [], rowsAffected: 0 }
  }

  private handleUpsert(sql: string): YdbResultSet & { rowsAffected?: number } {
    const match = /^UPSERT\s+INTO\s+(\w+)\s*\(([^)]+)\)\s*VALUES\s*\(([^)]+)\)/i.exec(sql)
    const table = this.expectMatchGroup(match, 1, 'Invalid UPSERT statement')
    const columnsGroup = this.expectMatchGroup(match, 2, 'Invalid UPSERT statement')
    const valuesGroup = this.expectMatchGroup(match, 3, 'Invalid UPSERT statement')
    const cols = columnsGroup.split(',').map((s) => s.trim().replace(/"/g, ''))
    const rawValues = this.splitCsvRespectingQuotes(valuesGroup)
    const values = rawValues.map((v) => this.parseLiteral(v))

    const schema = this.expectMapValue(this.tables, table, `Table not found: ${table}`)

    const rowObj: Record<string, any> = {}
    cols.forEach((c, i) => (rowObj[c] = values[i]))

    const store = this.expectMapValue(
      this.data,
      table,
      `Table storage is not initialized: ${table}`,
    )
    const pk = schema.primaryKey ?? this.expectArrayValue(cols, 0, 'UPSERT requires at least one column')
    const pkVal = rowObj[pk]
    if (pkVal === undefined) throw new Error('Primary key value is required for UPSERT')
    store.set(pkVal, rowObj)

    return { columns: [], rows: [], rowsAffected: 1 }
  }

  private handleSelectAll(sql: string): YdbResultSet & { rowsAffected?: number } {
    const match = /^SELECT\s+\*\s+FROM\s+(\w+)/i.exec(sql)
    const table = this.expectMatchGroup(match, 1, 'Invalid SELECT statement')

    const schema = this.expectMapValue(this.tables, table, `Table not found: ${table}`)
    const store = this.expectMapValue(this.data, table, `Table storage is not initialized: ${table}`)

    const columns = schema.columns
    const rows = Array.from(store.values()).map((obj) => columns.map((c) => obj[c.name]))

    return { columns, rows }
  }

  private handleSelectCount(sql: string): YdbResultSet & { rowsAffected?: number } {
    const match = /^SELECT\s+COUNT\(\*\)\s+AS\s+(\w+)\s+FROM\s+(\w+)/i.exec(sql)
    const alias = this.expectMatchGroup(match, 1, 'Invalid SELECT COUNT statement')
    const table = this.expectMatchGroup(match, 2, 'Invalid SELECT COUNT statement')

    const schema = this.expectMapValue(this.tables, table, `Table not found: ${table}`)
    const store = this.expectMapValue(this.data, table, `Table storage is not initialized: ${table}`)

    const columns = [{ name: alias, type: 'Int64' }]
    const rows = [[store.size]]
    return { columns, rows }
  }

  private splitCsvRespectingQuotes(s: string): string[] {
    const out: string[] = []
    let cur = ''
    let inQuotes = false
    for (let i = 0; i < s.length; i++) {
      const ch = s[i]
      if (ch === '"') {
        inQuotes = !inQuotes
        continue
      }
      if (ch === ',' && !inQuotes) {
        out.push(cur.trim())
        cur = ''
      } else {
        cur += ch
      }
    }
    if (cur.length) out.push(cur.trim())
    return out
  }

  private parseLiteral(v: string): any {
    const t = v.trim()
    if (/^".*"$/.test(t)) return t.slice(1, -1)
    if (/^-?\d+$/.test(t)) return Number(t)
    if (/^-?\d+\.\d+$/.test(t)) return Number(t)
    if (/^NULL$/i.test(t)) return null
    return t
  }

  private expectMatchGroup(
    match: RegExpExecArray | null,
    index: number,
    errorMessage: string,
  ): string {
    const group = match?.[index]
    if (!group) throw new Error(errorMessage)
    return group
  }

  private expectArrayValue<T>(values: T[], index: number, errorMessage: string): T {
    const value = values[index]
    if (value === undefined) throw new Error(errorMessage)
    return value
  }

  private expectMapValue<T>(map: Map<string, T>, key: string, errorMessage: string): T {
    const value = map.get(key)
    if (value === undefined) throw new Error(errorMessage)
    return value
  }
}
