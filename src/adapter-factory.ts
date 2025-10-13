import type { Provider, SqlDriverAdapter, SqlDriverAdapterFactory } from '@prisma/driver-adapter-utils'
import { PostgresCompatibilityAdapter, PrismaYdbAdapter } from './adapter.js'
import { YdbClientWrapper } from './client-wrapper.js'
import type { SessionPoolOptions } from './session-pool.js'
import type { YdbConnectionConfig } from './types.js'

export class YdbAdapterFactory<TAdapter extends PrismaYdbAdapter = PrismaYdbAdapter> {
  readonly provider: string = 'ydb'
  readonly adapterName = '@prisma/adapter-ydb'

  constructor(protected config: YdbConnectionConfig, protected readonly poolOptions: SessionPoolOptions = {}) {}

  async create(): Promise<TAdapter> {
    const client = await this.createClient()
    return this.createAdapter(client)
  }

  protected async createClient(): Promise<YdbClientWrapper> {
    const client = new YdbClientWrapper(this.config, this.poolOptions)
    await client.connect()
    return client
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  protected createAdapter(client: YdbClientWrapper): TAdapter {
    return new PrismaYdbAdapter(client) as TAdapter
  }
}

export class PrismaYdbAdapterFactory
  extends YdbAdapterFactory<PostgresCompatibilityAdapter>
  implements SqlDriverAdapterFactory
{
  readonly provider: Provider = 'postgres'

  async connect(): Promise<SqlDriverAdapter> {
    return this.create()
  }

  async connectToShadowDb(): Promise<PostgresCompatibilityAdapter> {
    return this.create()
  }

  protected override createAdapter(client: YdbClientWrapper): PostgresCompatibilityAdapter {
    return new PostgresCompatibilityAdapter(client)
  }
}
