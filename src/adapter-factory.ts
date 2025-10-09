import type { SqlDriverAdapter, SqlDriverAdapterFactory } from '@prisma/driver-adapter-utils'
import { PrismaYdbAdapter } from './adapter'
import { YdbClientWrapper } from './client-wrapper'
import type { YdbConnectionConfig } from './types'

/**
 * PrismaYdbAdapterFactory
 *
 * Фабрика создаёт экземпляры адаптера PrismaYdbAdapter,
 * инкапсулируя настройку соединения с YDB.
 *
 * Prisma ожидает, что фабрика реализует интерфейс SqlDriverAdapterFactory.
 * Это позволяет использовать адаптер в командах prisma migrate, studio и runtime.
 */
export class PrismaYdbAdapterFactory implements SqlDriverAdapterFactory {
  readonly provider = 'ydb'
  readonly adapterName = '@prisma/adapter-ydb'

  private config: YdbConnectionConfig

  constructor(config: YdbConnectionConfig) {
    this.config = config
  }

  /**
   * Обязательный метод фабрики по контракту Prisma —
   * устанавливает соединение и возвращает адаптер.
   */
  async connect(): Promise<SqlDriverAdapter> {
    const client = new YdbClientWrapper(this.config)
    await client.connect()
    return new PrismaYdbAdapter(client)
  }

  /**
   * Удобный алиас для локального кода/playground,
   * чтобы не ломать существующие вызовы factory.create().
   */
  async create(): Promise<PrismaYdbAdapter> {
    return (await this.connect()) as PrismaYdbAdapter
  }

  /**
   * Создание "shadow database" (теневой базы для миграций).
   * Можно оставить так же, как основное подключение, либо
   * при необходимости настраивать отдельный каталог/in-memory.
   */
  async connectToShadowDb(): Promise<PrismaYdbAdapter> {
    // TODO: при необходимости — отдельная in-memory YDB для миграций
    return this.create()
  }
}
