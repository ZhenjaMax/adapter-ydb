import type { Transaction as PrismaTransaction, TransactionOptions } from '@prisma/driver-adapter-utils'
import { YdbClientWrapper } from './client-wrapper'
import { YdbQueryable } from './queryable'

export class YdbTransaction extends YdbQueryable implements PrismaTransaction {
  readonly options: TransactionOptions

  constructor(protected txId: string, client: YdbClientWrapper, options?: Partial<TransactionOptions>) {
    super(client, txId)
    this.options = { usePhantomQuery: true, ...options }
  }

  async commit(): Promise<void> {
    await this.client.commitTransaction(this.txId)
  }

  async rollback(): Promise<void> {
    await this.client.rollbackTransaction(this.txId)
  }
}
