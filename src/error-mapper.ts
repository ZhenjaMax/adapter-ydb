import { DriverAdapterError } from '@prisma/driver-adapter-utils'

export class YdbErrorMapper {
  static toPrismaError(err: any): DriverAdapterError {
    // TODO: нормализация статусов YDB (ABORTED, TIMEOUT, BAD_REQUEST)
    return new DriverAdapterError('YDB_ERROR', err.message || 'Unknown YDB error')
  }
}
