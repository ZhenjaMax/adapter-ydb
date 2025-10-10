import { DriverAdapterError } from '@prisma/driver-adapter-utils'

type DriverAdapterErrorPayload = ConstructorParameters<typeof DriverAdapterError>[0]

const TABLE_NOT_FOUND_REGEX = /(?:table|relation)\s+[`'"]?([^\s`'"]+)[`'"]?/i
const COLUMN_NOT_FOUND_REGEX = /column\s+[`'"]?([^\s`'"]+)[`'"]?/i

export class YdbErrorMapper {
  static toPrismaError(err: unknown): DriverAdapterError {
    const payload = this.mapToPayload(err)
    return new DriverAdapterError(payload)
  }

  private static mapToPayload(err: unknown): DriverAdapterErrorPayload {
    const status = this.extractStatus(err)
    const message = this.extractMessage(err)
    const normalizedMessage = message.toLowerCase()

    if (status === 'ABORTED' || normalizedMessage.includes('transaction aborted')) {
      return this.withMeta({ kind: 'TransactionWriteConflict' }, status, message)
    }

    if (
      status === 'TIMEOUT' ||
      status === 'DEADLINE_EXCEEDED' ||
      normalizedMessage.includes('timeout') ||
      normalizedMessage.includes('timed out')
    ) {
      return this.withMeta({ kind: 'SocketTimeout' }, status, message)
    }

    if (
      status === 'UNAUTHORIZED' ||
      normalizedMessage.includes('permission denied') ||
      normalizedMessage.includes('access denied')
    ) {
      return this.withMeta({ kind: 'DatabaseAccessDenied' }, status, message)
    }

    if (status === 'UNAVAILABLE' || status === 'OVERLOADED') {
      return this.withMeta({ kind: 'DatabaseNotReachable' }, status, message)
    }

    if (
      normalizedMessage.includes('connection refused') ||
      normalizedMessage.includes('connection reset') ||
      normalizedMessage.includes('connection closed')
    ) {
      return this.withMeta({ kind: 'DatabaseNotReachable' }, status, message)
    }

    if (status === 'NOT_FOUND' || this.includesAll(normalizedMessage, ['table', 'not found'])) {
      const table = this.tryExtract(TABLE_NOT_FOUND_REGEX, message)
      return this.withMeta(
        {
          kind: 'TableDoesNotExist',
          ...(table !== undefined ? { table } : {}),
        },
        status,
        message,
      )
    }

    if (this.includesAll(normalizedMessage, ['column', 'not found'])) {
      const column = this.tryExtract(COLUMN_NOT_FOUND_REGEX, message)
      return this.withMeta(
        {
          kind: 'ColumnNotFound',
          ...(column !== undefined ? { column } : {}),
        },
        status,
        message,
      )
    }

    if (this.includesAll(normalizedMessage, ['already', 'exists'])) {
      return this.withMeta({ kind: 'DatabaseAlreadyExists' }, status, message)
    }

    if (this.includesAll(normalizedMessage, ['too many', 'connection'])) {
      return this.withMeta({ kind: 'TooManyConnections', cause: message }, status, message)
    }

    if (this.includesAll(normalizedMessage, ['out of range'])) {
      return this.withMeta({ kind: 'ValueOutOfRange', cause: message }, status, message)
    }

    if (this.includesAll(normalizedMessage, ['transaction', 'closed'])) {
      return this.withMeta({ kind: 'TransactionAlreadyClosed', cause: message }, status, message)
    }

    if (this.includesAll(normalizedMessage, ['tls', 'handshake'])) {
      return this.withMeta({ kind: 'TlsConnectionError', reason: message }, status, message)
    }

    return this.withMeta({ kind: 'DatabaseNotReachable' }, status, message)
  }

  private static extractStatus(err: unknown): string | undefined {
    if (typeof err === 'object' && err !== null) {
      const candidate =
        (err as any).status ??
        (err as any).code ??
        (err as any).errorCode ??
        ((err as any).issues && (err as any).issues[0]?.code)

      if (typeof candidate === 'string' && candidate.trim().length > 0) {
        return candidate.trim().toUpperCase()
      }
    }

    return undefined
  }

  private static extractMessage(err: unknown): string {
    if (typeof err === 'string') return err
    if (typeof err === 'object' && err !== null && 'message' in err && typeof (err as any).message === 'string') {
      return (err as any).message
    }
    if (err instanceof Error && typeof err.message === 'string') {
      return err.message
    }
    try {
      return JSON.stringify(err)
    } catch {
      return 'Unknown YDB error'
    }
  }

  private static withMeta<T extends DriverAdapterErrorPayload>(
    payload: T,
    status: string | undefined,
    message: string,
  ): DriverAdapterErrorPayload {
    return {
      ...payload,
      ...(status ? { originalCode: status } : {}),
      originalMessage: message,
    }
  }

  private static includesAll(message: string, parts: string[]): boolean {
    return parts.every((part) => message.includes(part))
  }

  private static tryExtract(regex: RegExp, message: string): string | undefined {
    const match = regex.exec(message)
    return match?.[1]
  }
}
