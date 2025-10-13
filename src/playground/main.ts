import { PrismaClient } from '@prisma/client'
import type { Prisma as PrismaNamespace } from '@prisma/client'
import { join, sqltag } from '@prisma/client/runtime/library'
import { PrismaYdbAdapterFactory } from '../adapter-factory.js'

const sql = sqltag

type TransactionClient = PrismaNamespace.TransactionClient

type UserRow = { id: bigint | number; name: string; age: number; created_at: Date }

type CountRow = { total: bigint | number }

const endpoint = process.env.YDB_ENDPOINT ?? 'grpc://localhost:2136'
const database = process.env.YDB_DATABASE ?? '/local'

async function ensureSchema(prisma: PrismaClient) {
  await prisma.$executeRawUnsafe(`
    CREATE TABLE IF NOT EXISTS users (
      id Uint64,
      name Utf8,
      age Int32,
      created_at Datetime,
      PRIMARY KEY (id)
    );
  `)
}

async function seed(prisma: PrismaClient) {
  const upsertQuery = sql`
    UPSERT INTO users (id, name, age, created_at)
    VALUES
      (${1n}, ${'Alice'}, ${30}, CurrentUtcDatetime()),
      (${2n}, ${'Bob'}, ${25}, CurrentUtcDatetime());
  `

  await prisma.$executeRaw(upsertQuery)
}

async function readUsers(prisma: PrismaClient) {
  const rows = (await prisma.$queryRaw(sql`
    SELECT id, name, age, created_at
    FROM users
    ORDER BY id;
  `)) as UserRow[]

  return rows.map((row: UserRow) => ({
    ...row,
    id: typeof row.id === 'bigint' ? Number(row.id) : row.id,
    created_at: row.created_at instanceof Date ? row.created_at.toISOString() : row.created_at,
  }))
}

async function transactionalSample(prisma: PrismaClient) {
  await prisma.$transaction(async (tx: TransactionClient) => {
    const insert = sql`
      UPSERT INTO users (id, name, age, created_at)
      VALUES (${3n}, ${'Charlie'}, ${28}, CurrentUtcDatetime());
    `

    await tx.$executeRaw(insert)

    const selectCount = sql`
      SELECT COUNT(*) AS total
      FROM users;
    `

    const [result] = (await tx.$queryRaw(selectCount)) as CountRow[]
    const count = result ? (typeof result.total === 'bigint' ? Number(result.total) : result.total) : 0
    console.log('📊 Users count inside transaction:', count)
  })
}

async function cleanup(prisma: PrismaClient) {
  const idsList = join([1n, 2n, 3n])
  await prisma.$executeRaw(sql`
    DELETE FROM users
    WHERE id IN (${idsList});
  `)
}

async function main() {
  const factory = new PrismaYdbAdapterFactory({ endpoint, database })

  const prisma = new PrismaClient({
    adapter: factory,
    log: ['query', 'info', 'warn', 'error'],
  })

  try {
    console.log('🚀 Connecting to YDB...')
    await ensureSchema(prisma)
    console.log('🛠 Table "users" ensured.')

    await cleanup(prisma)
    await seed(prisma)
    console.log('🌱 Seed data inserted.')

    const users = await readUsers(prisma)
    console.log('👥 Users:', users)

    await transactionalSample(prisma)
  } catch (error) {
    console.error('❌ Error during YDB interaction:', error)
  } finally {
    await prisma.$disconnect()
    console.log('🔌 Disconnected.')
  }
}

main().catch((error) => {
  console.error('❌ Unhandled error:', error)
  process.exit(1)
})
