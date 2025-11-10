import { PrismaClient } from '@prisma/client'
import { sqltag } from '@prisma/client/runtime/library'
import { PrismaYdbAdapterFactory } from '../adapter-factory.js'

const sql = sqltag
const uint64 = (value: bigint | number) => sql`CAST(${value} AS Uint64)`
const int32 = (value: number) => sql`CAST(${value} AS Int32)`

type UserRow = { id: bigint | number; name: string; age: number; created_at: Date | string }
type NormalizedUser = { id: number; name: string; age: number; created_at: string }
type CreateUserInput = { id: bigint | number; name: string; age: number }
type UpdateUserInput = { name: string; age: number }

const endpoint = process.env.YDB_ENDPOINT ?? 'grpc://localhost:2136'
const database = process.env.YDB_DATABASE ?? '/local'

function toIsoString(value: Date | string): string {
  if (value instanceof Date) return value.toISOString()
  const parsed = new Date(value)
  return Number.isNaN(parsed.getTime()) ? String(value) : parsed.toISOString()
}

function normalizeUser(row: UserRow): NormalizedUser {
  return {
    id: typeof row.id === 'bigint' ? Number(row.id) : row.id,
    name: row.name,
    age: row.age,
    created_at: toIsoString(row.created_at),
  }
}

async function ensureSchema(prisma: PrismaClient) {
  await prisma.$executeRawUnsafe(`CREATE TABLE IF NOT EXISTS users (
    id Uint64,
    name Utf8,
    age Int32,
    created_at Datetime,
    PRIMARY KEY (id)
  );`)
}

async function clearUsers(prisma: PrismaClient) {
  await prisma.$executeRaw(sql`
    DELETE FROM users;
  `)
}

async function createUser(prisma: PrismaClient, user: CreateUserInput): Promise<NormalizedUser> {
  await prisma.$executeRaw(sql`
    UPSERT INTO users (id, name, age, created_at)
    VALUES (${uint64(user.id)}, ${user.name}, ${int32(user.age)}, CurrentUtcDatetime());
  `)

  const created = await readUser(prisma, user.id)
  if (!created) {
    throw new Error(`User with id ${user.id} was not created`)
  }

  return created
}

async function readUser(prisma: PrismaClient, id: bigint | number): Promise<NormalizedUser | null> {
  const rows = (await prisma.$queryRaw(sql`
    SELECT id, name, age, created_at
    FROM users
    WHERE id = ${uint64(id)};
  `)) as UserRow[]

  const [row] = rows
  return row ? normalizeUser(row) : null
}

async function listUsers(prisma: PrismaClient): Promise<NormalizedUser[]> {
  const rows = (await prisma.$queryRaw(sql`
    SELECT id, name, age, created_at
    FROM users
    ORDER BY id;
  `)) as UserRow[]

  return rows.map(normalizeUser)
}

async function updateUser(
  prisma: PrismaClient,
  id: bigint | number,
  data: UpdateUserInput,
): Promise<NormalizedUser | null> {
  await prisma.$executeRaw(sql`
    UPDATE users
    SET name = ${data.name}, age = ${int32(data.age)}
    WHERE id = ${uint64(id)};
  `)

  return readUser(prisma, id)
}

async function deleteUser(prisma: PrismaClient, id: bigint | number): Promise<boolean> {
  await prisma.$executeRaw(sql`
    DELETE FROM users
    WHERE id = ${uint64(id)};
  `)

  const existing = await readUser(prisma, id)
  return existing === null
}

async function main() {
  const factory = new PrismaYdbAdapterFactory({ endpoint, database })

  const prisma = new PrismaClient({
    adapter: factory,
    log: [
      'error'
    ],
  })

  try {
    console.log('Connecting to YDB...')
    await ensureSchema(prisma)
    console.log('[OK] Table "users" ensured.')

    await clearUsers(prisma)
    console.log('[OK] Clean finished for CRUD demo.')

    const alice = await createUser(prisma, { id: 1n, name: 'Alice', age: 30 })
    console.log('[OK] Created user:', alice)

    const bob = await createUser(prisma, { id: 2n, name: 'Bob', age: 25 })
    console.log('[OK] Created user:', bob)

    const fetchedAlice = await readUser(prisma, 1n)
    console.log('[OK] Read user #1:', fetchedAlice)

    const updatedBob = await updateUser(prisma, 2n, { name: 'Robert', age: 26 })
    console.log('[OK] Updated user #2:', updatedBob)

    const usersAfterUpdate = await listUsers(prisma)
    console.log('[OK] Users after update:', usersAfterUpdate)

    const deletedAlice = await deleteUser(prisma, 1n)
    console.log('[OK] Deleted user #1:', deletedAlice ? 'success' : 'failed')

    const remainingUsers = await listUsers(prisma)
    console.log('[OK] Remaining users:', remainingUsers)
  } catch (error) {
    console.error('[ERROR] Error during YDB interaction:', error)
  } finally {
    await prisma.$disconnect()
    console.log('[OK] Disconnected.')
  }
}

main().catch((error) => {
  console.error('[ERROR] Unhandled error:', error)
  process.exit(1)
})
