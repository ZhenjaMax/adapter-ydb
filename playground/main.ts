import { PrismaClient } from '@prisma/client'
import { PrismaYdbAdapterFactory } from '../src/adapter-factory'

async function main() {
  // 1️⃣ Создаём фабрику адаптера с параметрами подключения к YDB
  const factory = new PrismaYdbAdapterFactory({
    endpoint: 'grpc://localhost:2136', // порт YDB из Docker
    database: '/local',                 // путь к базе (обычно /local)
    authToken: undefined                // если требуется — токен аутентификации
  })

  // 2️⃣ Инициализируем адаптер
  const adapter = await factory.create()

  // 3️⃣ Создаём PrismaClient с этим адаптером
  const prisma = new PrismaClient({
    adapter,
    log: ['query', 'info', 'warn', 'error'],
  })

  try {
    console.log('✅ Connected to YDB through Prisma adapter.')

    // 4️⃣ Пример выполнения raw-запроса (через YQL)
    const createTable = `
      CREATE TABLE IF NOT EXISTS users (
        id Uint64,
        name Utf8,
        age Int32,
        PRIMARY KEY (id)
      );
    `
    await prisma.$executeRawUnsafe(createTable)
    console.log('🛠 Table "users" ensured.')

    // 5️⃣ Вставка данных
    await prisma.$executeRawUnsafe(`
      UPSERT INTO users (id, name, age) VALUES (1, "Alice", 30);
    `)

    // 6️⃣ Чтение данных
    const users = await prisma.$queryRawUnsafe(`SELECT * FROM users;`)
    console.log('👥 Users:', users)

    // 7️⃣ Пример транзакции
    await prisma.$transaction(async (tx) => {
      await tx.$executeRawUnsafe(`UPSERT INTO users (id, name, age) VALUES (2, "Bob", 25);`)
      const count = await tx.$queryRawUnsafe(`SELECT COUNT(*) as cnt FROM users;`)
      console.log('📊 Users count inside transaction:', count)
    })

  } catch (err) {
    console.error('❌ Error during YDB interaction:', err)
  } finally {
    await prisma.$disconnect()
    await adapter.dispose()
    console.log('🔌 Disconnected.')
  }
}

main().catch((e) => {
  console.error(e)
  process.exit(1)
})
