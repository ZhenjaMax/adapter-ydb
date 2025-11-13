# Adapter-YDB
Неофициальный адаптер Prisma ORM для работы с базой данных YDB через драйверный API. Пакет предоставляет фабрику `PrismaYdbAdapterFactory`, которая позволяет использовать Prisma Client поверх YQL-запросов.

Версия: v0.0.7.

# Установка
```bash
npm install adapter-ydb@alpha
```

## Пример запуска
Полный пример представлен в файле [`src/playground/main.ts`](src/playground/main.ts). Он демонстрирует CRUD-операции с таблицей `users` в YDB.

### Создание Prisma Schema
Пример схемы представлен в [`prisma/schema.prisma`](prisma/schema.prisma).
```schema
generator client {
  provider = "prisma-client-js"
}

datasource db {
  provider = "postgresql"
  url      = env("DATABASE_URL")
}
```
- `provider = "postgresql"` необходим для совместимости;
- `url = env("DATABASE_URL")` необходим для запуска клиента;
    - воспользуйтесь командой `npm prisma init`;
    - добавьте в файл `.env` API KEY для Prisma; на текущем этапе разработки Prisma Client требует URL, он фактически не используется и нужен лишь для совместимости; можно указать dummy-URL на `prisma+postgres://accelerate.prisma-data.net`.

### Покдлючение к YDB и генерация клиента Prisma
```ts
const endpoint = 'grpc://localhost:2136'
const database = '/local'

сonst factory = new PrismaYdbAdapterFactory({ endpoint, database })
const prisma = new PrismaClient({
    adapter: factory,
    log: ['error']
})
```

Для подключения потребуется развернуть локально YDB, настроенные контейнеры представлены в [`src/playground/docker-compose.yml`](src/playground/main.ts)

### Выполнение запросов
```ts
await prisma.$executeRawUnsafe(`CREATE TABLE IF NOT EXISTS users (
    id Uint64,
    name Utf8,
    age Int32,
    created_at Datetime,
    PRIMARY KEY (id)
);`)

await prisma.$executeRaw(sql`
    UPSERT INTO users (id, name, age, created_at)
    VALUES (${uint64(user.id)}, ${user.name}, ${int32(user.age)}, CurrentUtcDatetime());
`)
```
### Отключение от YDB
```ts
await prisma.$disconnect()
```

# Текущие задачи
- Обеспечить выполнение большинства операций через API Prisma ORM.
- Упростить архитектуру программного модуля.
    - Убрать обёртку импорта провайдера (provider) через PostgreSQL.
    - Решить проблему с невозможностью импорта в Prisma собственного провайдера  БД.
- Реализовать поддержку специальных сценариев пользования:
    - федеративные запросы;
    - транзакционные операции.
- Реализовать автоматическое тестирование.

## Лицензия
Проект распространяется по лицензии [MIT](LICENSE).
