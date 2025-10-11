# Prisma YDB adapter

## Использование ydb-js-sdk (`@ydbjs/core`, `@ydbjs/query`)

В репозитории уже есть класс-обертка [`YdbClientWrapper`](src/client-wrapper.ts), который служит границей между Prisma и транспортом YDB. Сейчас он реализует лишь in-memory имитацию, поэтому для подключения к настоящему кластеру необходимо заменить содержимое методов `connect`, `executeQuery`, `beginTransaction`, `commitTransaction`, `rollbackTransaction` и `close` на вызовы SDK. Ниже приведён типовой план интеграции с официальными пакетами `ydb-js-sdk`.

### 1. Установите зависимости

```bash
npm install @ydbjs/core @ydbjs/query
```

### 2. Инициализируйте драйвер YDB

В методе `connect` создайте экземпляр `Driver` из `@ydbjs/core`, передав `endpoint`, `database` и механизм аутентификации (например, `TokenAuthService` для статических токенов или `getCredentialsFromEnv()` для работы в Yandex Cloud). После `await driver.initialize()` сохраните его в приватном поле и создайте пул сессий/клиент запросов (`QueryClient` или `SessionPool`) из `@ydbjs/query`, чтобы переиспользовать соединения.

```ts
// Псевдокод
this.driver = new Driver({
  endpoint: this.config.endpoint,
  database: this.config.database,
  authService: new TokenAuthService(this.config.authToken),
});
if (!(await this.driver.initialize())) {
  throw new Error('YDB driver is not ready');
}
this.queryClient = new QueryClient({ driver: this.driver });
```

### 3. Выполняйте запросы через Query API

Метод `executeQuery` должен брать сессию из пула (`await this.queryClient.withSession(...)`) и вызывать `session.executeQuery`. Параметры запроса удобно готовить с помощью уже существующего преобразования `YqlTypeMapper.toYdbParameter`, поэтому можно передавать результаты в `TypedValues`/`Params` из `@ydbjs/query`. Возвращайте структуру `YdbResultSet`, раскладывая `result.resultSets` YDB по колонкам и строкам.

### 4. Управляйте транзакциями

Методы `beginTransaction`/`commitTransaction`/`rollbackTransaction` могут использовать `session.beginTransaction`, `session.commitTransaction` и `session.rollbackTransaction` из `@ydbjs/query`. Храните идентификатор активной транзакции (например, `tx.id`) и передавайте его в `executeQuery` для продолжения работы в той же транзакции.

### 5. Завершайте работу

В `close` вызовите `this.queryClient.destroy()` (если используется пул) и `this.driver.destroy()` из `@ydbjs/core`, чтобы освободить соединения. После этого PrismaYdbAdapter продолжит использовать тот же интерфейс `YdbClientWrapper`, но уже поверх реального SDK.

Следуя этому плану, вы замените заглушку на полноценную интеграцию с YDB без изменения остальной части адаптера.
