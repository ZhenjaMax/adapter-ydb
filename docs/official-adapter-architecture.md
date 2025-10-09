# Official Prisma SQL Adapter Architecture

## Interfaces exposed by `@prisma/driver-adapter-utils`

All official SQL adapters are thin wrappers around the interfaces exported by
`@prisma/driver-adapter-utils`. The key contracts are `SqlQueryable`,
`SqlDriverAdapter`, and `SqlDriverAdapterFactory`, which respectively describe a
query executor, a connection-aware adapter, and a factory that creates such
adapters.【F:node_modules/@prisma/driver-adapter-utils/dist/index.d.ts†L279-L327】

These interfaces require adapters to expose provider metadata, implement raw
query execution, support transaction lifecycles, and optionally provide
connection metadata such as maximum bind parameters and schema names. They also
define the shared `SqlQuery`/`SqlResultSet` formats that each adapter must map
its driver-specific inputs and outputs to.【F:node_modules/@prisma/driver-adapter-utils/dist/index.d.ts†L337-L367】

## Common structure shared by the MSSQL and MariaDB adapters

The `@prisma/adapter-mssql` and `@prisma/adapter-mariadb` packages follow an
identical high-level structure:

1. **Queryable base class** – wraps a pool/connection/transaction from the
   underlying driver, implements `SqlQueryable`, and provides shared
   implementations of `queryRaw`, `executeRaw`, and error handling. These
   classes surface the provider name and adapter identifier for downstream
   tooling.【F:node_modules/@prisma/adapter-mssql/dist/index.d.ts†L17-L25】【F:node_modules/@prisma/adapter-mariadb/dist/index.d.ts†L21-L29】
2. **Adapter class** – extends the queryable base to fulfill the
   `SqlDriverAdapter` contract, adding transaction management, disposal logic,
   and optional connection metadata. Driver-specific hooks (for example,
   connection error callbacks or capability discovery) live here.【F:node_modules/@prisma/adapter-mssql/dist/index.d.ts†L36-L45】【F:node_modules/@prisma/adapter-mariadb/dist/index.d.ts†L40-L49】
3. **Factory class** – implements `SqlDriverAdapterFactory`, holds the
   configuration/connection string, and constructs driver pools before returning
   adapter instances. Factories normalize connection strings and set up
   error-handling hooks before returning a ready-to-use adapter.【F:node_modules/@prisma/adapter-mssql/dist/index.d.ts†L28-L34】【F:node_modules/@prisma/adapter-mariadb/dist/index.d.ts†L32-L38】

Across both adapters the compiled JavaScript reveals further shared patterns:

- **Argument and result mapping** – helper functions convert Prisma arg
  metadata into driver parameters and normalize result rows/column metadata into
  the `SqlResultSet` shape. This includes bigint/date coercion, binary data
  conversion, and per-column type mapping.【F:node_modules/@prisma/adapter-mssql/dist/index.js†L257-L337】【F:node_modules/@prisma/adapter-mariadb/dist/index.js†L105-L179】
- **Driver error translation** – adapter-specific functions translate native
  error codes into Prisma `DriverAdapterError` variants so the query engine can
  react consistently across providers.【F:node_modules/@prisma/adapter-mssql/dist/index.js†L339-L518】【F:node_modules/@prisma/adapter-mariadb/dist/index.js†L181-L303】
- **Transaction wrappers** – dedicated transaction subclasses serialize access
  to the underlying driver (using mutexes for MSSQL and explicit connection
  cleanup for MariaDB) while reusing the base queryable logic for executing
  statements within a transaction.【F:node_modules/@prisma/adapter-mssql/dist/index.js†L565-L596】【F:node_modules/@prisma/adapter-mariadb/dist/index.js†L356-L425】

## Notable provider-specific behaviors

While the architecture is shared, each adapter layers in provider-specific
behavior:

- **MSSQL**
  - Parses SQL Server connection strings to fill `mssql.ConnectionPool`
    configuration, including isolation levels and pool timeouts.【F:node_modules/@prisma/adapter-mssql/dist/index.js†L45-L184】
  - Converts `mssql` metadata to Prisma column types and normalizes driver rows
    (for example, handling `UniqueIdentifier` case normalization and `Real`
    precision issues).【F:node_modules/@prisma/adapter-mssql/dist/index.js†L189-L323】
  - Uses driver events to surface pool/connection errors to optional callbacks
    and captures schema metadata for Prisma introspection.【F:node_modules/@prisma/adapter-mssql/dist/index.js†L597-L665】

- **MariaDB**
  - Infers adapter capabilities (e.g., relation join support) by querying the
    server version and mapping it to known feature sets.【F:node_modules/@prisma/adapter-mariadb/dist/index.js†L427-L471】
  - Rewrites `mysql://` connection strings to `mariadb://` so the driver can
    interpret them correctly before pool creation.【F:node_modules/@prisma/adapter-mariadb/dist/index.js†L427-L480】
  - Configures per-query options (array mode, JSON handling, BIT conversion) to
    make the MariaDB driver behave like Prisma expects.【F:node_modules/@prisma/adapter-mariadb/dist/index.js†L329-L345】

## Status of the PostgreSQL adapter

Attempting to install `@prisma/adapter-pg` from npm currently fails with a `403`
response, so the package sources were not available inside this environment for
inspection.【ed83fc†L1-L7】 Prisma hosts the adapter in the public
`prisma/prisma` monorepo, and based on the consistent patterns observed above we
can expect it to expose the same trio of queryable, adapter, and factory types
while applying PostgreSQL-specific argument/column/error conversions.

## Implications for a YDB adapter

Designing a YDB adapter in line with the official adapters means:

- Implementing a YDB-backed `SqlQueryable` wrapper that translates Prisma
  queries into the YDB SDK calls and maps results back into `SqlResultSet`.
- Extending that wrapper into an adapter with transaction management and
  disposal semantics aligned with YDB sessions.
- Providing a factory that accepts Prisma connection strings or config objects,
  normalizes them into YDB client settings, and surfaces driver lifecycle
  hooks (pool errors, schema inference, etc.).
- Mirroring the error translation and argument/result conversion layers so the
  Prisma query engine can remain agnostic of the underlying database driver.

Following this template will keep the YDB adapter aligned with the officially
supported adapters and reduce surprises when integrating with Prisma's query
engine.
