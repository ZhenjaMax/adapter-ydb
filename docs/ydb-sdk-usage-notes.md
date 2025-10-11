# YDB SDK usage notes

- `YdbClientWrapper` wires Prisma queries to the official `@ydbjs` driver by creating a `Driver`, opening QueryService sessions, and streaming results with proper status checks before decoding values. This mirrors how official Prisma adapters lean on vendor SDKs like the `mssql` package.
- The wrapper keeps Prisma abstractions separated from raw YDB sessions by translating parameters (`prepareQuery`, `encodeParameters`) and normalizing YDB typed values through `fromYdb`/`toJs` helpers before returning them to Prisma.
- Transaction helpers (`beginTransaction`, `commitTransaction`, `rollbackTransaction`) pin sessions to transaction IDs and ensure sessions are deleted on completion, demonstrating adherence to the YDB transactional workflow.
