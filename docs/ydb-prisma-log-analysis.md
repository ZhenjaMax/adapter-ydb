# YDB + Prisma Playground Log Analysis

This document explains the runtime logs captured while running `src/playground/main.ts` against the local YDB playground and suggests ways to address the reported issues.

## Sequence overview

1. **Prisma Client generation** – `prisma generate` runs successfully and places the client in `node_modules/@prisma/client`.
2. **YDB startup warnings** – the emulator reports that console configs cannot be fetched and that the `profiles` import table failed to load. These usually indicate missing optional metadata in the local container and can be ignored unless you rely on those resources.
3. **Schema provisioning** – the script executes the `CREATE TABLE IF NOT EXISTS users` statement (see [`src/playground/main.ts`](../src/playground/main.ts#L17-L38)). YDB logs show a `StatusAlreadyExists` warning because the table already exists, which is expected for idempotent provisioning.
4. **Cleanup and data seeding** – the script attempts to delete previous rows and then upsert two rows.

## Cause of the failure

YDB rejects the `UPSERT` statement because the adapter currently binds JavaScript numbers as 64-bit integers, while the table schema expects different numeric types:

- `id` column is declared as `Uint64`, but bound values arrive as signed `Int64`.
- `age` column is declared as `Int32`, but bound values arrive as (optional) `Int32` and the adapter sends non-optional `Int64`.

The YDB compiler therefore raises `Failed to convert type: Struct<'age':Int64,'created_at':Datetime,'id':Int64,'name':Utf8> to Struct<'age':Int32?,'created_at':Datetime?,'id':Uint64?,'name':Utf8?>` and aborts with a `Type annotation` error. The Prisma Client then surfaces this as `PrismaClientKnownRequestError (P2010)`.

## Suggested next steps

- **Align bound types with the table schema.** Update the adapter layer so that numeric parameters destined for `Uint64` columns are encoded as unsigned values, and `Int32` columns receive 32-bit signed integers (or wrap literals with explicit YQL `CAST`).
- **Normalize literal handling in helper utilities.** Functions like `join([1n, 2n, 3n])` currently emit bare literals; ensure they inherit the correct YDB type wrappers before being interpolated into raw queries.
- **Add regression tests.** Extend the adapter's integration tests to cover common numeric combinations (Uint64, Int32) to catch similar mismatches.
- **Monitor startup warnings.** If the `profiles` table is needed, load it before running the playground; otherwise document that the warning is benign in local setups.
