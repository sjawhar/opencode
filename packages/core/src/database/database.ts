export * as Database from "./database"

import { EffectDrizzleSqlite } from "@opencode-ai/effect-drizzle-sqlite"
import { layer as sqliteLayer } from "#sqlite"
import { Cause, Context, Deferred, Duration, Effect, Exit, Layer, Scope } from "effect"
import { Global } from "../global"
import { Flag } from "../flag/flag"
import { isAbsolute, join } from "path"
import { DatabaseMigration } from "./migration"
import { makeGlobalNode } from "../effect/app-node"
import { InstallationChannel } from "../installation/version"
import { existsSync, mkdirSync, mkdtempSync } from "fs"
import { Database as NativeDatabase } from "bun:sqlite"
import { tmpdir } from "os"

const makeDatabase = EffectDrizzleSqlite.makeWithDefaults()
type DatabaseShape = Effect.Success<typeof makeDatabase>
type TransactionShape = Pick<DatabaseShape, "all" | "delete" | "get" | "insert" | "run" | "select" | "update">
type RunnableDatabase = Pick<DatabaseShape, "run">
type CopyDatabase = Pick<DatabaseShape, "all" | "run">
type MemoryCopyPair = {
  readonly tx: CopyDatabase
  readonly global: DatabaseShape
}
type MemoryTableSpec = {
  readonly table: string
  readonly column: string
  readonly columns: readonly string[]
}

export interface Interface {
  db: DatabaseShape
  sessionDir: string
  monitor: Effect.Effect<Monitor>
  stats: Effect.Effect<Stats>
  transaction: <A, R>(
    callback: (tx: TransactionShape) => Effect.Effect<A, unknown, R>,
    options?: TransactionOptions,
  ) => Effect.Effect<A, unknown, R>
  writeWithBusyRetry: <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>
  session: (id: string) => Effect.Effect<DatabaseShape>
  hasSession: (id: string) => Effect.Effect<boolean>
  sessionRoot: (id: string) => Effect.Effect<string | undefined>
  ensureShard: (id: string) => Effect.Effect<string | undefined>
  resolveSession: (id: string) => Effect.Effect<DatabaseShape>
  closeSession: (id: string) => Effect.Effect<void>
  resetSwept: Effect.Effect<void>
}

export interface Stats {
  write: number
  retry: number
  exhausted: number
}

export interface Monitor {
  wal_bytes: number
  checkpoint?: {
    blocked: number
    wal_pages: number
    checkpointed_pages: number
  }
  metrics: Record<string, number>
}

interface TransactionOptions {
  behavior?: "deferred" | "immediate" | "exclusive"
}

export class Service extends Context.Service<Service, Interface>()("@opencode/v2/storage/Database") {}

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const db = yield* makeDatabase
    const sessionDir = path() === ":memory:" ? mkdtempSync(join(tmpdir(), "opencode-sessions-")) : join(Global.Path.data, "sessions")
    const cache = new Map<string, { db: DatabaseShape; scope: Scope.Closeable }>()
    // In-flight seed per shard root. Concurrent resolvers must WAIT on it: a
    // non-blocking guard here let the losing caller use a half-seeded shard,
    // which surfaced as transient "Session not found" reads under load.
    const seeding = new Map<string, Deferred.Deferred<void>>()
    const swept = new Set<string>()
    const count: Stats = { write: 0, retry: 0, exhausted: 0 }

    yield* configure(db, true)
    yield* DatabaseMigration.apply(db)

    const session: Interface["session"] = Effect.fn("Database.session")(function* (id: string) {
      if (!/^[a-zA-Z0-9_-]+$/.test(id)) return yield* Effect.die(new Error(`invalid session id: ${id}`))
      const cached = cache.get(id)
      if (cached) return cached.db
      mkdirSync(sessionDir, { recursive: true })
      const scope = yield* Scope.make()
      const sqlite = yield* Layer.buildWithScope(sqliteLayer({ filename: join(sessionDir, `${id}.db`) }), scope)
      const next = yield* makeDatabase.pipe(Effect.provide(sqlite), Effect.orDie)
      yield* configure(next, false).pipe(Effect.orDie)
      yield* prepareShardMigrationState(next).pipe(Effect.orDie)
      yield* DatabaseMigration.apply(next).pipe(Effect.orDie)
      yield* next.run("PRAGMA foreign_keys = OFF").pipe(Effect.orDie)
      yield* next.run("CREATE TABLE IF NOT EXISTS _meta (key TEXT PRIMARY KEY, value TEXT)").pipe(Effect.orDie)
      cache.set(id, { db: next, scope })
      return next
    })

    const hasSession: Interface["hasSession"] = Effect.fn("Database.hasSession")(function* (id: string) {
      if (cache.has(id)) return true
      const file = join(sessionDir, `${id}.db`)
      if (!existsSync(file)) return false
      return yield* Effect.sync(() => {
        let readonly: NativeDatabase | undefined
        try {
          readonly = new NativeDatabase(file, { readonly: true, create: false })
          const rows = readonly
            .query("SELECT name FROM sqlite_master WHERE type = 'table' AND name IN ('message', 'part', 'todo', 'event_sequence', 'event')")
            .all() as Array<{ name: string }>
          return rows.length === 5
        } catch {
          return false
        } finally {
          readonly?.close()
        }
      })
    })

    const sessionRoot: Interface["sessionRoot"] = Effect.fn("Database.sessionRoot")(function* (id: string) {
      if (yield* hasSession(id)) return id
      const seen = new Set<string>()
      let next: string | undefined = id
      for (let hop = 0; hop < 100 && next; hop++) {
        if (seen.has(next)) return undefined
        seen.add(next)
        const row: { parent_id: string | null } | undefined = yield* db
          .get<{ parent_id: string | null }>(`SELECT parent_id FROM session WHERE id = '${quote(next)}'`)
          .pipe(Effect.orDie)
        const parent: string | undefined = row?.parent_id ?? undefined
        if (!parent) return undefined
        if (yield* hasSession(parent)) return parent
        next = parent
      }
      return undefined
    })

    const seedBody = Effect.fnUntraced(function* (root: string) {
        const shard = yield* session(root)
        const ids = yield* db
          .all<{ id: string }>(
            `WITH RECURSIVE t(id) AS (SELECT id FROM session WHERE id = '${quote(root)}' UNION ALL SELECT s.id FROM session s JOIN t ON s.parent_id = t.id) SELECT id FROM t`,
          )
          .pipe(Effect.orDie)
        if (ids.length === 0) return
        const list = ids.map((item) => `'${quote(item.id)}'`).join(",")
        const global = path()
        if (global === ":memory:") {
          yield* shard.transaction((tx) =>
            Effect.gen(function* () {
              yield* copyMemorySessionRows(tx, db, list)
              for (const id of ids) {
                yield* markSwept(tx, db, id.id)
              }
            }),
          ).pipe(Effect.orDie)
          return
        }
        yield* shard.run(`ATTACH DATABASE '${quote(global)}' AS global`).pipe(Effect.orDie)
        try {
          yield* shard.transaction((tx) =>
            Effect.gen(function* () {
              yield* copySessionRows(tx, list)
              for (const id of ids) {
                yield* markSwept(tx, db, id.id)
              }
            }),
          ).pipe(Effect.orDie)
        } finally {
          yield* shard.run("DETACH DATABASE global").pipe(Effect.ignore)
        }
    })

    const seed = Effect.fn("Database.seedShard")(function* (root: string) {
      // Check-and-set synchronously: a yield between the two would let a second
      // fiber start a duplicate seed of the same root.
      const inflight = seeding.get(root)
      if (inflight) return yield* Deferred.await(inflight)
      const gate = Deferred.makeUnsafe<void>()
      seeding.set(root, gate)
      yield* seedBody(root).pipe(
        Effect.ensuring(
          Effect.sync(() => {
            seeding.delete(root)
          }).pipe(Effect.andThen(Deferred.succeed(gate, undefined)), Effect.asVoid),
        ),
      )
    })

    const ensureShard: Interface["ensureShard"] = Effect.fn("Database.ensureShard")(function* (id: string) {
      const root = yield* sessionRoot(id)
      if (root) {
        // A shard file becomes visible before its seed transaction commits;
        // never hand it out while a seed for this root is still in flight.
        const inflight = seeding.get(root)
        if (inflight) yield* Deferred.await(inflight)
        if (!swept.has(id)) {
          swept.add(id)
          const shard = yield* session(root)
          yield* shard.run("CREATE TABLE IF NOT EXISTS _meta (key TEXT PRIMARY KEY, value TEXT)").pipe(Effect.orDie)
          if (yield* markerStale(shard, db, id)) yield* seed(root)
        }
        return root
      }
      const target = yield* findRoot(db, id)
      if (!target) return undefined
      // Never LAZILY create a shard off a :memory: global: every writer guards
      // on path() and keeps writing to the global db, so a shard created here
      // is a frozen seed-time snapshot that only readers see - a permanent
      // read/write split brain. Explicitly created shards (database.session)
      // still resolve through sessionRoot above.
      if (path() === ":memory:") return undefined
      yield* seed(target)
      return target
    })

    const resolveSession: Interface["resolveSession"] = Effect.fn("Database.resolveSession")(function* (id: string) {
      const root = yield* ensureShard(id)
      if (root) return yield* session(root)
      return db
    })

    const stats: Interface["stats"] = Effect.sync(() => ({ ...count }))

    const monitor: Interface["monitor"] = Effect.gen(function* () {
      const result: Monitor = {
        wal_bytes: 0,
        metrics: {
          "db.global.writes": count.write,
          "db.global.retries": count.retry,
          "db.global.busy_errors": count.exhausted,
        },
      }
      const dbPath = path()
      if (dbPath !== ":memory:") {
        const wal = `${dbPath}-wal`
        if (existsSync(wal)) {
          result.wal_bytes = Bun.file(wal).size
        }
        const checkpoint = yield* db
          .get<{ busy: number; log: number; checkpointed: number }>("PRAGMA wal_checkpoint(PASSIVE)")
          .pipe(Effect.orDie)
        if (checkpoint) {
          result.checkpoint = {
            blocked: checkpoint.busy,
            wal_pages: checkpoint.log,
            checkpointed_pages: checkpoint.checkpointed,
          }
        }
      }
      return result
    })

    const retryOnBusy = <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
      Effect.gen(function* () {
        count.write++
        let first: Cause.Cause<E> | undefined
        for (let i = 0; i <= retry.length; i++) {
          const exit = yield* effect.pipe(Effect.exit)
          if (Exit.isSuccess(exit)) return exit.value
          const type = busy(exit.cause)
          if (!type) return yield* Effect.failCause(exit.cause)
          first ??= exit.cause
          const base = retry[i]
          if (base === undefined) {
            count.exhausted++
            return yield* Effect.failCause(first)
          }
          const delay = backoff(base)
          count.retry++
          yield* Effect.sleep(Duration.millis(delay))
        }
        return yield* Effect.failCause(first!)
      })

    // Whole-transaction retry: re-runs db.transaction from a fresh BEGIN each attempt
    // (rollback+restart), which is the correct handling for SQLITE_BUSY and SQLITE_BUSY_SNAPSHOT.
    const transaction: Interface["transaction"] = (callback, options) =>
      retryOnBusy(db.transaction((tx) => callback(tx), { behavior: options?.behavior }))

    // For DIRECT autocommit writes that do not go through transaction(): a single statement
    // that returns SQLITE_BUSY did not complete, so re-running it is safe. Pass DB-only effects.
    const writeWithBusyRetry: Interface["writeWithBusyRetry"] = (effect) => retryOnBusy(effect)

    return {
      db,
      sessionDir,
      monitor,
      stats,
      transaction,
      writeWithBusyRetry,
      session,
      hasSession,
      sessionRoot,
      ensureShard,
      resolveSession,
      closeSession: (id: string) =>
        Effect.gen(function* () {
          const cached = cache.get(id)
          if (!cached) return
          cache.delete(id)
          yield* Scope.close(cached.scope, Exit.void)
        }),
      resetSwept: Effect.sync(() => swept.clear()),
    }
  }).pipe(Effect.orDie),
)

function quote(value: string) {
  return value.replaceAll("'", "''")
}

const retry = [50, 200, 500]

function backoff(ms: number) {
  return Math.round(ms * (0.75 + Math.random() * 0.5))
}

function busy(cause: Cause.Cause<unknown>) {
  const text = [Cause.pretty(cause), ...Cause.prettyErrors(cause).map((error) => render(error))].join("\n")
  if (text.includes("SQLITE_BUSY_SNAPSHOT")) return "SQLITE_BUSY_SNAPSHOT"
  if (text.includes("SQLITE_BUSY_RECOVERY")) return "SQLITE_BUSY_RECOVERY"
  if (text.includes("SQLITE_BUSY") || text.includes("database is locked")) return "SQLITE_BUSY"
}

function render(value: unknown, seen = new WeakSet<object>()): string {
  if (value === null || typeof value !== "object") return String(value)
  if (seen.has(value)) return "[Circular]"
  seen.add(value)
  return [
    value instanceof Error ? `${value.name}: ${value.message}` : "",
    ...Object.entries(value).map(([key, item]) => `${key}: ${render(item, seen)}`),
  ].join("\n")
}

function configure(db: DatabaseShape, global: boolean) {
  return Effect.gen(function* () {
    yield* db.run("PRAGMA journal_mode = WAL")
    yield* db.run("PRAGMA synchronous = NORMAL")
    // 0, not a positive value: bun:sqlite is synchronous on the main thread, so a
    // busy_timeout parks the whole event loop; SQLITE_BUSY is retried async in transaction().
    yield* db.run("PRAGMA busy_timeout = 0")
    // Cap the WAL file so it is truncated back down after a checkpoint can reset it, instead of
    // growing without bound (observed 79-224MB shard WALs). Does not block.
    yield* db.run("PRAGMA journal_size_limit = 67108864")
    yield* db.run("PRAGMA cache_size = -64000")
    yield* db.run(`PRAGMA foreign_keys = ${global ? "ON" : "OFF"}`)
    yield* db.run("PRAGMA mmap_size = 134217728")
    yield* db.run("PRAGMA temp_store = MEMORY")
    yield* db.run("PRAGMA wal_checkpoint(PASSIVE)")
  })
}

function prepareShardMigrationState(db: DatabaseShape) {
  return Effect.gen(function* () {
    const legacy = yield* db
      .get<{ name: string }>("SELECT name FROM sqlite_master WHERE type = 'table' AND name IN ('message', 'part', 'todo') LIMIT 1")
      .pipe(Effect.orDie)
    if (!legacy) return
    yield* db.run("CREATE TABLE IF NOT EXISTS migration (id TEXT PRIMARY KEY, time_completed INTEGER NOT NULL)")
    // This shard may have been migrated by Drizzle's journal (`__drizzle_migrations`)
    // before the custom runner existed. Adopt every applied migration name so
    // DatabaseMigration.apply() does NOT replay them: replaying e.g.
    // 20260211171708_add_project_commands fails with "duplicate column name: commands"
    // and aborts the shard open via Effect.orDie, which surfaces as a blank
    // conversation when resuming an existing session. migration.ts performs the
    // same seed, but only when its migration table starts empty — the explicit
    // marker inserts below would defeat that guard, so we seed here instead.
    const drizzle = yield* db
      .get<{ name: string }>("SELECT name FROM sqlite_master WHERE type = 'table' AND name = '__drizzle_migrations'")
      .pipe(Effect.orDie)
    if (drizzle) {
      yield* db
        .run(
          "INSERT OR IGNORE INTO migration (id, time_completed) SELECT name, strftime('%s','now') * 1000 FROM __drizzle_migrations WHERE name IS NOT NULL",
        )
        .pipe(Effect.orDie)
    }
    yield* db
      .run("INSERT OR IGNORE INTO migration (id, time_completed) VALUES ('20260127222353_familiar_lady_ursula', strftime('%s','now') * 1000)")
      .pipe(Effect.orDie)
    const cursor = yield* db
      .get<{ name: string }>("SELECT name FROM sqlite_master WHERE type = 'index' AND name = 'message_session_time_created_id_idx'")
      .pipe(Effect.orDie)
    if (cursor) {
      yield* db
        .run("INSERT OR IGNORE INTO migration (id, time_completed) VALUES ('20260312043431_session_message_cursor', strftime('%s','now') * 1000)")
        .pipe(Effect.orDie)
    }
    const events = yield* db
      .get<{ name: string }>("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'event'")
      .pipe(Effect.orDie)
    if (events) {
      yield* db
        .run("INSERT OR IGNORE INTO migration (id, time_completed) VALUES ('20260323234822_events', strftime('%s','now') * 1000)")
        .pipe(Effect.orDie)
    }
  })
}

function copySessionRows(tx: RunnableDatabase, ids: string) {
  return Effect.gen(function* () {
    yield* tx.run(`INSERT OR IGNORE INTO session (id, project_id, workspace_id, parent_id, slug, directory, path, title, version, share_url, summary_additions, summary_deletions, summary_files, summary_diffs, metadata, cost, tokens_input, tokens_output, tokens_reasoning, tokens_cache_read, tokens_cache_write, revert, permission, agent, model, time_created, time_updated, time_compacting, time_archived) SELECT id, project_id, workspace_id, parent_id, slug, directory, path, title, version, share_url, summary_additions, summary_deletions, summary_files, summary_diffs, metadata, cost, tokens_input, tokens_output, tokens_reasoning, tokens_cache_read, tokens_cache_write, revert, permission, agent, model, time_created, time_updated, time_compacting, time_archived FROM global.session WHERE id IN (${ids})`)
    yield* tx.run(`INSERT OR IGNORE INTO message (id, session_id, time_created, time_updated, data) SELECT id, session_id, time_created, time_updated, data FROM global.message WHERE session_id IN (${ids})`)
    yield* tx.run(`INSERT OR IGNORE INTO part (id, message_id, session_id, time_created, time_updated, data) SELECT id, message_id, session_id, time_created, time_updated, data FROM global.part WHERE session_id IN (${ids})`)
    yield* tx.run(`INSERT OR IGNORE INTO todo (session_id, content, status, priority, position, time_created, time_updated) SELECT session_id, content, status, priority, position, time_created, time_updated FROM global.todo WHERE session_id IN (${ids})`)
    yield* tx.run(`INSERT OR IGNORE INTO event_sequence (aggregate_id, seq, owner_id) SELECT aggregate_id, seq, owner_id FROM global.event_sequence WHERE aggregate_id IN (${ids})`)
    yield* tx.run(`INSERT OR IGNORE INTO event (id, aggregate_id, seq, type, data) SELECT id, aggregate_id, seq, type, data FROM global.event WHERE aggregate_id IN (${ids})`)
    // Reconcile the per-aggregate event-sequence counter to the actual max event seq. The
    // event_sequence copy above is INSERT OR IGNORE, so a shard seeded earlier (when global had
    // fewer events, or with a counter that never tracked global-side writes) keeps a stale seq.
    // Once writes route to the shard, a stale counter allocates a colliding seq and every prompt
    // fails with UNIQUE(aggregate_id, seq). Sync the counter up to the highest event actually present.
    yield* tx.run(`INSERT INTO event_sequence (aggregate_id, seq) SELECT aggregate_id, MAX(seq) FROM event WHERE aggregate_id IN (${ids}) GROUP BY aggregate_id ON CONFLICT(aggregate_id) DO UPDATE SET seq = MAX(event_sequence.seq, excluded.seq)`)
    yield* tx.run(`INSERT OR IGNORE INTO session_message (id, session_id, type, seq, time_created, time_updated, data) SELECT id, session_id, type, seq, time_created, time_updated, data FROM global.session_message WHERE session_id IN (${ids})`)
    yield* tx.run(`INSERT OR IGNORE INTO session_input (id, session_id, prompt, delivery, admitted_seq, promoted_seq, time_created) SELECT id, session_id, prompt, delivery, admitted_seq, promoted_seq, time_created FROM global.session_input WHERE session_id IN (${ids})`)
    yield* tx.run(`INSERT OR IGNORE INTO session_context_epoch (session_id, baseline, snapshot, baseline_seq) SELECT session_id, baseline, snapshot, baseline_seq FROM global.session_context_epoch WHERE session_id IN (${ids})`)
  })
}

function copyMemorySessionRows(tx: CopyDatabase, global: DatabaseShape, ids: string) {
  return Effect.gen(function* () {
    for (const spec of memoryCopyTables) {
      yield* copyMemoryTable({ tx, global }, spec, ids)
    }
    // Reconcile the event-sequence counter to the max event seq (same as copySessionRows); the
    // row copies above are INSERT OR IGNORE, so a stale counter would otherwise cause a colliding
    // seq once writes route to the shard.
    yield* tx.run(`INSERT INTO event_sequence (aggregate_id, seq) SELECT aggregate_id, MAX(seq) FROM event WHERE aggregate_id IN (${ids}) GROUP BY aggregate_id ON CONFLICT(aggregate_id) DO UPDATE SET seq = MAX(event_sequence.seq, excluded.seq)`)
  })
}

const memoryCopyTables = [
  {
    table: "session",
    column: "id",
    columns: [
      "id",
      "project_id",
      "workspace_id",
      "parent_id",
      "slug",
      "directory",
      "path",
      "title",
      "version",
      "share_url",
      "summary_additions",
      "summary_deletions",
      "summary_files",
      "summary_diffs",
      "metadata",
      "cost",
      "tokens_input",
      "tokens_output",
      "tokens_reasoning",
      "tokens_cache_read",
      "tokens_cache_write",
      "revert",
      "permission",
      "agent",
      "model",
      "time_created",
      "time_updated",
      "time_compacting",
      "time_archived",
    ],
  },
  { table: "message", column: "session_id", columns: ["id", "session_id", "time_created", "time_updated", "data"] },
  { table: "part", column: "session_id", columns: ["id", "message_id", "session_id", "time_created", "time_updated", "data"] },
  { table: "todo", column: "session_id", columns: ["session_id", "content", "status", "priority", "position", "time_created", "time_updated"] },
  { table: "event_sequence", column: "aggregate_id", columns: ["aggregate_id", "seq", "owner_id"] },
  { table: "event", column: "aggregate_id", columns: ["id", "aggregate_id", "seq", "type", "data"] },
  { table: "session_message", column: "session_id", columns: ["id", "session_id", "type", "seq", "time_created", "time_updated", "data"] },
  { table: "session_input", column: "session_id", columns: ["id", "session_id", "prompt", "delivery", "admitted_seq", "promoted_seq", "time_created"] },
  { table: "session_context_epoch", column: "session_id", columns: ["session_id", "baseline", "snapshot", "baseline_seq"] },
] as const satisfies readonly MemoryTableSpec[]

function copyMemoryTable(databases: MemoryCopyPair, spec: MemoryTableSpec, ids: string) {
  return Effect.gen(function* () {
    const rows = yield* databases.global
      .all<Record<string, unknown>>(`SELECT ${spec.columns.join(", ")} FROM ${spec.table} WHERE ${spec.column} IN (${ids})`)
      .pipe(Effect.orDie)
    for (const row of rows) {
      yield* databases.tx
        .run(`INSERT OR IGNORE INTO ${spec.table} (${spec.columns.join(", ")}) VALUES (${spec.columns.map((column) => sqlValue(row[column])).join(", ")})`)
        .pipe(Effect.orDie)
    }
  })
}

function sqlValue(value: unknown): string {
  if (value === undefined || value === null) return "NULL"
  if (typeof value === "number") return Number.isFinite(value) ? String(value) : "NULL"
  if (typeof value === "bigint") return String(value)
  if (typeof value === "boolean") return value ? "1" : "0"
  if (typeof value === "string") return `'${quote(value)}'`
  return `'${quote(JSON.stringify(value))}'`
}

function maxTime(db: DatabaseShape, table: string, sessionID: string) {
  const column = table === "session" ? "id" : "session_id"
  return db
    .get<{ value: number | null }>(`SELECT MAX(time_created) as value FROM ${table} WHERE ${column} = '${quote(sessionID)}'`)
    .pipe(Effect.map((row) => String(row?.value ?? 0)), Effect.orDie)
}

function markSwept(shard: RunnableDatabase, global: DatabaseShape, sessionID: string) {
  return Effect.gen(function* () {
    yield* shard.run("CREATE TABLE IF NOT EXISTS _meta (key TEXT PRIMARY KEY, value TEXT)")
    for (const table of ["session", "message", "part", "todo", "session_message", "session_input"]) {
      yield* shard
        .run(`INSERT OR REPLACE INTO _meta (key, value) VALUES ('swept:${table}:${quote(sessionID)}', '${yield* maxTime(global, table, sessionID)}')`)
        .pipe(Effect.orDie)
    }
  })
}

function markerStale(shard: DatabaseShape, global: DatabaseShape, sessionID: string) {
  return Effect.gen(function* () {
    for (const table of ["session", "message", "part", "todo", "session_message", "session_input"]) {
      const row = yield* shard
        .get<{ value: string }>(`SELECT value FROM _meta WHERE key = 'swept:${table}:${quote(sessionID)}'`)
        .pipe(Effect.orDie)
      if (!row || row.value !== (yield* maxTime(global, table, sessionID))) return true
    }
    // Event-log staleness. Under the pre-sharding write path, durable events were written to the
    // GLOBAL db while projected rows (messages/parts) went to the shard, so a shard can be behind
    // global on the event log even when the message/part markers above match. Re-seed when global
    // has newer events so the shard's event log is complete before writes route to it. Once the
    // shard leads (post-migration writes land there), global is no longer ahead and this is false.
    const globalEventMax = yield* global
      .get<{ value: number | null }>(`SELECT MAX(seq) AS value FROM event WHERE aggregate_id = '${quote(sessionID)}'`)
      .pipe(Effect.orDie)
    const shardEventMax = yield* shard
      .get<{ value: number | null }>(`SELECT MAX(seq) AS value FROM event WHERE aggregate_id = '${quote(sessionID)}'`)
      .pipe(Effect.orDie)
    if (Number(globalEventMax?.value ?? -1) > Number(shardEventMax?.value ?? -1)) return true
    return false
  })
}

function findRoot(db: DatabaseShape, id: string) {
  return Effect.gen(function* () {
    const seen = new Set<string>()
    let current: string | undefined = id
    for (let hop = 0; hop < 100 && current; hop++) {
      if (seen.has(current)) return undefined
      seen.add(current)
      const row: { parent_id: string | null } | undefined = yield* db
        .get<{ parent_id: string | null }>(`SELECT parent_id FROM session WHERE id = '${quote(current)}'`)
        .pipe(Effect.orDie)
      if (!row) return undefined
      if (!row.parent_id) return current
      current = row.parent_id
    }
    return undefined
  })
}

export function layerFromPath(filename: string) {
  return layer.pipe(Layer.provide(sqliteLayer({ filename })))
}

export function path() {
  if (Flag.OPENCODE_DB) {
    if (Flag.OPENCODE_DB === ":memory:" || isAbsolute(Flag.OPENCODE_DB)) return Flag.OPENCODE_DB
    return join(Global.Path.data, Flag.OPENCODE_DB)
  }
  if (
    ["latest", "beta", "prod"].includes(InstallationChannel) ||
    process.env.OPENCODE_DISABLE_CHANNEL_DB === "1" ||
    process.env.OPENCODE_DISABLE_CHANNEL_DB === "true"
  )
    return join(Global.Path.data, "opencode.db")
  return join(Global.Path.data, `opencode-${InstallationChannel.replace(/[^a-zA-Z0-9._-]/g, "-")}.db`)
}

export const node = makeGlobalNode({ service: Service, layer: layerFromPath(path()), deps: [] })
