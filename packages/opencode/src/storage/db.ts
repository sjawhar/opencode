import { migrate } from "drizzle-orm/bun-sqlite/migrator"
import { type SQLiteTransaction } from "drizzle-orm/sqlite-core"
export * from "drizzle-orm"
import { Database as SQLite } from "bun:sqlite"
import { LocalContext } from "@/util/local-context"
import { lazy } from "../util/lazy"
import { Global } from "@opencode-ai/core/global"
import * as Log from "@opencode-ai/core/util/log"
import { NamedError } from "@opencode-ai/core/util/error"
import z from "zod"
import path from "path"
import { readFileSync, readdirSync, existsSync, mkdirSync } from "fs"
import { Flag } from "@opencode-ai/core/flag/flag"
import { InstallationChannel } from "@opencode-ai/core/installation/version"
import { InstanceState } from "@/effect/instance-state"
import { iife } from "@/util/iife"
import { init } from "#db"

declare const OPENCODE_MIGRATIONS: { sql: string; timestamp: number; name: string }[] | undefined

export const NotFoundError = NamedError.create(
  "NotFoundError",
  z.object({
    message: z.string(),
  }),
)

const log = Log.create({ service: "db" })

export function getChannelPath() {
  if (["latest", "beta", "prod"].includes(InstallationChannel) || Flag.OPENCODE_DISABLE_CHANNEL_DB)
    return path.join(Global.Path.data, "opencode.db")
  const safe = InstallationChannel.replace(/[^a-zA-Z0-9._-]/g, "-")
  return path.join(Global.Path.data, `opencode-${safe}.db`)
}

export const Path = iife(() => {
  if (Flag.OPENCODE_DB) {
    if (Flag.OPENCODE_DB === ":memory:" || path.isAbsolute(Flag.OPENCODE_DB)) return Flag.OPENCODE_DB
    return path.join(Global.Path.data, Flag.OPENCODE_DB)
  }
  return getChannelPath()
})

export type Transaction = SQLiteTransaction<"sync", void>

type Client = ReturnType<typeof init>

type Entry = {
  db: Client
  at: number
}

type Journal = { sql: string; timestamp: number; name: string }[]

// Drizzle's migrate overloads trigger expensive variance checks here; narrow to the journal overload we actually use.
const migrateFromJournal = migrate as unknown as (db: Client, entries: Journal) => void

function applyMigrations(db: Client, entries: Journal) {
  migrateFromJournal(db, entries)
}

function hasTable(db: Client, name: string) {
  return !!db.$client.query("SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?").get(name)
}

function hasIndex(db: Client, name: string) {
  return !!db.$client.query("SELECT name FROM sqlite_master WHERE type = 'index' AND name = ?").get(name)
}

function hasMigration(db: Client, name: string) {
  if (!hasTable(db, "__drizzle_migrations")) return false
  return !!db.$client.query("SELECT name FROM __drizzle_migrations WHERE name = ?").get(name)
}

function markMigration(db: Client, entry: Journal[number]) {
  if (hasMigration(db, entry.name)) return
  db.$client
    .query("INSERT INTO __drizzle_migrations (hash, created_at, name, applied_at) VALUES (?, ?, ?, ?)")
    .run("", entry.timestamp, entry.name, new Date().toISOString())
}

function ensureShardMigrationState(db: Client, entries: Journal) {
  const legacy = hasTable(db, "message") || hasTable(db, "part") || hasTable(db, "todo")
  if (!legacy) return
  db.run(
    "CREATE TABLE IF NOT EXISTS __drizzle_migrations (id INTEGER PRIMARY KEY, hash text NOT NULL, created_at numeric, name text, applied_at TEXT)",
  )
  db.run(
    "CREATE TABLE IF NOT EXISTS project (id text PRIMARY KEY, worktree text NOT NULL, vcs text, name text, icon_url text, icon_color text, time_created integer NOT NULL, time_updated integer NOT NULL, time_initialized integer, sandboxes text NOT NULL)",
  )
  db.run(
    "CREATE TABLE IF NOT EXISTS message (id text PRIMARY KEY, session_id text NOT NULL, time_created integer NOT NULL, time_updated integer NOT NULL, data text NOT NULL, CONSTRAINT fk_message_session_id_session_id_fk FOREIGN KEY (session_id) REFERENCES session(id) ON DELETE CASCADE)",
  )
  db.run(
    "CREATE TABLE IF NOT EXISTS part (id text PRIMARY KEY, message_id text NOT NULL, session_id text NOT NULL, time_created integer NOT NULL, time_updated integer NOT NULL, data text NOT NULL, CONSTRAINT fk_part_message_id_message_id_fk FOREIGN KEY (message_id) REFERENCES message(id) ON DELETE CASCADE)",
  )
  db.run(
    "CREATE TABLE IF NOT EXISTS permission (project_id text PRIMARY KEY, time_created integer NOT NULL, time_updated integer NOT NULL, data text NOT NULL, CONSTRAINT fk_permission_project_id_project_id_fk FOREIGN KEY (project_id) REFERENCES project(id) ON DELETE CASCADE)",
  )
  db.run(
    "CREATE TABLE IF NOT EXISTS session (id text PRIMARY KEY, project_id text NOT NULL, parent_id text, slug text NOT NULL, directory text NOT NULL, title text NOT NULL, version text NOT NULL, share_url text, summary_additions integer, summary_deletions integer, summary_files integer, summary_diffs text, revert text, permission text, time_created integer NOT NULL, time_updated integer NOT NULL, time_compacting integer, time_archived integer, CONSTRAINT fk_session_project_id_project_id_fk FOREIGN KEY (project_id) REFERENCES project(id) ON DELETE CASCADE)",
  )
  db.run(
    "CREATE TABLE IF NOT EXISTS todo (session_id text NOT NULL, content text NOT NULL, status text NOT NULL, priority text NOT NULL, position integer NOT NULL, time_created integer NOT NULL, time_updated integer NOT NULL, CONSTRAINT todo_pk PRIMARY KEY(session_id, position), CONSTRAINT fk_todo_session_id_session_id_fk FOREIGN KEY (session_id) REFERENCES session(id) ON DELETE CASCADE)",
  )
  db.run(
    "CREATE TABLE IF NOT EXISTS session_share (session_id text PRIMARY KEY, id text NOT NULL, secret text NOT NULL, url text NOT NULL, time_created integer NOT NULL, time_updated integer NOT NULL, CONSTRAINT fk_session_share_session_id_session_id_fk FOREIGN KEY (session_id) REFERENCES session(id) ON DELETE CASCADE)",
  )
  db.run("CREATE INDEX IF NOT EXISTS message_session_idx ON message (session_id)")
  db.run("CREATE INDEX IF NOT EXISTS part_message_idx ON part (message_id)")
  db.run("CREATE INDEX IF NOT EXISTS part_session_idx ON part (session_id)")
  db.run("CREATE INDEX IF NOT EXISTS session_project_idx ON session (project_id)")
  db.run("CREATE INDEX IF NOT EXISTS session_parent_idx ON session (parent_id)")
  db.run("CREATE INDEX IF NOT EXISTS todo_session_idx ON todo (session_id)")
  const first = entries.find((entry) => entry.name === "20260127222353_familiar_lady_ursula")
  if (first) markMigration(db, first)
  const cursor = entries.find((entry) => entry.name === "20260312043431_session_message_cursor")
  if (cursor && hasIndex(db, "message_session_time_created_id_idx") && hasIndex(db, "part_message_id_id_idx")) {
    markMigration(db, cursor)
  }
  const events = entries.find((entry) => entry.name === "20260323234822_events")
  if (events && hasTable(db, "event_sequence") && hasTable(db, "event")) markMigration(db, events)
}

function applyShardMigrations(db: Client, entries: Journal) {
  ensureShardMigrationState(db, entries)
  applyMigrations(db, entries)
}

const limit = 50
const retry = [50, 200, 800]
const count = {
  write: 0,
  retry: 0,
  exhausted: 0,
}
const buf = new Int32Array(new SharedArrayBuffer(4))

const cache = new Map<string, Entry>()
const migrating = new Set<string>()
const swept = new Set<string>()

const schema = [
  `CREATE TABLE IF NOT EXISTS message (
    id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    time_created INTEGER,
    time_updated INTEGER,
    data TEXT NOT NULL
  )`,
  `CREATE INDEX IF NOT EXISTS message_session_time_created_id_idx ON message (session_id, time_created, id)`,
  `CREATE TABLE IF NOT EXISTS part (
    id TEXT PRIMARY KEY,
    message_id TEXT NOT NULL REFERENCES message(id) ON DELETE CASCADE,
    session_id TEXT NOT NULL,
    time_created INTEGER,
    time_updated INTEGER,
    data TEXT NOT NULL
  )`,
  `CREATE INDEX IF NOT EXISTS part_message_id_id_idx ON part (message_id, id)`,
  `CREATE INDEX IF NOT EXISTS part_session_idx ON part (session_id)`,
  `CREATE TABLE IF NOT EXISTS todo (
    session_id TEXT NOT NULL,
    content TEXT NOT NULL,
    status TEXT NOT NULL,
    priority TEXT NOT NULL,
    position INTEGER NOT NULL,
    time_created INTEGER,
    time_updated INTEGER,
    PRIMARY KEY (session_id, position)
  )`,
  `CREATE INDEX IF NOT EXISTS todo_session_idx ON todo (session_id)`,
  `CREATE TABLE IF NOT EXISTS event_sequence (
    aggregate_id TEXT NOT NULL PRIMARY KEY,
    seq INTEGER NOT NULL
  )`,
  `CREATE TABLE IF NOT EXISTS event (
    id TEXT PRIMARY KEY,
    aggregate_id TEXT NOT NULL REFERENCES event_sequence(aggregate_id) ON DELETE CASCADE,
    seq INTEGER NOT NULL,
    type TEXT NOT NULL,
    data TEXT NOT NULL,
    origin TEXT
  )`,
]
const meta = `CREATE TABLE IF NOT EXISTS _meta (key TEXT PRIMARY KEY, value TEXT)`

const tables = ["message", "part", "todo", "event_sequence", "event"]

function time(tag: string) {
  const match = /^(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})/.exec(tag)
  if (!match) return 0
  return Date.UTC(
    Number(match[1]),
    Number(match[2]) - 1,
    Number(match[3]),
    Number(match[4]),
    Number(match[5]),
    Number(match[6]),
  )
}

function migrations(dir: string): Journal {
  const dirs = readdirSync(dir, { withFileTypes: true })
    .filter((entry) => entry.isDirectory())
    .map((entry) => entry.name)

  const sql = dirs
    .map((name) => {
      const file = path.join(dir, name, "migration.sql")
      if (!existsSync(file)) return
      return {
        sql: readFileSync(file, "utf-8"),
        timestamp: time(name),
        name,
      }
    })
    .filter(Boolean) as Journal

  return sql.sort((a, b) => a.timestamp - b.timestamp)
}

function pragma(db: Client, isGlobal: boolean = false) {
  db.run("PRAGMA journal_mode = WAL")
  db.run("PRAGMA synchronous = NORMAL")
  const timeout = isGlobal ? 10000 : 5000
  db.run(`PRAGMA busy_timeout = ${timeout}`)
  db.run("PRAGMA cache_size = -64000")
  db.run(`PRAGMA foreign_keys = ${isGlobal ? "ON" : "OFF"}`)
  db.run("PRAGMA mmap_size = 134217728")
  db.run("PRAGMA temp_store = MEMORY")
  db.run("PRAGMA wal_checkpoint(PASSIVE)")
}

function wait(ms: number) {
  if (ms <= 0) return
  Atomics.wait(buf, 0, 0, ms)
}

function backoff(ms: number) {
  return Math.round(ms * (0.75 + Math.random() * 0.5))
}

function busy(err: unknown) {
  if (!(err instanceof Error)) return
  const code = "code" in err && typeof err.code === "string" ? err.code : undefined
  if (code?.startsWith("SQLITE_BUSY")) return code
  if (err.message.includes("SQLITE_BUSY_SNAPSHOT")) return "SQLITE_BUSY_SNAPSHOT"
  if (err.message.includes("SQLITE_BUSY_RECOVERY")) return "SQLITE_BUSY_RECOVERY"
  if (err.message.includes("SQLITE_BUSY") || err.message.includes("database is locked")) return "SQLITE_BUSY"
}

function evict() {
  if (cache.size <= limit) return
  const sorted = [...cache.entries()].sort((a, b) => a[1].at - b[1].at)
  while (cache.size > limit && sorted.length) {
    const item = sorted.shift()!
    item[1].db.$client.close()
    cache.delete(item[0])
  }
}

const idle = 5 * 60_000
function metrics() {
  return {
    "db.global.writes": count.write,
    "db.global.retries": count.retry,
    "db.global.busy_errors": count.exhausted,
  }
}

export function monitor() {
  try {
    const result = {
      wal_bytes: 0,
      checkpoint: undefined as
        | {
            blocked: number
            wal_pages: number
            checkpointed_pages: number
          }
        | undefined,
      metrics: metrics(),
    }
    if (!Path.endsWith(":memory:")) {
      const db = Client()
      const file = `${Path}-wal`
      if (existsSync(file)) {
        const bytes = Bun.file(file).size
        const mb = (bytes / (1024 * 1024)).toFixed(2)
        result.wal_bytes = bytes
        if (bytes > 50 * 1024 * 1024) {
          log.warn("wal.size.large", { bytes, mb })
        } else {
          log.info("wal.size", { bytes, mb })
        }
      }
      const checkpoint = db.$client.query("PRAGMA wal_checkpoint(PASSIVE)").get() as {
        busy: number
        log: number
        checkpointed: number
      } | null
      if (checkpoint) {
        result.checkpoint = {
          blocked: checkpoint.busy,
          wal_pages: checkpoint.log,
          checkpointed_pages: checkpoint.checkpointed,
        }
        if (checkpoint.log > checkpoint.checkpointed) {
          log.warn("wal.checkpoint.incomplete", result.checkpoint)
        } else {
          log.info("wal.checkpoint.complete", result.checkpoint)
        }
      }
    }
    log.info("db.metrics", result.metrics)
    return result
  } catch (err) {
    log.warn("wal.monitoring.error", { error: err })
    return {
      wal_bytes: 0,
      checkpoint: undefined,
      metrics: metrics(),
    }
  }
}

const sweep = setInterval(() => {
  const cutoff = Date.now() - idle
  for (const [id, item] of cache) {
    if (item.at < cutoff) {
      item.db.$client.close()
      cache.delete(id)
    }
  }
  monitor()
}, 60_000)
if (typeof sweep === "object" && "unref" in sweep) sweep.unref()

export const Client = lazy(() => {
  log.info("opening database", { path: Path })

  const db = init(Path)
  const versionRow = db.$client.query("SELECT sqlite_version()").get() as Record<string, string>
  const version = versionRow ? Object.values(versionRow)[0] : "unknown"
  log.info("sqlite version", { version })
  const [major, minor, patch] = version.split(".").map(Number)
  if (major < 3 || (major === 3 && minor < 51) || (major === 3 && minor === 51 && patch < 3)) {
    log.warn("SQLite < 3.51.3 — WAL-reset race possible", { version })
  }

  pragma(db, true)

  // Apply schema migrations
  const entries =
    typeof OPENCODE_MIGRATIONS !== "undefined"
      ? OPENCODE_MIGRATIONS
      : migrations(path.join(import.meta.dirname, "../../migration"))
  if (entries.length > 0) {
    log.info("applying migrations", {
      count: entries.length,
      mode: typeof OPENCODE_MIGRATIONS !== "undefined" ? "bundled" : "dev",
    })
    if (Flag.OPENCODE_SKIP_MIGRATIONS) {
      for (const item of entries) {
        item.sql = "select 1;"
      }
    }
    applyMigrations(db, entries)
  }

  try {
    db.run("UPDATE session SET origin_machine = 'unknown' WHERE origin_machine IS NULL")
  } catch (err) {
    log.warn("origin_machine backfill failed", { error: err })
  }

  return db
})

export const sessionDir = iife(() => path.join(Global.Path.data, "sessions"))

export function session(id: string) {
  if (!/^[a-zA-Z0-9_-]+$/.test(id)) throw new Error(`invalid session id: ${id}`)
  const item = cache.get(id)
  if (item) {
    item.at = Date.now()
    return item.db
  }

  mkdirSync(sessionDir, { recursive: true })
  const file = path.join(sessionDir, id + ".db")
  const db = init(file)
  pragma(db, false)

  const entries =
    typeof OPENCODE_MIGRATIONS !== "undefined"
      ? OPENCODE_MIGRATIONS
      : migrations(path.join(import.meta.dirname, "../../migration"))
  if (entries.length > 0) applyShardMigrations(db, entries)
  db.run("PRAGMA foreign_keys = OFF")
  db.run(meta)

  cache.set(id, { db, at: Date.now() })
  evict()
  return db
}

export function hasSession(id: string) {
  if (cache.has(id)) return true
  const file = path.join(sessionDir, id + ".db")
  if (!existsSync(file)) return false
  let db: SQLite | undefined
  try {
    db = new SQLite(file, { readonly: true, create: false })
    const rows = db
      .query("SELECT name FROM sqlite_master WHERE type = 'table' AND name IN (?, ?, ?, ?, ?)")
      .all(...tables) as Array<{ name: string }>
    return rows.length === tables.length
  } catch {
    return false
  } finally {
    db?.close()
  }
}

export function sessionRoot(id: string) {
  if (hasSession(id)) return id

  const seen = new Set<string>()
  let next: string | undefined = id

  for (let hop = 0; hop < 100 && next; hop++) {
    if (seen.has(next)) {
      log.warn("parent chain cycle detected", { id })
      return
    }

    seen.add(next)

    const row = Client().$client.query("SELECT parent_id FROM session WHERE id = ?").get(next) as {
      parent_id: string | null
    } | null
    const parent = row?.parent_id ?? undefined
    if (!parent) return
    if (hasSession(parent)) return parent
    next = parent
  }

  if (next) log.warn("parent chain cycle detected", { id })
}

function findRoot(id: string): string | undefined {
  const seen = new Set<string>()
  let curr: string | undefined = id
  for (let hop = 0; hop < 100 && curr; hop++) {
    if (seen.has(curr)) return undefined
    seen.add(curr)
    const row = Client().$client.query("SELECT parent_id FROM session WHERE id = ?").get(curr) as {
      parent_id: string | null
    } | null
    if (!row) return undefined
    if (!row.parent_id) return curr
    curr = row.parent_id
  }
  return undefined
}

function seed(root: string) {
  if (migrating.has(root)) return
  migrating.add(root)
  try {
    const shard = session(root)
    const src = Client().$client
    const dst = shard.$client

    const tree = src
      .query(
        `WITH RECURSIVE t(id) AS (
          SELECT id FROM session WHERE id = ?
          UNION ALL
          SELECT s.id FROM session s JOIN t ON s.parent_id = t.id
        ) SELECT id FROM t`,
      )
      .all(root) as { id: string }[]
    const ids = tree.map((r) => r.id)
    if (ids.length === 0) return

    log.info("migrate.start", { root, sessions: ids.length })

    const ph = ids.map(() => "?").join(",")
    const mem = Path.endsWith(":memory:")
    if (!mem) dst.exec(`ATTACH DATABASE '${Path}' AS global`)
    try {
      dst.exec("BEGIN IMMEDIATE")
      try {
        if (mem) {
          const msg = dst.prepare(
            "INSERT OR IGNORE INTO message (id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?)",
          )
          for (const r of src.query(`SELECT * FROM message WHERE session_id IN (${ph})`).all(...ids) as any[])
            msg.run(r.id, r.session_id, r.time_created, r.time_updated, r.data)
          const part = dst.prepare(
            "INSERT OR IGNORE INTO part (id, message_id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?, ?)",
          )
          for (const r of src.query(`SELECT * FROM part WHERE session_id IN (${ph})`).all(...ids) as any[])
            part.run(r.id, r.message_id, r.session_id, r.time_created, r.time_updated, r.data)
          const todo = dst.prepare(
            "INSERT OR IGNORE INTO todo (session_id, content, status, priority, position, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?, ?)",
          )
          for (const r of src.query(`SELECT * FROM todo WHERE session_id IN (${ph})`).all(...ids) as any[])
            todo.run(r.session_id, r.content, r.status, r.priority, r.position, r.time_created, r.time_updated)
          const seq = dst.prepare("INSERT OR IGNORE INTO event_sequence (aggregate_id, seq) VALUES (?, ?)")
          for (const r of src.query(`SELECT * FROM event_sequence WHERE aggregate_id IN (${ph})`).all(...ids) as any[])
            seq.run(r.aggregate_id, r.seq)
          const evt = dst.prepare("INSERT OR IGNORE INTO event (id, aggregate_id, seq, type, data) VALUES (?, ?, ?, ?, ?)")
          for (const r of src.query(`SELECT * FROM event WHERE aggregate_id IN (${ph})`).all(...ids) as any[])
            evt.run(r.id, r.aggregate_id, r.seq, r.type, r.data)
        } else {
          dst
            .query(
              `INSERT OR IGNORE INTO message (id, session_id, time_created, time_updated, data) SELECT id, session_id, time_created, time_updated, data FROM global.message WHERE session_id IN (${ph})`,
            )
            .run(...ids)
          dst
            .query(
              `INSERT OR IGNORE INTO part (id, message_id, session_id, time_created, time_updated, data) SELECT id, message_id, session_id, time_created, time_updated, data FROM global.part WHERE session_id IN (${ph})`,
            )
            .run(...ids)
          dst
            .query(
              `INSERT OR IGNORE INTO todo (session_id, content, status, priority, position, time_created, time_updated) SELECT session_id, content, status, priority, position, time_created, time_updated FROM global.todo WHERE session_id IN (${ph})`,
            )
            .run(...ids)
          dst
            .query(
              `INSERT OR IGNORE INTO event_sequence (aggregate_id, seq) SELECT aggregate_id, seq FROM global.event_sequence WHERE aggregate_id IN (${ph})`,
            )
            .run(...ids)
          dst
            .query(
              `INSERT OR IGNORE INTO event (id, aggregate_id, seq, type, data) SELECT id, aggregate_id, seq, type, data FROM global.event WHERE aggregate_id IN (${ph})`,
            )
            .run(...ids)
        }
        dst.exec("COMMIT")
        dst.run(meta)
        for (const sid of ids) {
          const msg = src.query("SELECT MAX(time_created) as t FROM message WHERE session_id = ?").get(sid) as {
            t: number | null
          } | null
          const part = src.query("SELECT MAX(time_created) as t FROM part WHERE session_id = ?").get(sid) as {
            t: number | null
          } | null
          const todo = src.query("SELECT MAX(time_created) as t FROM todo WHERE session_id = ?").get(sid) as {
            t: number | null
          } | null
          dst
            .query("INSERT OR REPLACE INTO _meta (key, value) VALUES (?, ?)")
            .run(`swept:msg:${sid}`, String(msg?.t ?? 0))
          dst
            .query("INSERT OR REPLACE INTO _meta (key, value) VALUES (?, ?)")
            .run(`swept:part:${sid}`, String(part?.t ?? 0))
          dst
            .query("INSERT OR REPLACE INTO _meta (key, value) VALUES (?, ?)")
            .run(`swept:todo:${sid}`, String(todo?.t ?? 0))
        }
        log.info("migrate.done", { root, sessions: ids.length })
      } catch (err) {
        dst.exec("ROLLBACK")
        throw err
      }
    } finally {
      if (!mem) dst.exec("DETACH DATABASE global")
    }
  } finally {
    migrating.delete(root)
  }
}

export function ensureShard(id: string): string | undefined {
  const root = sessionRoot(id)
  if (root) {
    if (!swept.has(id)) {
      swept.add(id)
      const db = session(root).$client
      db.run(meta)
      const msg = db.query("SELECT value FROM _meta WHERE key = ?").get(`swept:msg:${id}`) as { value: string } | null
      const part = db.query("SELECT value FROM _meta WHERE key = ?").get(`swept:part:${id}`) as {
        value: string
      } | null
      const todo = db.query("SELECT value FROM _meta WHERE key = ?").get(`swept:todo:${id}`) as {
        value: string
      } | null
      const gmsg = Client()
        .$client.query("SELECT MAX(time_created) as t FROM message WHERE session_id = ?")
        .get(id) as { t: number | null } | null
      const gpart = Client().$client.query("SELECT MAX(time_created) as t FROM part WHERE session_id = ?").get(id) as {
        t: number | null
      } | null
      const gtodo = Client().$client.query("SELECT MAX(time_created) as t FROM todo WHERE session_id = ?").get(id) as {
        t: number | null
      } | null
      const stale =
        !msg ||
        String(gmsg?.t ?? 0) !== msg.value ||
        !part ||
        String(gpart?.t ?? 0) !== part.value ||
        !todo ||
        String(gtodo?.t ?? 0) !== todo.value
      if (stale) seed(root)
    }
    return root
  }

  const target = findRoot(id)
  if (!target) return undefined

  seed(target)
  return target
}

export function resolveSession(id: string) {
  const root = ensureShard(id)
  if (root) return session(root)
  return Client()
}

export function closeSession(id: string) {
  const item = cache.get(id)
  if (!item) return
  item.db.$client.close()
  cache.delete(id)
}

export function resetSwept() {
  swept.clear()
}

export function close() {
  if (!Client.loaded()) return
  for (const item of cache.values()) item.db.$client.close()
  cache.clear()
  swept.clear()
  Client().$client.close()
  Client.reset()
}

export function stats() {
  return { ...count }
}

export type TxOrDb = Transaction | Client

const ctx = LocalContext.create<{
  tx: TxOrDb
  effects: (() => void | Promise<void>)[]
}>("database")

export function use<T>(callback: (trx: TxOrDb) => T): T {
  try {
    return callback(ctx.use().tx)
  } catch (err) {
    if (err instanceof LocalContext.NotFound) {
      const effects: (() => void | Promise<void>)[] = []
      const result = ctx.provide({ effects, tx: Client() }, () => callback(Client()))
      for (const effect of effects) effect()
      return result
    }
    throw err
  }
}

export function effect(fn: () => any | Promise<any>) {
  const bound = InstanceState.bind(fn)
  try {
    ctx.use().effects.push(bound)
  } catch {
    bound()
  }
}

type NotPromise<T> = T extends Promise<any> ? never : T

export function transaction<T>(
  callback: (tx: TxOrDb) => NotPromise<T>,
  options?: {
    behavior?: "deferred" | "immediate" | "exclusive"
  },
): NotPromise<T> {
  try {
    return callback(ctx.use().tx)
  } catch (err) {
    if (err instanceof LocalContext.NotFound) {
      count.write += 1
      let first: unknown
      for (let i = 0; i <= retry.length; i++) {
        const effects: (() => void | Promise<void>)[] = []
        const txCallback = InstanceState.bind((tx: TxOrDb) => ctx.provide({ tx, effects }, () => callback(tx)))
        try {
          const result = Client().transaction(txCallback, { behavior: options?.behavior })
          for (const effect of effects) effect()
          return result as NotPromise<T>
        } catch (err) {
          const type = busy(err)
          if (!type) throw err
          first ??= err
          const base = retry[i]
          if (base === undefined) {
            count.exhausted += 1
            log.warn("sqlite.busy.exhausted", { attempt: i + 1, type, aggregate: stats() })
            throw first
          }
          const delay = backoff(base)
          count.retry += 1
          log.warn("sqlite.busy.retry", { attempt: i + 1, delay, type, aggregate: stats() })
          wait(delay)
        }
      }
      throw err
    }
    throw err
  }
}

export * as Database from "./db"
