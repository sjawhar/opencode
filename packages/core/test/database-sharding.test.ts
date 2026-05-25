import { describe, expect } from "bun:test"
import { existsSync, unlinkSync } from "fs"
import { Database as NativeDatabase } from "bun:sqlite"
import path from "path"
import { Effect } from "effect"
import { and, eq, gt, sql } from "drizzle-orm"
import { Database } from "@opencode-ai/core/database/database"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { EventSequenceTable, EventTable } from "@opencode-ai/core/event/sql"
import { EventV2 } from "@opencode-ai/core/event"
import { ModelV2 } from "@opencode-ai/core/model"
import { MessageTable, PartTable, SessionTable, TodoTable } from "@opencode-ai/core/session/sql"
import { Project } from "@opencode-ai/core/project"
import { ProjectTable } from "@opencode-ai/core/project/sql"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { SessionSchema } from "@opencode-ai/core/session/schema"
import { MessageID, PartID, SessionV1 } from "@opencode-ai/core/v1/session"
import { testEffect } from "./lib/effect"

const it = testEffect(AppNodeBuilder.build(Database.node))

const removeShard = (database: Database.Interface, sessionID: SessionSchema.ID) =>
  Effect.gen(function* () {
    yield* database.closeSession(sessionID)
    for (const ext of [".db", ".db-shm", ".db-wal"]) {
      const file = path.join(database.sessionDir, `${sessionID}${ext}`)
      if (existsSync(file)) unlinkSync(file)
    }
    yield* database.resetSwept
  })

const resetSession = (database: Database.Interface, sessionID: SessionSchema.ID) =>
  Effect.gen(function* () {
    yield* removeShard(database, sessionID)
    yield* database.db.delete(TodoTable).where(eq(TodoTable.session_id, sessionID)).run().pipe(Effect.orDie)
    yield* database.db.delete(PartTable).where(eq(PartTable.session_id, sessionID)).run().pipe(Effect.orDie)
    yield* database.db.delete(MessageTable).where(eq(MessageTable.session_id, sessionID)).run().pipe(Effect.orDie)
    yield* database.db.delete(EventTable).where(eq(EventTable.aggregate_id, sessionID)).run().pipe(Effect.orDie)
    yield* database.db.delete(SessionTable).where(eq(SessionTable.id, sessionID)).run().pipe(Effect.orDie)
  })

const createSession = (database: Database.Interface, sessionID: SessionSchema.ID, parentID?: SessionSchema.ID) =>
  database.db
    .insert(ProjectTable)
    .values({ id: Project.ID.global, worktree: AbsolutePath.make("/project"), sandboxes: [] })
    .onConflictDoNothing()
    .run()
    .pipe(
      Effect.orDie,
      Effect.flatMap(() =>
        database.db
          .insert(SessionTable)
          .values({
            id: sessionID,
            project_id: Project.ID.global,
            slug: sessionID,
            directory: "/project",
            parent_id: parentID,
            title: sessionID,
            version: "test",
          })
          .onConflictDoNothing()
          .run()
          .pipe(Effect.orDie),
      ),
    )

describe("Database session shards", () => {
  it.live("opens session shards with shard-specific pragmas and migrations", () =>
    Effect.gen(function* () {
      const database = yield* Database.Service
      const sessionID = SessionSchema.ID.make("ses_shard_pragmas")
      yield* resetSession(database, sessionID)
      const shard = yield* database.session(sessionID)
      const tables = yield* shard
        .all<{ name: string }>(
          sql`SELECT name FROM sqlite_master WHERE type = 'table' AND name IN ('migration', 'session', 'session_message') ORDER BY name`,
        )
        .pipe(Effect.orDie)
      const foreignKeys = yield* shard.get<{ foreign_keys: number }>(sql`PRAGMA foreign_keys`).pipe(Effect.orDie)
      const busyTimeout = yield* shard.get<{ timeout: number }>(sql`PRAGMA busy_timeout`).pipe(Effect.orDie)
      const globalBusyTimeout = yield* database.db.get<{ timeout: number }>(sql`PRAGMA busy_timeout`).pipe(Effect.orDie)

      expect(tables.map((row) => row.name)).toEqual(["migration", "session", "session_message"])
      expect(foreignKeys?.foreign_keys).toBe(0)
      // busy_timeout must be 0 on both global and shard connections: bun:sqlite is
      // synchronous on the main thread, so any positive timeout parks the whole event
      // loop under SQLITE_BUSY (the nested-subagent lockup). Retry is async in transaction().
      expect(busyTimeout?.timeout).toBe(0)
      expect(globalBusyTimeout?.timeout).toBe(0)
      yield* removeShard(database, sessionID)
    }),
  )

  it.live("routes child sessions through the root shard", () =>
    Effect.gen(function* () {
      const database = yield* Database.Service
      const parentID = SessionSchema.ID.make("ses_shard_parent")
      const childID = SessionSchema.ID.make("ses_shard_child")
      yield* resetSession(database, childID)
      yield* resetSession(database, parentID)
      yield* createSession(database, parentID)
      yield* createSession(database, childID, parentID)
      yield* database.session(parentID)

      expect(yield* database.hasSession(parentID)).toBe(true)
      expect(yield* database.hasSession(childID)).toBe(false)
      expect(yield* database.sessionRoot(childID)).toBe(parentID)
      expect(yield* database.ensureShard(childID)).toBe(parentID)
      expect(yield* database.resolveSession(childID)).toBe(yield* database.session(parentID))

      yield* removeShard(database, parentID)
      yield* removeShard(database, childID)
    }),
  )

  it.live("copies legacy global session rows into a lazily-created shard", () =>
    Effect.gen(function* () {
      const database = yield* Database.Service
      const sessionID = SessionSchema.ID.make("ses_shard_backfill")
      const messageID = MessageID.ascending()
      const partID = PartID.ascending()
      const now = Date.now()
      yield* resetSession(database, sessionID)
      yield* createSession(database, sessionID)
      const messageData = {
        role: "user",
        time: { created: now },
        agent: "test",
        model: { providerID: ProviderV2.ID.make("test"), modelID: ModelV2.ID.make("test") },
        tools: {},
      } satisfies Omit<SessionV1.User, "id" | "sessionID">
      const partData = { type: "text", text: "legacy" } satisfies Omit<
        SessionV1.TextPart,
        "id" | "sessionID" | "messageID"
      >
      yield* database.db
        .insert(MessageTable)
        .values({ id: messageID, session_id: sessionID, time_created: now, data: messageData })
        .run()
        .pipe(Effect.orDie)
      yield* database.db
        .insert(PartTable)
        .values({ id: partID, message_id: messageID, session_id: sessionID, time_created: now, data: partData })
        .run()
        .pipe(Effect.orDie)
      yield* database.db
        .insert(TodoTable)
        .values({ session_id: sessionID, content: "copy", status: "pending", priority: "high", position: 0 })
        .run()
        .pipe(Effect.orDie)

      const shard = yield* database.resolveSession(sessionID)
      expect(yield* shard.select({ id: MessageTable.id }).from(MessageTable).where(eq(MessageTable.id, messageID)).get()).toEqual({ id: messageID })
      expect(yield* shard.select({ id: PartTable.id }).from(PartTable).where(eq(PartTable.id, partID)).get()).toEqual({ id: partID })
      expect(yield* shard.select({ content: TodoTable.content }).from(TodoTable).where(eq(TodoTable.session_id, sessionID)).get()).toEqual({ content: "copy" })

      yield* resetSession(database, sessionID)
    }),
  )

  it.live("re-seeds a shard whose event log lags global and reconciles the sequence counter", () =>
    Effect.gen(function* () {
      const database = yield* Database.Service
      const sessionID = SessionSchema.ID.make("ses_shard_stale_events")
      yield* resetSession(database, sessionID)
      yield* createSession(database, sessionID)
      yield* database.db
        .insert(EventSequenceTable)
        .values({ aggregate_id: sessionID, seq: 5 })
        .onConflictDoUpdate({ target: EventSequenceTable.aggregate_id, set: { seq: 5 } })
        .run()
        .pipe(Effect.orDie)
      for (let seq = 0; seq <= 5; seq++) {
        yield* database.db
          .insert(EventTable)
          .values({ id: EventV2.ID.create(), aggregate_id: sessionID, seq, type: "session.updated.1", data: {} })
          .run()
          .pipe(Effect.orDie)
      }
      // Create the shard explicitly: a :memory: global never creates shards
      // lazily (writers would keep writing to the global db), and this test is
      // about re-seed semantics for a shard that already exists.
      yield* database.session(sessionID)
      const shard = yield* database.resolveSession(sessionID)
      // Reproduce the split-brain: shard frozen behind global (0..2) with a stale counter (0).
      yield* shard.delete(EventTable).where(and(eq(EventTable.aggregate_id, sessionID), gt(EventTable.seq, 2))).run().pipe(Effect.orDie)
      yield* shard.update(EventSequenceTable).set({ seq: 0 }).where(eq(EventSequenceTable.aggregate_id, sessionID)).run().pipe(Effect.orDie)
      yield* database.closeSession(sessionID)
      yield* database.resetSwept
      const reshard = yield* database.resolveSession(sessionID)
      const count = yield* reshard.select({ value: sql<number>`count(*)` }).from(EventTable).where(eq(EventTable.aggregate_id, sessionID)).get().pipe(Effect.orDie)
      const counter = yield* reshard.select({ seq: EventSequenceTable.seq }).from(EventSequenceTable).where(eq(EventSequenceTable.aggregate_id, sessionID)).get().pipe(Effect.orDie)
      expect(Number(count?.value)).toBe(6)
      expect(Number(counter?.seq)).toBe(5)
      yield* reshard
        .insert(EventTable)
        .values({ id: EventV2.ID.create(), aggregate_id: sessionID, seq: 6, type: "session.updated.1", data: {} })
        .run()
        .pipe(Effect.orDie)
      yield* resetSession(database, sessionID)
    }),
  )

  it.live("adopts drizzle journal on shard open so existing shards are not re-migrated", () =>
    Effect.gen(function* () {
      const database = yield* Database.Service
      const sessionID = SessionSchema.ID.make("ses_drizzle_journal_shard")
      yield* removeShard(database, sessionID)

      // Build a normal shard via the custom runner so it has the full current
      // schema (including project.commands from 20260211171708_add_project_commands).
      const created = yield* database.session(sessionID)
      const now = Date.now()
      const messageData = {
        role: "user",
        time: { created: now },
        agent: "test",
        model: { providerID: ProviderV2.ID.make("test"), modelID: ModelV2.ID.make("test") },
        tools: {},
      } satisfies Omit<SessionV1.User, "id" | "sessionID">
      yield* created
        .insert(MessageTable)
        .values({ id: MessageID.ascending(), session_id: sessionID, time_created: now, data: messageData })
        .run()
        .pipe(Effect.orDie)
      const applied = yield* created.all<{ id: string }>(sql`SELECT id FROM migration`).pipe(Effect.orDie)
      expect(applied.length).toBeGreaterThan(3)

      // Rewrite the shard into the legacy "Drizzle journal" state: drop the custom
      // `migration` table and record the applied migrations in `__drizzle_migrations`
      // instead, exactly like a shard last migrated by the pre-runner Drizzle system.
      yield* database.closeSession(sessionID)
      yield* database.resetSwept
      yield* Effect.sync(() => {
        const raw = new NativeDatabase(path.join(database.sessionDir, `${sessionID}.db`))
        try {
          raw.run("DROP TABLE migration")
          raw.run(
            "CREATE TABLE __drizzle_migrations (id INTEGER PRIMARY KEY, hash text NOT NULL, created_at numeric, name text, applied_at TEXT)",
          )
          const insert = raw.prepare("INSERT INTO __drizzle_migrations (hash, name) VALUES ('', ?)")
          for (const row of applied) insert.run(row.id)
        } finally {
          raw.close()
        }
      })

      // Reopening must NOT replay migrations (replay fails with "duplicate column
      // name: commands" and aborts the open -> blank conversation on resume). It
      // must adopt the journal and still read the message back.
      const reopened = yield* database.session(sessionID)
      const rows = yield* reopened.all<{ c: number }>(sql`SELECT count(*) as c FROM message`).pipe(Effect.orDie)
      expect(rows[0]?.c).toBe(1)

      yield* removeShard(database, sessionID)
    }),
  )
})
