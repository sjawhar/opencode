import { existsSync } from "fs"
import path from "path"
import { describe, expect, test } from "bun:test"
import { tmpdir } from "../fixture/fixture"

const root = path.join(import.meta.dirname, "../..")
const sid = "retry-session"

function env(tmp: string, file?: string) {
  return {
    ...process.env,
    OPENCODE_DB: file,
    OPENCODE_BUSY_TIMEOUT: "1",
    OPENCODE_GO: path.join(tmp, "go"),
    OPENCODE_READY: path.join(tmp, "ready"),
    OPENCODE_SESSION_ID: sid,
    XDG_CACHE_HOME: tmp,
    XDG_CONFIG_HOME: tmp,
    XDG_DATA_HOME: tmp,
    XDG_STATE_HOME: tmp,
  }
}

function call(kind: "global" | "shard", tmp: string, file?: string) {
  return Bun.spawn({
    cmd: [
      "bun",
      "-e",
      [
        'import { existsSync } from "fs"',
        'import { Effect } from "effect"',
        'import { Database } from "@opencode-ai/core/database/database"',
        'import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"',
        'import { EventSequenceTable } from "@opencode-ai/core/event/sql"',
        'const id = process.env.OPENCODE_SESSION_ID || "retry-session"',
        'const go = process.env.OPENCODE_GO || "go"',
        'const ready = process.env.OPENCODE_READY || "ready"',
        "let stats = null",
        "const program = Effect.gen(function* () {",
        "const database = yield* Database.Service",
        'const db = process.env.OPENCODE_KIND === "global" ? database.db : yield* database.session(id)',
        'yield* db.run(`PRAGMA busy_timeout = ${process.env.OPENCODE_BUSY_TIMEOUT || "1"}`).pipe(Effect.orDie)',
        'yield* Effect.promise(() => Bun.write(ready, "ready"))',
        "while (!existsSync(go)) yield* Effect.sleep(10)",
        "const write = (tx) => tx.insert(EventSequenceTable).values({ aggregate_id: id, seq: 1 }).run().pipe(Effect.as('ok'))",
        'const exit = yield* (process.env.OPENCODE_KIND === "global" ? database.transaction(write, { behavior: "immediate" }) : db.transaction(write, { behavior: "immediate" })).pipe(Effect.exit)',
        "stats = yield* database.stats",
        "if (exit._tag === 'Success') return exit.value",
        "return yield* Effect.failCause(exit.cause)",
        "})",
        "try {",
        "  const result = await Effect.runPromise(program.pipe(Effect.provide(AppNodeBuilder.build(Database.node)), Effect.scoped))",
        "  await Bun.write(Bun.stdout, JSON.stringify({ ok: true, result, stats }))",
        "} catch (err) {",
        "  await Bun.write(Bun.stdout, JSON.stringify({ ok: false, name: err?.name, message: err?.message, code: err?.code, errno: err?.errno, stats }))",
        "  process.exit(1)",
        "}",
      ].join(";"),
    ],
    cwd: root,
    env: {
      ...env(tmp, file),
      OPENCODE_KIND: kind,
    },
    stdout: "pipe",
    stderr: "pipe",
  })
}

function hold(file: string, tmp: string, ms: number) {
  return Bun.spawn({
    cmd: [
      "bun",
      "-e",
      [
        'import { Database } from "bun:sqlite"',
        'const db = new Database(process.env.OPENCODE_LOCK_DB || "")',
        'db.run("BEGIN IMMEDIATE")',
        "db.run(\"INSERT INTO event_sequence (aggregate_id, seq) VALUES ('hold', 1)\")",
        'await Bun.write(process.env.OPENCODE_LOCK || "lock", "lock")',
        'await Bun.sleep(Number(process.env.OPENCODE_HOLD || "0"))',
        'db.run("ROLLBACK")',
        "db.close()",
      ].join(";"),
    ],
    cwd: root,
    env: {
      ...process.env,
      OPENCODE_HOLD: String(ms),
      OPENCODE_LOCK: path.join(tmp, "lock"),
      OPENCODE_LOCK_DB: file,
    },
    stdout: "pipe",
    stderr: "pipe",
  })
}


async function wait(file: string, proc?: Bun.Subprocess, timeout: number = 5_000) {
  for (let i = 0; i < timeout / 10; i++) {
    if (existsSync(file)) return
    await Bun.sleep(10)
  }
  const stream = proc?.stderr
  const err = stream && typeof stream !== "number" ? await new Response(stream).text() : ""
  throw new Error(`timed out waiting for ${file}\n${err}`)
}

describe("Database.transaction busy retry", () => {
  test("retries global transactions with backoff and jitter", async () => {
    await using tmp = await tmpdir()
    const file = path.join(tmp.path, "global.db")
    const proc = call("global", tmp.path, file)
    await wait(path.join(tmp.path, "ready"), proc, 15_000)

    const lock = hold(file, tmp.path, 120)
    await wait(path.join(tmp.path, "lock"), lock)
    await Bun.write(path.join(tmp.path, "go"), "go")

    const code = await proc.exited
    await lock.exited
    const text = await new Response(proc.stdout).text()
    const out = JSON.parse(text) as {
      ok: boolean
      result?: string
      stats: Record<string, number> | null
    }

    expect(code).toBe(0)
    expect(out.ok).toBe(true)
    expect(out.result).toBe("ok")
    expect(out.stats).toBeDefined()
    expect(out.stats?.write).toBe(1)
    expect(out.stats?.retry).toBe(2)
    expect(out.stats?.exhausted).toBe(0)
  }, 15_000)

  test("rethrows the original busy error after retries exhaust", async () => {
    await using tmp = await tmpdir()
    const file = path.join(tmp.path, "global.db")
    const proc = call("global", tmp.path, file)
    await wait(path.join(tmp.path, "ready"), proc, 15_000)

    const lock = hold(file, tmp.path, 2000)
    await wait(path.join(tmp.path, "lock"), lock)
    await Bun.write(path.join(tmp.path, "go"), "go")

    const code = await proc.exited
    await lock.exited

    const text = await new Response(proc.stdout).text()
    const out = JSON.parse(text) as {
      ok: boolean
      name: string
      message: string
      code: string
      errno: number
      stats: Record<string, number> | null
    }

    expect(code).toBe(1)
    expect(out.ok).toBe(false)
    expect(out.name).toBe("effect/sql/SqlError")
    expect(out.message).toContain("Failed to execute statement")
    expect(out.stats).toBeDefined()
    expect(out.stats?.write).toBe(1)
    expect(out.stats?.retry).toBe(3)
    expect(out.stats?.exhausted).toBe(1)
  }, 15_000)

  test("shard transactions skip global retry overhead", async () => {
    await using tmp = await tmpdir()
    const proc = call("shard", tmp.path)

    const file = path.join(tmp.path, "opencode", "sessions", `${sid}.db`)
    await wait(path.join(tmp.path, "ready"), proc, 15_000)
    const lock = hold(file, tmp.path, 2000)
    await wait(path.join(tmp.path, "lock"), lock)
    await Bun.write(path.join(tmp.path, "go"), "go")

    const code = await proc.exited
    await lock.exited

    const text = await new Response(proc.stdout).text()
    const out = JSON.parse(text) as {
      ok: boolean
      name: string
      message: string
      stats: Record<string, number> | null
    }

    expect(code).toBe(1)
    expect(out.ok).toBe(false)
    expect(out.name).toBe("effect/sql/SqlError")
    expect(out.message).toContain("Failed to execute statement")
    expect(out.stats).toBeDefined()
    expect(out.stats?.write).toBe(0)
    expect(out.stats?.retry).toBe(0)
    expect(out.stats?.exhausted).toBe(0)
  }, 15_000)
})
