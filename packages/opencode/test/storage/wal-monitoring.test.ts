import { describe, expect, test } from "bun:test"
import path from "path"
import { tmpdir } from "../fixture/fixture"

const root = path.join(import.meta.dirname, "../..")

async function probe(file: string, body: string[]) {
  const run = Bun.spawnSync({
    cmd: [
      "bun",
      "-e",
      [
        'import { Effect } from "effect"',
        'import { Database } from "@opencode-ai/core/database/database"',
        'import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"',
        "const program = Effect.gen(function* () {",
        "const database = yield* Database.Service",
        "const db = database.db",
        ...body,
        "return yield* database.monitor",
        "})",
        "const result = await Effect.runPromise(program.pipe(Effect.provide(AppNodeBuilder.build(Database.node)), Effect.scoped))",
        "await Bun.write(Bun.stdout, JSON.stringify(result))",
      ].join(";"),
    ],
    cwd: root,
    env: {
      ...process.env,
      OPENCODE_DB: file,
    },
    stdout: "pipe",
    stderr: "pipe",
  })
  if (run.exitCode !== 0) {
    throw new Error(run.stderr.toString() || run.stdout.toString())
  }
  return {
    out: JSON.parse(run.stdout.toString()) as {
      wal_bytes: number
      metrics: Record<string, number>
    },
    err: run.stderr.toString(),
  }
}

describe("WAL health monitoring", () => {
  test("Database.monitor logs wal size for file-backed databases", async () => {
    await using tmp = await tmpdir()
    const file = path.join(tmp.path, "wal.db")
    const result = await probe(file, [
      'yield* db.run("CREATE TABLE IF NOT EXISTS test (id TEXT PRIMARY KEY, data TEXT)").pipe(Effect.orDie)',
      'for (let i = 0; i < 200; i++) yield* db.run(`INSERT OR REPLACE INTO test VALUES (\'id-${i}\', \'data-${i}\')`).pipe(Effect.orDie)',
    ])

    // Monitor data is delivered through the returned payload; the console.error
    // logging these assertions used to grep for was deliberately removed in
    // "fix(db): async BUSY retry at write boundaries + bound WAL".
    expect(result.out.wal_bytes).toBeGreaterThan(0)
    expect(result.out.metrics["db.global.writes"]).toBeGreaterThanOrEqual(0)
  })
})
