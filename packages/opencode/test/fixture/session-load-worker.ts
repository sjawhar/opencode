import { Effect } from "effect"
import { eq } from "drizzle-orm"
import path from "path"
import { Database } from "@opencode-ai/core/database/database"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { Project } from "@opencode-ai/core/project"
import { ProjectTable } from "@opencode-ai/core/project/sql"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { SessionSchema } from "@opencode-ai/core/session/schema"
import { MessageTable, PartTable, SessionTable } from "@opencode-ai/core/session/sql"
import { MessageID, PartID, SessionV1 } from "@opencode-ai/core/v1/session"
import { ModelV2 } from "@opencode-ai/core/model"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { InstallationVersion } from "@opencode-ai/core/installation/version"

type Msg = {
  mode: "sharded" | "global"
  idx: number
  dir: string
  file: string
  msgs: number
  updates: number
}

type Out = {
  idx: number
  mode: "sharded" | "global"
  session: SessionSchema.ID
  started: number
  ended: number
  global: number[]
  shard: number[]
  errors: string[]
  counts: {
    created: number
    updated: number
    messages: number
    parts: number
  }
}

const input = JSON.parse(process.argv[2] ?? "{}") as Msg
const out: Out = {
  idx: input.idx,
  mode: input.mode,
  session: SessionSchema.ID.make(`ses_load_${input.idx}_${Date.now()}`),
  started: Date.now(),
  ended: 0,
  global: [],
  shard: [],
  errors: [],
  counts: {
    created: 0,
    updated: 0,
    messages: 0,
    parts: 0,
  },
}

function fail(err: unknown) {
  if (err instanceof Error) return `${err.name}: ${err.message}`
  return String(err)
}

const program = Effect.gen(function* () {
  const database = yield* Database.Service
  const projectID = Project.ID.make(`project-load-${input.idx}`)
  const directory = AbsolutePath.make(path.join(input.dir, `session-${input.idx}`))
  const now = Date.now()

  const atCreate = performance.now()
  yield* database.transaction((tx) =>
    Effect.gen(function* () {
      yield* tx
        .insert(ProjectTable)
        .values({ id: projectID, worktree: directory, time_created: now, time_updated: now, sandboxes: [] })
        .onConflictDoNothing()
        .run()
      yield* tx
        .insert(SessionTable)
        .values({
          id: out.session,
          project_id: projectID,
          slug: `load-${input.idx}`,
          directory,
          title: `load-${input.idx}`,
          version: InstallationVersion,
        })
        .onConflictDoNothing()
        .run()
    }),
    { behavior: "immediate" },
  )
  out.global.push(Number((performance.now() - atCreate).toFixed(3)))
  out.counts.created++

  if (input.mode === "sharded") {
    const shard = yield* database.session(out.session)
    for (let i = 0; i < input.msgs; i++) {
      const messageID = MessageID.ascending()
      const partID = PartID.ascending()
      const created = Date.now()
      const messageData = {
        role: "user",
        time: { created },
        agent: `worker-${input.idx}`,
        model: { providerID: ProviderV2.ID.make("test"), modelID: ModelV2.ID.make("test") },
        tools: {},
      } satisfies Omit<SessionV1.User, "id" | "sessionID">
      const partData = { type: "text", text: `load-${input.idx}-${i}` } satisfies Omit<
        SessionV1.TextPart,
        "id" | "sessionID" | "messageID"
      >
      const at = performance.now()
      yield* shard.transaction((tx) =>
        Effect.gen(function* () {
          yield* tx
            .insert(MessageTable)
            .values({ id: messageID, session_id: out.session, time_created: created, data: messageData })
            .run()
          yield* tx
            .insert(PartTable)
            .values({ id: partID, message_id: messageID, session_id: out.session, time_created: created, data: partData })
            .run()
        }),
      )
      out.shard.push(Number((performance.now() - at).toFixed(3)))
      out.counts.messages++
      out.counts.parts++
    }
  }

  for (let i = 0; i < input.updates; i++) {
    const at = performance.now()
    yield* database.transaction(
      (tx) =>
        tx
          .update(SessionTable)
          .set({ title: `load-${input.idx}-${i}`, time_updated: Date.now() })
          .where(eq(SessionTable.id, out.session))
          .run(),
      { behavior: "immediate" },
    )
    out.global.push(Number((performance.now() - at).toFixed(3)))
    out.counts.updated++
  }
})

try {
  await Effect.runPromise(program.pipe(Effect.provide(AppNodeBuilder.build(Database.node)), Effect.scoped))
} catch (err) {
  out.errors.push(fail(err))
} finally {
  out.ended = Date.now()
}

await Bun.write(Bun.stdout, JSON.stringify(out))
process.exit(out.errors.length ? 1 : 0)
