import { afterAll, beforeAll, beforeEach, describe, expect } from "bun:test"
import { Effect, Layer, Stream } from "effect"
import path from "path"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { ModelV2 } from "@opencode-ai/core/model"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { Usage, type LLMEvent } from "@opencode-ai/llm"
import type { Agent } from "../../src/agent/agent"
import { Provider } from "@/provider/provider"
import { LLM } from "../../src/session/llm"
import { SessionProcessor } from "../../src/session/processor"
import { MessageID, PartID, SessionID } from "../../src/session/schema"
import { Session } from "@/session/session"
import { SessionSummary } from "../../src/session/summary"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { provideTmpdirInstance } from "../fixture/fixture"
import { testEffect } from "../lib/effect"

type Capture = { headers?: Record<string, string> }

const ref = {
  providerID: ProviderV2.ID.make("test"),
  modelID: ModelV2.ID.make("test-model"),
}

const config = (baseURL: string) => ({
  enabled_providers: ["test"],
  provider: {
    test: {
      name: "Test",
      id: "test",
      env: [],
      npm: "@ai-sdk/openai-compatible",
      models: {
        "test-model": {
          id: "test-model",
          name: "Test Model",
          attachment: false,
          reasoning: false,
          temperature: false,
          tool_call: false,
          release_date: "2025-01-01",
          limit: { context: 100000, output: 10000 },
          cost: { input: 0, output: 0 },
          options: {},
        },
      },
      options: {
        apiKey: "test-key",
        baseURL,
      },
    },
  },
})

function agent(): Agent.Info {
  return {
    name: "build",
    mode: "primary",
    options: {},
    permission: [{ permission: "*", pattern: "*", action: "allow" }],
  }
}

function chatStream(text: string) {
  const payload =
    [
      `data: ${JSON.stringify({
        id: "chatcmpl-capture",
        object: "chat.completion.chunk",
        choices: [{ delta: { role: "assistant" } }],
      })}`,
      `data: ${JSON.stringify({
        id: "chatcmpl-capture",
        object: "chat.completion.chunk",
        choices: [{ delta: { content: text } }],
      })}`,
      `data: ${JSON.stringify({
        id: "chatcmpl-capture",
        object: "chat.completion.chunk",
        choices: [{ delta: {}, finish_reason: "stop" }],
      })}`,
      "data: [DONE]",
    ].join("\n\n") + "\n\n"
  const encoder = new TextEncoder()
  return new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(encoder.encode(payload))
      controller.close()
    },
  })
}

let server: ReturnType<typeof Bun.serve> | undefined
let requests = 0

beforeAll(() => {
  server = Bun.serve({
    port: 0,
    async fetch(req) {
      requests++
      await req.json().catch(() => undefined)
      return new Response(chatStream("hello"), {
        status: 200,
        headers: {
          "Content-Type": "text/event-stream",
          "x-capture-test": "present",
        },
      })
    },
  })
})

beforeEach(() => {
  requests = 0
})

afterAll(() => {
  void server?.stop()
})

const llmIt = testEffect(
  LayerNode.compile(LayerNode.group([LLM.node, Provider.node]), [
    [RuntimeFlags.node, RuntimeFlags.layer({ experimentalEventSystem: false })],
  ]),
)

describe("session capture plumbing", () => {
  llmIt.instance(
    "llm.stream writes provider response headers into the caller's capture object",
    () =>
      Effect.gen(function* () {
        if (!server) return yield* Effect.die("test server was not started")
        const provider = yield* Provider.Service
        const model = yield* provider.getModel(ref.providerID, ref.modelID)
        const sessionID = SessionID.make("session-capture-llm")
        const capture: Capture = {}
        const user = {
          id: MessageID.make("msg_user-capture-llm"),
          sessionID,
          role: "user",
          time: { created: Date.now() },
          agent: "build",
          model: { providerID: ref.providerID, modelID: ref.modelID },
        } satisfies SessionV1.User

        yield* LLM.Service.use((svc) =>
          svc
            .stream({
              user,
              sessionID,
              model,
              agent: agent(),
              system: [],
              messages: [{ role: "user", content: "hi" }],
              tools: {},
              capture,
            } as LLM.StreamInput & { capture: Capture })
            .pipe(Stream.runDrain),
        )

        expect(requests).toBe(1)
        expect(capture.headers?.["x-capture-test"]).toBe("present")
      }),
    { config: () => config(`${server!.url.origin}/v1`) },
  )
})

const summary = Layer.succeed(
  SessionSummary.Service,
  SessionSummary.Service.of({
    summarize: () => Effect.void,
    diff: () => Effect.succeed([]),
    computeDiff: () => Effect.succeed([]),
  }),
)

const fakeLLMState: { input?: LLM.StreamInput; capture?: Capture } = {}
const fakeLLM = Layer.succeed(
  LLM.Service,
  LLM.Service.of({
    stream(input) {
      fakeLLMState.input = input
      fakeLLMState.capture = (input as LLM.StreamInput & { capture?: Capture }).capture
      if (!fakeLLMState.capture) throw new Error("missing capture")
      fakeLLMState.capture.headers = { "x-processor-capture": "present" }
      return Stream.fromIterable([
        { type: "step-start", index: 0 },
        { type: "step-finish", reason: "stop", usage: new Usage({}) },
        { type: "finish" },
      ] as LLMEvent[])
    },
  }),
)

const spawnerLayer = LayerNode.compile(CrossSpawnSpawner.node)
const processorEnv = LayerNode.compile(LayerNode.group([SessionProcessor.node, Provider.node, Session.node]), [
  [SessionSummary.node, summary],
  [RuntimeFlags.node, RuntimeFlags.layer({ experimentalEventSystem: false })],
  [LLM.node, fakeLLM],
])
const processorIt = testEffect(processorEnv)

const makeUser = Effect.fn("captureTest.user")(function* (sessionID: SessionID, text: string) {
  const session = yield* Session.Service
  const msg = yield* session.updateMessage({
    id: MessageID.ascending(),
    role: "user",
    sessionID,
    agent: "build",
    model: ref,
    time: { created: Date.now() },
  })
  yield* session.updatePart({
    id: PartID.ascending(),
    messageID: msg.id,
    sessionID,
    type: "text",
    text,
  })
  return msg
})

const makeAssistant = Effect.fn("captureTest.assistant")(function* (
  sessionID: SessionID,
  parentID: MessageID,
  root: string,
) {
  const session = yield* Session.Service
  const msg: SessionV1.Assistant = {
    id: MessageID.ascending(),
    role: "assistant",
    sessionID,
    mode: "build",
    agent: "build",
    path: { cwd: root, root },
    cost: 0,
    tokens: { total: 0, input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
    modelID: ref.modelID,
    providerID: ref.providerID,
    parentID,
    time: { created: Date.now() },
    finish: "end_turn",
  }
  yield* session.updateMessage(msg)
  return msg
})

describe("session processor capture plumbing", () => {
  processorIt.live(
    "processor passes its capture object to llm.stream",
    () =>
      provideTmpdirInstance((directory) =>
        Effect.gen(function* () {
        fakeLLMState.input = undefined
        fakeLLMState.capture = undefined
        const processors = yield* SessionProcessor.Service
        const session = yield* Session.Service
        const provider = yield* Provider.Service
        const chat = yield* session.create({})
        const parent = yield* makeUser(chat.id, "hi")
        const msg = yield* makeAssistant(chat.id, parent.id, path.resolve(directory))
        const model = yield* provider.getModel(ref.providerID, ref.modelID)
        const handle = yield* processors.create({ assistantMessage: msg, sessionID: chat.id, model })
        const input = {
          user: {
            id: parent.id,
            sessionID: chat.id,
            role: "user",
            time: parent.time,
            agent: parent.agent,
            model: { providerID: ref.providerID, modelID: ref.modelID },
          } satisfies SessionV1.User,
          sessionID: chat.id,
          model,
          agent: agent(),
          system: [],
          messages: [{ role: "user", content: "hi" }],
          tools: {},
        } satisfies LLM.StreamInput

        const result = yield* handle.process(input)
        const streamInput = fakeLLMState.input as (LLM.StreamInput & { capture?: Capture }) | undefined

        expect(result).toBe("continue")
        if (!streamInput) throw new Error("fake LLM did not receive a stream input")
        if (!streamInput.capture) throw new Error("stream input did not carry capture")
        if (!fakeLLMState.capture) throw new Error("processor did not pass capture to llm.stream")
        const captured: Capture = fakeLLMState.capture
        expect(captured).toBe(streamInput.capture)
        expect(captured.headers).toEqual({ "x-processor-capture": "present" })
        }),
      { config: () => config("http://localhost:1/v1") }).pipe(Effect.provide(spawnerLayer)),
  )
})
