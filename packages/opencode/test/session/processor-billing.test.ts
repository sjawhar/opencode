import { describe, expect } from "bun:test"
import { Effect, Layer, Stream } from "effect"
import path from "path"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { ModelV2 } from "@opencode-ai/core/model"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { SessionProjector } from "@opencode-ai/core/session/projector"
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

const modelID = ModelV2.ID.make("test-model")

const config = () => ({
  enabled_providers: ["anthropic", "openai"],
  provider: {
    anthropic: providerConfig("Anthropic", "anthropic"),
    openai: providerConfig("OpenAI", "openai"),
  },
})

function providerConfig(name: string, id: string) {
  return {
    name,
    id,
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
      baseURL: "http://localhost:1/v1",
    },
  }
}

function agent(): Agent.Info {
  return {
    name: "build",
    mode: "primary",
    options: {},
    permission: [{ permission: "*", pattern: "*", action: "allow" }],
  }
}

const summary = Layer.succeed(
  SessionSummary.Service,
  SessionSummary.Service.of({
    summarize: () => Effect.void,
    diff: () => Effect.succeed([]),
    computeDiff: () => Effect.succeed([]),
  }),
)

const fakeLLMState: { headers?: Record<string, string> } = {}
const fakeLLM = Layer.succeed(
  LLM.Service,
  LLM.Service.of({
    stream(input) {
      const capture = (input as LLM.StreamInput & { capture?: Capture }).capture
      if (!capture) throw new Error("missing capture")
      capture.headers = fakeLLMState.headers
      return Stream.fromIterable([
        { type: "step-start", index: 0 },
        { type: "step-finish", reason: "stop", usage: new Usage({}) },
        { type: "finish" },
      ] as LLMEvent[])
    },
  }),
)

const spawnerLayer = LayerNode.compile(CrossSpawnSpawner.node)
// SessionProjector materializes published session/message events into rows;
// without it session.messages() reads an empty store and every assertion here
// fails with "Session not found".
const processorEnv = LayerNode.compile(
  LayerNode.group([SessionProcessor.node, Provider.node, Session.node, SessionProjector.node]),
  [
    [SessionSummary.node, summary],
    [RuntimeFlags.node, RuntimeFlags.layer({ experimentalEventSystem: false })],
    [LLM.node, fakeLLM],
  ],
)
const processorIt = testEffect(processorEnv)

const makeUser = Effect.fn("processorBillingTest.user")(function* (
  sessionID: SessionID,
  providerID: ProviderV2.ID,
) {
  const session = yield* Session.Service
  const msg = yield* session.updateMessage({
    id: MessageID.ascending(),
    role: "user",
    sessionID,
    agent: "build",
    model: { providerID, modelID },
    time: { created: Date.now() },
  })
  yield* session.updatePart({
    id: PartID.ascending(),
    messageID: msg.id,
    sessionID,
    type: "text",
    text: "hi",
  })
  return msg
})

const makeAssistant = Effect.fn("processorBillingTest.assistant")(function* (
  sessionID: SessionID,
  parentID: MessageID,
  providerID: ProviderV2.ID,
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
    modelID,
    providerID,
    parentID,
    time: { created: Date.now() },
    finish: "end_turn",
  }
  yield* session.updateMessage(msg)
  return msg
})

type BillingExpectation = {
  readonly providerID: ProviderV2.ID
  readonly headers?: Record<string, string>
  readonly billingMode: SessionV1.Assistant["billingMode"]
  readonly poolBillingLane?: "subscription" | "metered"
  readonly anthropicOverageInUse?: boolean
  readonly codexPrimaryUsedPercent?: number
}

const cases: ReadonlyArray<[string, BillingExpectation]> = [
  [
    "persists metered billing for anthropic overage header",
    {
      providerID: ProviderV2.ID.make("anthropic"),
      headers: { "anthropic-ratelimit-unified-overage-in-use": "true" },
      billingMode: "metered",
      anthropicOverageInUse: true,
    },
  ],
  [
    "persists subscription billing for anthropic non-overage header",
    {
      providerID: ProviderV2.ID.make("anthropic"),
      headers: { "anthropic-ratelimit-unified-overage-in-use": "false" },
      billingMode: "subscription",
      anthropicOverageInUse: false,
    },
  ],
  [
    "persists metered billing for openai codex exhausted header",
    {
      providerID: ProviderV2.ID.make("openai"),
      headers: { "x-codex-primary-used-percent": "100" },
      billingMode: "metered",
      codexPrimaryUsedPercent: 100,
    },
  ],
  [
    "persists metered billing for explicit pool lane override (anthropic)",
    {
      providerID: ProviderV2.ID.make("anthropic"),
      headers: { "x-opencode-billing-lane": "metered" },
      billingMode: "metered",
      poolBillingLane: "metered",
    },
  ],
  [
    "persists unknown billing for anthropic without billing headers",
    {
      providerID: ProviderV2.ID.make("anthropic"),
      billingMode: "unknown",
    },
  ],
]

describe("session processor billing persistence", () => {
  for (const [name, expectation] of cases) {
    processorIt.live(
      name,
      () =>
        provideTmpdirInstance((directory) =>
          Effect.gen(function* () {
          fakeLLMState.headers = expectation.headers
          const processors = yield* SessionProcessor.Service
          const session = yield* Session.Service
          const provider = yield* Provider.Service
          const chat = yield* session.create({})
          const parent = yield* makeUser(chat.id, expectation.providerID)
          const assistant = yield* makeAssistant(chat.id, parent.id, expectation.providerID, path.resolve(directory))
          const model = yield* provider.getModel(expectation.providerID, modelID)
          const handle = yield* processors.create({ assistantMessage: assistant, sessionID: chat.id, model })

          const result = yield* handle.process({
            user: {
              id: parent.id,
              sessionID: chat.id,
              role: "user",
              time: parent.time,
              agent: parent.agent,
              model: { providerID: expectation.providerID, modelID },
            } satisfies SessionV1.User,
            sessionID: chat.id,
            model,
            agent: agent(),
            system: [],
            messages: [{ role: "user", content: "hi" }],
            tools: {},
          })

          expect(result).toBe("continue")
          const messages = yield* session.messages({ sessionID: chat.id })
          const persistedMessage = messages.find((message) => message.info.id === assistant.id)
          const persisted = persistedMessage?.info
          if (!persisted || persisted.role !== "assistant") throw new Error("assistant message was not persisted")
          const stepFinish = persistedMessage.parts.find((part) => part.type === "step-finish")
          if (!stepFinish) throw new Error("step-finish part was not persisted")

          expect(persisted.billingMode).toBe(expectation.billingMode)
          expect(persisted.billingSignals?.poolBillingLane).toBe(expectation.poolBillingLane)
          expect(persisted.billingSignals?.anthropicOverageInUse).toBe(expectation.anthropicOverageInUse)
          expect(persisted.billingSignals?.codexPrimaryUsedPercent).toBe(expectation.codexPrimaryUsedPercent)
          expect(stepFinish.billingMode).toBe(expectation.billingMode)
          expect(stepFinish.billingSignals?.poolBillingLane).toBe(expectation.poolBillingLane)
          expect(stepFinish.billingSignals?.anthropicOverageInUse).toBe(expectation.anthropicOverageInUse)
          expect(stepFinish.billingSignals?.codexPrimaryUsedPercent).toBe(expectation.codexPrimaryUsedPercent)
          }),
        { config }).pipe(Effect.provide(spawnerLayer)),
    )
  }
})
