import { describe, expect, test } from "bun:test"
import { createHeadersCaptureMiddleware, extractBillingSignals } from "../../src/session/billing"
import { deriveBillingMode } from "../../src/session/billing"

describe("extractBillingSignals", () => {
  test("returns empty object for empty headers", () => {
    expect(extractBillingSignals({})).toEqual({})
  })

  test("parses anthropic overage-in-use=true", () => {
    expect(
      extractBillingSignals({ "anthropic-ratelimit-unified-overage-in-use": "true" }),
    ).toEqual({ anthropicOverageInUse: true })
  })

  test("parses anthropic overage-in-use=false", () => {
    expect(
      extractBillingSignals({ "anthropic-ratelimit-unified-overage-in-use": "false" }),
    ).toEqual({ anthropicOverageInUse: false })
  })

  test("treats 5h utilization present + overage absent as not-in-overage (subscription)", () => {
    expect(
      extractBillingSignals({ "anthropic-ratelimit-unified-5h-utilization": "0.42" }),
    ).toEqual({ anthropicOverageInUse: false })
  })

  test("treats 7d utilization present + overage absent as not-in-overage", () => {
    expect(
      extractBillingSignals({ "anthropic-ratelimit-unified-7d-utilization": "0.10" }),
    ).toEqual({ anthropicOverageInUse: false })
  })

  test("explicit overage header wins over utilization presence", () => {
    expect(
      extractBillingSignals({
        "anthropic-ratelimit-unified-5h-utilization": "0.42",
        "anthropic-ratelimit-unified-overage-in-use": "true",
      }),
    ).toEqual({ anthropicOverageInUse: true })
  })

  test("no anthropic headers at all → no anthropicOverageInUse signal", () => {
    expect(extractBillingSignals({ "x-irrelevant": "noise" })).toEqual({})
  })

  test("parses codex used-percent headers", () => {
    expect(
      extractBillingSignals({
        "x-codex-primary-used-percent": "42.5",
        "x-codex-secondary-used-percent": "15.0",
        "x-codex-credits-balance": "3336",
      }),
    ).toEqual({
      codexPrimaryUsedPercent: 42.5,
      codexSecondaryUsedPercent: 15.0,
      codexCreditsBalance: "3336",
    })
  })

  test("ignores unknown headers", () => {
    expect(extractBillingSignals({ "x-irrelevant": "noise" })).toEqual({})
  })

  test("is case-insensitive on header names", () => {
    expect(
      extractBillingSignals({ "ANTHROPIC-RATELIMIT-UNIFIED-OVERAGE-IN-USE": "true" }),
    ).toEqual({ anthropicOverageInUse: true })
  })

  test("skips invalid numbers", () => {
    expect(extractBillingSignals({ "x-codex-primary-used-percent": "not-a-number" })).toEqual({})
  })

  test("reads explicit pool billing lane header (metered)", () => {
    expect(extractBillingSignals({ "x-opencode-billing-lane": "metered" })).toEqual({ poolBillingLane: "metered" })
  })

  test("reads explicit pool billing lane header (subscription)", () => {
    expect(extractBillingSignals({ "x-opencode-billing-lane": "subscription" })).toEqual({
      poolBillingLane: "subscription",
    })
  })

  test("ignores invalid lane value", () => {
    expect(extractBillingSignals({ "x-opencode-billing-lane": "bogus" })).toEqual({})
  })

  test("lane header coexists with anthropic util headers", () => {
    const s = extractBillingSignals({
      "x-opencode-billing-lane": "metered",
      "anthropic-ratelimit-unified-5h-utilization": "0.4",
    })
    expect(s.poolBillingLane).toBe("metered")
    expect(s.anthropicOverageInUse).toBe(false)
  })
})

describe("deriveBillingMode", () => {
  test("anthropic with overageInUse=true → metered", () => {
    expect(deriveBillingMode("anthropic", "claude-opus-4-7", { anthropicOverageInUse: true })).toBe("metered")
  })

  test("anthropic with overageInUse=false → subscription", () => {
    expect(deriveBillingMode("anthropic", "claude-opus-4-7", { anthropicOverageInUse: false })).toBe("subscription")
  })

  test("anthropic with no overage signal → unknown", () => {
    expect(deriveBillingMode("anthropic", "claude-opus-4-7", {})).toBe("unknown")
  })

  test("anthropic utilization-only response → subscription (via extract+derive)", () => {
    const signals = extractBillingSignals({ "anthropic-ratelimit-unified-5h-utilization": "0.42" })
    expect(deriveBillingMode("anthropic", "claude-opus-4-7", signals)).toBe("subscription")
  })

  test("pool lane overrides provider logic (metered)", () => {
    expect(deriveBillingMode("anthropic", "claude-x", { poolBillingLane: "metered" })).toBe("metered")
  })

  test("pool lane overrides (subscription) even with overage header", () => {
    expect(
      deriveBillingMode("anthropic", "claude-x", { poolBillingLane: "subscription", anthropicOverageInUse: true }),
    ).toBe("subscription")
  })

  test("apikey-anthropic (lane=metered, no util headers) → metered not unknown", () => {
    const s = extractBillingSignals({ "x-opencode-billing-lane": "metered" })
    expect(deriveBillingMode("anthropic", "claude-x", s)).toBe("metered")
  })

  test("openai with codex primary < 100 and secondary < 100 → subscription", () => {
    expect(
      deriveBillingMode("openai", "gpt-5.5", { codexPrimaryUsedPercent: 50, codexSecondaryUsedPercent: 50 }),
    ).toBe("subscription")
  })

  test("openai with codex primary >= 100 → metered (over-count)", () => {
    expect(
      deriveBillingMode("openai", "gpt-5.5", { codexPrimaryUsedPercent: 100, codexSecondaryUsedPercent: 50 }),
    ).toBe("metered")
  })

  test("openai with codex secondary >= 100 → metered", () => {
    expect(
      deriveBillingMode("openai", "gpt-5.5", { codexPrimaryUsedPercent: 50, codexSecondaryUsedPercent: 100 }),
    ).toBe("metered")
  })

  test("openai with no codex headers → metered (assumes API key path)", () => {
    expect(deriveBillingMode("openai", "gpt-5.5", {})).toBe("metered")
  })

  test("google with antigravity-* model → subscription", () => {
    expect(deriveBillingMode("google", "antigravity-gemini-3-pro", {})).toBe("subscription")
  })

  test("google with regular gemini model → metered", () => {
    expect(deriveBillingMode("google", "gemini-3.1-pro-preview", {})).toBe("metered")
  })

  test("github-copilot → subscription", () => {
    expect(deriveBillingMode("github-copilot", "gpt-4o", {})).toBe("subscription")
  })

  test("unknown provider → unknown", () => {
    expect(deriveBillingMode("mystery-provider", "some-model", {})).toBe("unknown")
  })
})

describe("createHeadersCaptureMiddleware", () => {
  test("stores headers from doStream's response into target", async () => {
    const target: { headers?: Record<string, string> } = {}
    const middleware = createHeadersCaptureMiddleware(target)
    const fakeDoStream = async () => ({
      stream: {} as ReadableStream,
      response: { headers: { "anthropic-ratelimit-unified-overage-in-use": "true" } },
    })
    const result = await middleware.wrapStream!({
      doStream: fakeDoStream as any,
      params: {} as any,
      model: {} as any,
    } as any)
    expect(target.headers).toEqual({ "anthropic-ratelimit-unified-overage-in-use": "true" })
    expect(result.stream).toBeDefined()
  })

  test("stores empty object when response has no headers", async () => {
    const target: { headers?: Record<string, string> } = {}
    const middleware = createHeadersCaptureMiddleware(target)
    const fakeDoStream = async () => ({ stream: {} as ReadableStream, response: {} })
    await middleware.wrapStream!({
      doStream: fakeDoStream as any,
      params: {} as any,
      model: {} as any,
    } as any)
    expect(target.headers).toEqual({})
  })

  test("does not throw when response is missing entirely", async () => {
    const target: { headers?: Record<string, string> } = {}
    const middleware = createHeadersCaptureMiddleware(target)
    const fakeDoStream = async () => ({ stream: {} as ReadableStream })
    await middleware.wrapStream!({
      doStream: fakeDoStream as any,
      params: {} as any,
      model: {} as any,
    } as any)
    expect(target.headers).toEqual({})
  })
})
