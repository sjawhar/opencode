export type BillingMode = "subscription" | "metered" | "unknown"

export interface BillingSignals {
  poolBillingLane?: "subscription" | "metered"
  anthropicOverageInUse?: boolean
  codexPrimaryUsedPercent?: number
  codexSecondaryUsedPercent?: number
  codexCreditsBalance?: string
}

function normalizeHeaders(headers: Record<string, string>): Record<string, string> {
  const out: Record<string, string> = {}
  for (const [k, v] of Object.entries(headers)) out[k.toLowerCase()] = v
  return out
}

export function extractBillingSignals(headers: Record<string, string>): BillingSignals {
  const h = normalizeHeaders(headers)
  const result: BillingSignals = {}

  const overage = h["anthropic-ratelimit-unified-overage-in-use"]
  if (overage !== undefined) result.anthropicOverageInUse = overage === "true"
  else if (
    h["anthropic-ratelimit-unified-5h-utilization"] !== undefined ||
    h["anthropic-ratelimit-unified-7d-utilization"] !== undefined
  ) {
    result.anthropicOverageInUse = false
  }

  const primary = h["x-codex-primary-used-percent"]
  if (primary !== undefined) {
    const n = parseFloat(primary)
    if (Number.isFinite(n)) result.codexPrimaryUsedPercent = n
  }

  const secondary = h["x-codex-secondary-used-percent"]
  if (secondary !== undefined) {
    const n = parseFloat(secondary)
    if (Number.isFinite(n)) result.codexSecondaryUsedPercent = n
  }

  const balance = h["x-codex-credits-balance"]
  if (balance !== undefined) result.codexCreditsBalance = balance

  const lane = h["x-opencode-billing-lane"]
  if (lane === "metered" || lane === "subscription") result.poolBillingLane = lane

  return result
}

export function deriveBillingMode(
  providerID: string,
  modelID: string,
  signals: BillingSignals,
): BillingMode {
  // explicit plugin lane is authoritative; trumps provider header heuristics
  if (signals.poolBillingLane) return signals.poolBillingLane
  if (providerID === "anthropic") {
    if (signals.anthropicOverageInUse === true) return "metered"
    if (signals.anthropicOverageInUse === false) return "subscription"
    return "unknown"
  }
  if (providerID === "openai") {
    const p = signals.codexPrimaryUsedPercent
    const s = signals.codexSecondaryUsedPercent
    if (p === undefined && s === undefined) return "metered"
    if ((p ?? 0) >= 100 || (s ?? 0) >= 100) return "metered"
    return "subscription"
  }
  if (providerID === "google") {
    return modelID.startsWith("antigravity-") ? "subscription" : "metered"
  }
  if (providerID === "github-copilot") return "subscription"
  return "unknown"
}

export function createHeadersCaptureMiddleware(target: { headers?: Record<string, string> }) {
  return {
    specificationVersion: "v3" as const,
    async wrapStream({ doStream }: { doStream: () => Promise<any> }) {
      const result = await doStream()
      target.headers = result?.response?.headers ?? {}
      return result
    },
  }
}
