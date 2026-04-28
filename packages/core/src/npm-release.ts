/// <reference lib="dom" />
// dom lib required for Response global typing (text/json/ok/etc.) — bun's runtime
// implements all of these but @types/bun's type fallback strips them when DOM lib
// isn't loaded. This file uses fetch + Response directly so we opt in here.

// GitHub release asset pre-resolver.
//
// Arborist's reify() can fetch HTTPS tarballs but uses plain HTTP — no GitHub
// auth — so private release assets fail with 404. This module pre-downloads
// the asset using GITHUB_TOKEN / GH_TOKEN / `gh auth token`, stashes it at a
// stable cache path, and returns a local file path that callers transform into
// a `file:` spec for Arborist.

import { mkdir, rename, rm } from "fs/promises"
import path from "path"
import { createWriteStream } from "fs"
import { pipeline } from "stream/promises"
import { Readable } from "stream"

const RELEASE_RE = /^https:\/\/github\.com\/([^/]+)\/([^/]+)\/releases\/download\/([^/]+)\/([^?#]+)$/
const DEFAULT_API_BASE = "https://api.github.com"

export interface ReleaseSpec {
  owner: string
  repo: string
  tag: string
  asset: string
}

export function classifyReleaseUrl(spec: string): ReleaseSpec | undefined {
  const match = RELEASE_RE.exec(spec)
  if (!match) return undefined
  const [, owner, repo, tag, asset] = match
  return { owner, repo, tag, asset }
}

function apiBase(): string {
  return process.env.TEST_GITHUB_API_BASE ?? DEFAULT_API_BASE
}

function authHeaders(token: string | undefined, extra: Record<string, string>): Record<string, string> {
  const out: Record<string, string> = { ...extra }
  if (token) out.authorization = `Bearer ${token}`
  return out
}

async function ghToken(): Promise<string | undefined> {
  if (typeof Bun === "undefined") return undefined
  try {
    const proc = Bun.spawn(["gh", "auth", "token"], { stderr: "pipe", stdout: "pipe" })
    const code = await proc.exited
    if (code !== 0) return undefined
    const out = (await new Response(proc.stdout).text()).trim()
    return out || undefined
  } catch {
    return undefined
  }
}

async function resolveToken(): Promise<string | undefined> {
  const env = process.env.GITHUB_TOKEN ?? process.env.GH_TOKEN
  if (env) return env
  return ghToken()
}

async function exists(p: string): Promise<boolean> {
  try {
    await Bun.file(p).stat()
    return true
  } catch {
    return false
  }
}

function cacheDirName(spec: ReleaseSpec): string {
  return `github-release-${spec.owner}-${spec.repo}-${spec.tag}-${spec.asset}`.replace(/[^a-zA-Z0-9_.-]/g, "_")
}

export async function preResolveReleaseAsset(
  spec: ReleaseSpec,
  opts: { cacheRoot: string; token?: string | undefined },
): Promise<string> {
  const pkgDir = path.join(opts.cacheRoot, "packages", cacheDirName(spec))
  const finalPath = path.join(pkgDir, "asset.tgz")
  if (await exists(finalPath)) return finalPath

  await mkdir(pkgDir, { recursive: true })
  const token = "token" in opts ? opts.token : await resolveToken()

  const metaUrl = `${apiBase()}/repos/${spec.owner}/${spec.repo}/releases/tags/${spec.tag}`
  const metaRes = await fetch(metaUrl, {
    headers: authHeaders(token, { accept: "application/vnd.github+json" }),
    redirect: "follow",
  })
  if (!metaRes.ok) {
    throw new Error(
      `release asset metadata fetch failed (${metaRes.status} on ${metaUrl}); set GITHUB_TOKEN/GH_TOKEN or run 'gh auth login'`,
    )
  }
  const meta = (await metaRes.json()) as { assets: Array<{ id: number; name: string }> }
  const asset = meta.assets.find((a) => a.name === spec.asset)
  if (!asset) {
    throw new Error(
      `release asset missing: ${spec.asset} not in release ${spec.owner}/${spec.repo}@${spec.tag}`,
    )
  }

  const assetUrl = `${apiBase()}/repos/${spec.owner}/${spec.repo}/releases/assets/${asset.id}`
  const dlRes = await fetch(assetUrl, {
    headers: authHeaders(token, {
      accept: "application/octet-stream",
      "x-github-api-version": "2022-11-28",
    }),
    redirect: "follow",
  })
  if (!dlRes.ok || !dlRes.body) {
    if (dlRes.status === 401 || dlRes.status === 403 || dlRes.status === 404) {
      throw new Error(
        `release asset download failed (${dlRes.status} on ${assetUrl}); GitHub auth missing or insufficient`,
      )
    }
    throw new Error(`release asset download failed: ${dlRes.status} on ${assetUrl}`)
  }

  const tmpPath = `${finalPath}.${Math.random().toString(36).slice(2)}.tmp`
  try {
    await pipeline(Readable.fromWeb(dlRes.body as never), createWriteStream(tmpPath))
    await rename(tmpPath, finalPath)
  } catch (err) {
    await rm(tmpPath, { force: true }).catch(() => {})
    throw err
  }
  return finalPath
}
