// Git ::path: subdir pre-resolver.
//
// Arborist's reify() doesn't reliably handle git specs with subdir selectors
// (e.g. github:owner/repo#main::path:packages/foo). We pre-pack the subdir
// into a tarball using pacote, then return a local file path that callers
// transform into a `file:` spec for Arborist.

import path from "path"
import { mkdir } from "fs/promises"
import pacote from "pacote"

type Flat = Record<string, unknown>

const SUBDIR_RE = /::path:/

export function isGitSubdirSpec(spec: string): boolean {
  return SUBDIR_RE.test(spec)
}

async function exists(p: string): Promise<boolean> {
  try {
    await Bun.file(p).stat()
    return true
  } catch {
    return false
  }
}

function sanitize(spec: string): string {
  return `git-subdir-${spec}`.replace(/[^a-zA-Z0-9_.-]/g, "_")
}

export async function preResolveGitSubdir(
  spec: string,
  opts: { cacheRoot: string; npmConfig: Flat },
): Promise<string> {
  const dir = path.join(opts.cacheRoot, "packages", sanitize(spec))
  const file = path.join(dir, "git-subdir.tgz")
  if (await exists(file)) return file

  await mkdir(dir, { recursive: true })

  // pacote.tarball.file extracts the spec, runs prepare lifecycle if needed,
  // and writes the resulting tarball to `file`.
  await pacote.tarball.file(spec, file, { ...opts.npmConfig })
  return file
}
