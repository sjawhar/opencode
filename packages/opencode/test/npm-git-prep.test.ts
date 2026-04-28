import fs from "fs/promises"
import path from "path"
import { describe, expect, test } from "bun:test"
import { $ } from "bun"
import { Npm } from "@opencode-ai/core/npm"
import { tmpdir } from "./fixture/fixture"

// Regression: pacote "prepares" git deps whose manifest declares
// prepare/prepack-family scripts by spawning an npm binary derived from our
// npmPath - a path that ships no npm-cli.js, so every such git spec failed
// with "git dep preparation failed". With the no-op prep override the dep
// installs as checked out, and its scripts never run (ignoreScripts).
describe("Npm.install with git deps", () => {
  test("installs a git dep with prepare/prepack scripts as checked out", async () => {
    await using tmp = await tmpdir()

    const repo = path.join(tmp.path, "dep-repo")
    await fs.mkdir(repo, { recursive: true })
    await Bun.write(
      path.join(repo, "package.json"),
      JSON.stringify({
        name: "test-git-prep",
        version: "1.0.0",
        main: "index.js",
        // Both scripts fail loudly if anything ever executes them.
        scripts: { prepack: "exit 1", prepare: "exit 1" },
      }),
    )
    await Bun.write(path.join(repo, "index.js"), "module.exports = 'raw-checkout'\n")
    await $`git -C ${repo} init -q`.quiet()
    await $`git -C ${repo} add .`.quiet()
    await $`git -C ${repo} -c user.email=test@test.invalid -c user.name=test -c commit.gpgsign=false commit -qm init`.quiet()

    const project = path.join(tmp.path, "project")
    await fs.mkdir(project)
    await Bun.write(
      path.join(project, "package.json"),
      JSON.stringify({
        name: "fixture",
        version: "1.0.0",
        dependencies: { "test-git-prep": `git+file://${repo}` },
      }),
    )

    await Npm.install(project)

    const installed = path.join(project, "node_modules", "test-git-prep")
    expect(await fs.readFile(path.join(installed, "index.js"), "utf8")).toContain("raw-checkout")
  })
})
