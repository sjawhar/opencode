declare global {
  const OPENCODE_VERSION: string
  const OPENCODE_CHANNEL: string
  const OPENCODE_PLUGIN_VERSION: string
}

export const InstallationVersion = typeof OPENCODE_VERSION === "string" ? OPENCODE_VERSION : "local"
export const InstallationChannel = typeof OPENCODE_CHANNEL === "string" ? OPENCODE_CHANNEL : "local"
export const InstallationLocal = InstallationChannel === "local"
// Version of the @opencode-ai/plugin package to install for project plugin auto-install.
// Defaults to InstallationVersion so mainline builds keep their existing exact-version pin.
// Forks override this at build time (via OPENCODE_PLUGIN_VERSION) so that, even though their
// binary version (e.g. 1.14.26-sami.<timestamp>) is not published on npm, they pin to the
// matching mainline plugin version (e.g. 1.14.26) which is.
export const InstallationPluginVersion =
  typeof OPENCODE_PLUGIN_VERSION === "string" ? OPENCODE_PLUGIN_VERSION : InstallationVersion
