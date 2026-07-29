# RiotManifestGo Project Rules

## Branch and Pull Request Policy

- `develop` is the integration base for ongoing feature development.
- When work needs an isolated branch, create it from the latest `develop`.
- Integrate branches only through GitHub pull requests; do not merge a branch
  locally and push the merge result to a shared branch.
- Merge `develop` into `main` only when publishing a new version, and perform
  that integration through a GitHub pull request.
- Normal development merges stop at `develop`; do not create a release or tag
  unless the user explicitly requests one.

## Test Fixture Policy

- Do not commit Riot `.manifest` binary files to the repository. They are large
  binary artifacts, and deleting them later would not remove them from git
  history.
- Store only small fixture metadata in git: upstream index path, CDN URL,
  version, manifest ID, byte size, SHA256, selection reason, and expected parse
  summary.
- Default upstream index source: `Morilli/riot-manifests`
  (`https://github.com/Morilli/riot-manifests`), scoped to `LoL/OC1` unless a
  test explicitly needs another region.
- Downloaded fixture binaries must live in one fixed ignored local cache, such as
  `pkg/rman/testdata/.cache/riot-manifests/`, and must be recreated by a script
  from the committed metadata.
- For each fixture kind, keep at most 3 committed metadata cases. A fixture kind
  is the tested data family and purpose, such as `LoL/OC1/lol-game-client
  .manifest parser fixtures`.
- The 3-case fixture shape is: one fixed old version, one fixed middle version,
  and one latest-version case refreshed only for release/pre-release checks.
  Normal development does not need to chase the newest upstream manifest.
- Old and middle fixture versions are stable once selected. Do not replace them
  unless there is a concrete parser-coverage reason and the user agrees.
- Latest-version discovery must sort versions numerically, not lexicographically.
  For example, `16.12.x` is newer than `16.9.x` even though plain text sorting
  can put `16.9` later.
- Do not add broad version matrices, every-patch samples, or duplicate platform
  fixtures by default.
- If Windows and macOS point to the same manifest ID, keep only one metadata case
  unless a test explicitly needs both platform contexts.
- Current CI scope should remain offline and deterministic. Do not add weekly
  latest-version smoke tests or CI jobs that fetch live Riot CDN data unless the
  user explicitly asks for that later.
