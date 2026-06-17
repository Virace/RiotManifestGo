# Riot Manifest Fixture Metadata

This directory is for small fixture metadata only. Do not commit Riot
`.manifest` binary files here.

Default upstream index source:

- Repository: `Morilli/riot-manifests`
- URL: `https://github.com/Morilli/riot-manifests`
- Default scope: `LoL/OC1`

Downloaded `.manifest` files should be recreated locally from metadata and kept
under the ignored cache:

```text
pkg/rman/testdata/.cache/riot-manifests/
```

Initialize or refresh the local cache with:

```powershell
.\scripts\init-fixtures.ps1
```

Run fixture-backed parser tests with:

```powershell
go test -tags=fixtures ./pkg/rman -count=1
```

## Fixture Slots

For one parser fixture kind, keep at most three metadata cases:

1. Fixed old version
2. Fixed middle version
3. Latest version, refreshed only during release/pre-release checks

The fixed old and fixed middle versions should not change after selection unless
there is a concrete parser coverage reason. The latest slot is intentionally not
chased during normal development.

## Version Sorting

Do not use lexicographic filename sorting to determine the latest upstream
manifest. Sort version segments numerically.

Example:

- `16.12.787.5550` is newer than `16.9.x`
- `16.12.7869679` is newer than `16.9.7728292`

Current upstream examples verified from `LoL/OC1`:

- `windows/league-client/16.12.787.5550.txt`
- `windows/lol-game-client/16.12.7869679.txt`

## Candidate Source Links

These are source links, not committed binary fixtures:

| Slot | Platform | Product | Upstream index path | CDN manifest ID |
| --- | --- | --- | --- | --- |
| fixed-old | windows | lol-game-client | `LoL/OC1/windows/lol-game-client/10.9.3185929.txt` | `037EC59D5BD7C5D3` |
| fixed-middle | windows | lol-game-client | `LoL/OC1/windows/lol-game-client/13.12.5142556.txt` | `FB988FB8D46FDD43` |
| latest-release-check | resolved numerically | selected product | resolved from `Morilli/riot-manifests/LoL/OC1` | resolved during fixture init |

If a macOS fixture is required to cover platform-specific parser behavior, it
must consume one of the fixture slots or be approved as a separate fixture kind.
