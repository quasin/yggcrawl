# AGENTS.md

YggCrawl is a Yggdrasil NodeInfo crawler written entirely in Zig 0.17.0. One
binary walks the Yggdrasil graph through the local Admin API (`getPeers`,
`debug_remotegetpeers`, `getNodeInfo`), probes a TCP port on every discovered
address, keeps state in SQLite and exports `yggcrawl.json`. The only C surface is
the system `libsqlite3`, translated from `src/c.h` in `build.zig`.

## Layout

| Path | Purpose |
|------|---------|
| `src/main.zig` | The whole program (~1040 lines): flag parsing, SQLite, Admin API client, graph walk, port probe, JSON export |
| `src/c.h` | Single line, `#include <sqlite3.h>`; only exists to feed translate-c |
| `build.zig` | Target defaults, translate-c module, `linkSystemLibrary("sqlite3")`, `run` step |
| `build.zig.zon` | `minimum_zig_version = "0.17.0"`, no dependencies |
| `README.md` | Architecture rationale (timeouts, resolver, concurrency); read it before touching networking |
| `.zig-cache/`, `zig-out/` | Generated build output, gitignored |
| `yggcrawl.db*`, `yggcrawl.json`, `*.json.tmp` | Runtime artifacts written to the CWD, gitignored |

## Dev environment

- Zig **0.17.0** exactly (`zig version`); the toolchain's `lib/std` is the only
  authority for std APIs.
- System SQLite headers: Arch `sqlite`, Debian/Ubuntu `libsqlite3-dev`.
- A real crawl needs a local yggdrasil node with `AdminListen:
  "tcp://127.0.0.1:9001"` in `yggdrasil.conf`.

## Build, run, check

```sh
zig build                                   # debug binary at zig-out/bin/yggcrawl
zig build -Doptimize=ReleaseFast            # release build
zig build run -- --help                     # run step forwards args after --
./zig-out/bin/yggcrawl --help               # banner, defaults, exit 0
zig fmt --check build.zig src/main.zig      # currently clean; `zig fmt` to fix
```

Full crawl (network-bound, writes `yggcrawl.db` + `yggcrawl.json` into the CWD):

```sh
./zig-out/bin/yggcrawl -workers 32 -port 80 -admin 127.0.0.1:9001 -db /tmp/probe.db
```

There is **no test step and no tests**. `build.zig` defines only `run`, so
`zig build test` does nothing and exits 0 — never read that as a passing suite.
Validate a change with `zig build`, `zig fmt --check`, `--help`, then a run
against a live node using a throwaway `-db`.

## Conventions

- One file, sectioned with `// ===` banners and `// ---- phase:` markers. Keep new
  functions inside the matching section.
- Naming as in the code: functions `camelCase`, file-scope constants
  `SCREAMING_SNAKE`, context structs `PascalCase` (`RemoteCtx`, `ScanCtx`, `Task`).
- Errors: functions return Zig error sets; `main` catches per phase, prints
  `"<phase> failed: {s}"` with `@errorName(err)` and continues or returns. Long
  phases degrade instead of aborting — no stack traces during a crawl.
- User output goes through `println`, which writes to the global `g_out` buffered
  writer, flushed at the end of `main`. Don't add `std.log` chatter for progress.
- Flags: add the short and long spelling through `nextArg`/`flagMatch` (both
  `-flag value` and `-flag=value`), add a line to `printUsage`, and keep the help
  path before any DB/open/network side effect.
- SQLite is touched from the main thread only, in `BEGIN`/`COMMIT` batches;
  workers write into pre-allocated result slots and never call sqlite3.
- Schema lives in `initDB` as `CREATE TABLE IF NOT EXISTS` (no migration system):
  schema edits must stay idempotent for existing database files.
- Comments explain *why* (timeout panics, atomic writes, borrowed column text),
  not what. Match that density on non-obvious code.
- JSON export field order is fixed: `address, key, nodeinfo, port_status,
  port_checked, name`.
- Commits are imperative, capitalized, unprefixed ("Add --help that prints usage
  and exits before any side effect"). LF endings are enforced by `.gitattributes`.

## Pitfalls

- Keep `default_target .cpu_model = .baseline`. Zig defaults to `native`, which
  bakes in AVX-512 and dies with `Illegal instruction` on older machines;
  `-Dcpu=native` is opt-in only.
- `@cImport` does not exist in 0.17. C symbols come from the translate-c module
  built out of `src/c.h` and imported as `@import("c")`.
- libc must stay linked (`link_libc` on the translate-c step): libsqlite3's first
  `malloc` crashes without it, even though the binary would still link.
- `std.Io.net`'s `IpAddress.connect` `@panic`s whenever a timeout is passed in
  0.17, so `connectWithTimeout` (non-blocking connect + poll, then back to
  blocking) is deliberate — don't replace it with `connect`.
- The crawl loop runs until the known-node count stops growing, with 2 s sleeps;
  it can stall for minutes on a slow admin API and won't exit early. Use `--help`
  for quick smoke checks and verify real work by the artifacts on disk.
- Output files are written atomically via `createFileAtomic`, leaving a temporary
  `*.json.tmp` sibling; both are gitignored — never commit them.
