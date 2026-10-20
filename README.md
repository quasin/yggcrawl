# YggCrawl

YggCrawl — Yggdrasil NodeInfo crawler

Add to yggdrasil.conf: 'AdminListen: "tcp://127.0.0.1:9001"'

Features:

1. Collect the local peers of the current node through the Yggdrasil Admin API.
2. Walk the network graph in rounds (`debug_remotegetpeers` + `getNodeInfo`)
   until the number of known nodes stabilizes.
3. Probe the given TCP port on every known address.
4. Export the results to `yggcrawl.json`.
5. Keep state in SQLite (WAL, `busy_timeout=5000`, `synchronous=NORMAL`).

## Requirements

- Zig **0.17.0**
- System `sqlite3` library with the `sqlite3.h` header
  (Debian/Ubuntu: `libsqlite3-dev`; Arch: `sqlite`)

## Build

```sh
zig build -Doptimize=ReleaseFast
# binary: zig-out/bin/yggcrawl
```

The default target is the portable x86_64 baseline, so the resulting binary runs
on any x86_64 machine. For a machine-local build with the full ISA of the build
host (AVX-512 on Zen 4, etc.) add `-Dcpu=native` — but that binary dies with
`Illegal instruction` on any CPU older than the build host.

## Usage

```sh
./zig-out/bin/yggcrawl \
    -workers 32 \
    -port 80 \
    -admin 127.0.0.1:9001 \
    -db ./yggcrawl.db
```

`./zig-out/bin/yggcrawl --help` prints the banner, the option list with the
current values and exits 0 — it does not open the database, does not touch the
Admin API and does not start a crawl.

Flags (both the `-flag value` and the `-flag=value` form are accepted):

| Flag           | Default          | Description                         |
|----------------|------------------|-------------------------------------|
| `-workers`     | `32`             | number of parallel workers           |
| `-port`        | `80`             | TCP port to probe                    |
| `-admin`       | `127.0.0.1:9001` | Yggdrasil Admin API TCP address      |
| `-db`          | `./yggcrawl.db`  | path to the SQLite file              |
| `-h`, `--help` | —                | print help and exit, without crawling |

Every flag also has a `--long` spelling (`--workers`, `--port`, `--admin`,
`--db`).

## Architecture

The program is written entirely in Zig; the only C surface is the SQLite
library, whose headers are turned into a Zig module by `b.addTranslateC`
(`src/c.h` contains nothing but `#include <sqlite3.h>`). libc is linked only
because `libsqlite3` needs the C runtime — no C function is called directly.

- Networking uses `std.Io.net` (`IpAddress`, `Stream`, `HostName`) with explicit
  deadlines on every operation. Zig 0.17's `IpAddress.connect` panics as soon as
  a timeout is requested, so the connect deadline is implemented in Zig on top of
  the syscall layer: a non-blocking `connect` + `poll` with the timeout, then the
  socket is put back into blocking mode for the I/O that follows.
- Name resolution is std's own resolver — literal IP addresses, `/etc/hosts`,
  then the DNS servers from `/etc/resolv.conf`. `getaddrinfo` is never used.
- Concurrency comes from the `Io` implementation: the program builds a
  `std.Io.Threaded` whose async limit *is* `-workers`, and the crawl phases add
  their network tasks to an `Io.Group`. No manual thread pool, no mutexes.
- JSON uses `std.json` (`Value`/`ObjectMap`); file output uses `std.Io.Dir` with
  an atomic create-and-replace, so `yggcrawl.json` never appears half-written.
- The database is written from the main thread only, in batched transactions.

## Notes

The same key can be updated both through `address` and through the `peers` map —
in that case the `port_status` records overlap. JSON field order:
`address, key, nodeinfo, port_status, port_checked, name`.

## License

Apache-2.0.
