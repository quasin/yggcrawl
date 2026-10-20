const std = @import("std");
const builtin = @import("builtin");
const c = @import("c");

const Io = std.Io;
const Threaded = Io.Threaded;
const net = Io.net;
const File = Io.File;
const Dir = Io.Dir;
const Allocator = std.mem.Allocator;

// ---------------------------------------------------------------------------
// YggCrawl — Yggdrasil NodeInfo crawler on Zig 0.17.0, no C glue code.
//
// Architecture:
//   1) collect local peers through the Yggdrasil Admin API (std.Io.net + std.json)
//   2) walk the network graph in rounds: debug_remotegetpeers + getNodeInfo
//   3) probe the target TCP port on every known address
//   4) export the results to JSON
//
// Everything except the SQLite library itself is std: sockets and timeouts come
// from `std.Io.net`, name resolution is std's own resolver (/etc/hosts +
// /etc/resolv.conf), file output is `std.Io.Dir` + `std.Io.File`, and the
// concurrency comes from `Io.Group.async` bounded by the `-workers` limit.
// ---------------------------------------------------------------------------

const DEFAULT_WORKERS: usize = 32;
const DEFAULT_PORT: u16 = 80;
const DEFAULT_ADMIN = "127.0.0.1:9001";
const DEFAULT_DB = "./yggcrawl.db";
const EXPORT_FILE = "yggcrawl.json";

const ADMIN_TIMEOUT_MS: i64 = 3000;
const REMOTE_TIMEOUT_MS: i64 = 4000;
const PROBE_TIMEOUT_MS: i64 = 3000;

const SQLITE_OK = c.SQLITE_OK;
const SQLITE_ROW = c.SQLITE_ROW;

const SQLITE_TRANSIENT: c.sqlite3_destructor_type =
    @ptrFromInt(@as(usize, @bitCast(@as(isize, -1))));

var g_out: *std.Io.Writer = undefined;

fn println(comptime fmt: []const u8, args: anytype) void {
    g_out.print(fmt ++ "\n", args) catch {};
    g_out.flush() catch {};
}

/// The startup banner. Shared with `--help` so the two cannot disagree about
/// what the program is about to do.
fn printBanner(workers: usize, admin: []const u8, port: u16) void {
    println("YggCrawl - High-performance Yggdrasil crawler (SQLite)", .{});
    println("Workers: {d} | Admin API: {s} | Port to probe: {d}", .{ workers, admin, port });
    println("------------------------------------------------------------------", .{});
}

/// Printed by `-h`/`--help` and then `main` returns: no database is opened, no
/// network request is made, no crawl starts.
fn printUsage(workers: usize, port: u16, admin: []const u8, db_path: []const u8) void {
    printBanner(workers, admin, port);
    println("Usage: yggcrawl [options]", .{});
    println("", .{});
    println("Options (each also accepts the --long and -flag=value forms):", .{});
    println("  -workers <n>        concurrent network tasks / Io pool size (default {d})", .{DEFAULT_WORKERS});
    println("  -port <n>           TCP port to probe on every address (default {d})", .{DEFAULT_PORT});
    println("  -admin <host:port>  Yggdrasil Admin API address (default {s})", .{DEFAULT_ADMIN});
    println("  -db <path>          SQLite database file (default {s})", .{DEFAULT_DB});
    println("  -h, --help          print this help and exit, without crawling", .{});
    println("", .{});
    println("Current values: workers={d} port={d} admin={s} db={s}", .{ workers, port, admin, db_path });
    println("Results: {s} (SQLite) and {s} (JSON) in the working directory.", .{ DEFAULT_DB, EXPORT_FILE });
}

/// Declaration of how long an `Io` operation may take before giving up,
/// measured on the awake clock.
fn timeoutOf(ms: i64) Io.Timeout {
    return .{ .duration = .{ .raw = Io.Duration.fromMilliseconds(ms), .clock = .awake } };
}

// ===========================================================================
// main
// ===========================================================================

pub fn main(init: std.process.Init) !void {
    const alloc = init.gpa;

    const argv = try init.minimal.args.toSlice(init.arena.allocator());

    var workers: usize = DEFAULT_WORKERS;
    var port: u16 = DEFAULT_PORT;
    var admin: []const u8 = DEFAULT_ADMIN;
    var db_path: []const u8 = DEFAULT_DB;
    var help = false;

    var i: usize = 1;
    while (i < argv.len) : (i += 1) {
        if (nextArg(argv, &i, argv[i], "-workers", "--workers")) |v| {
            workers = std.fmt.parseInt(usize, v, 10) catch workers;
        } else if (nextArg(argv, &i, argv[i], "-port", "--port")) |v| {
            port = std.fmt.parseInt(u16, v, 10) catch port;
        } else if (nextArg(argv, &i, argv[i], "-admin", "--admin")) |v| {
            admin = v;
        } else if (nextArg(argv, &i, argv[i], "-db", "--db")) |v| {
            db_path = v;
        } else if (flagMatch(argv[i], "-h", "--help") != null) {
            help = true;
        }
    }
    if (workers == 0) workers = 1;

    // The `-workers` flag *is* the size of the Io thread pool: async tasks are
    // dispatched to at most this many pool threads, the rest run eagerly on the
    // calling thread.
    var threaded: Io.Threaded = .init(alloc, .{ .async_limit = .limited(workers) });
    defer threaded.deinit();
    const io = threaded.io();

    var stdout_buf: [1 << 16]u8 = undefined;
    var stdout_writer = File.stdout().writer(io, &stdout_buf);
    g_out = &stdout_writer.interface;

    // Answer the question before any side effect: no database file, no admin
    // API request, no crawl — just print and exit 0.
    if (help) {
        printUsage(workers, port, admin, db_path);
        return;
    }

    printBanner(workers, admin, port);

    const db = initDB(alloc, db_path) catch |err| {
        println("Database initialization failed: {s}", .{@errorName(err)});
        return;
    };
    defer _ = c.sqlite3_close(db);

    // 1. Local peers
    println("[1/4] Collecting local peers...", .{});
    fetchAndInsertLocalPeers(alloc, io, db, admin) catch |err| {
        println("Failed to fetch local peers: {s}", .{@errorName(err)});
        return;
    };

    // 2. Graph walk
    println("[2/4] Starting the network graph walk...", .{});
    var previous_count: i64 = -1;
    while (true) {
        fetchAndUpdateRemoteGetPeers(alloc, io, db, admin, workers) catch |err| {
            println("remote get peers failed: {s}", .{@errorName(err)});
        };
        fetchAndUpdateNodeInfo(alloc, io, db, admin, workers) catch |err| {
            println("nodeinfo update failed: {s}", .{@errorName(err)});
        };

        const current_count = countKeys(db) catch |err| {
            println("Counting keys failed: {s}", .{@errorName(err)});
            break;
        };
        println("Known nodes in the network: {d}", .{current_count});
        if (current_count == previous_count) {
            println("Graph fully explored (no new nodes). Crawling finished.", .{});
            break;
        }
        previous_count = current_count;
        io.sleep(.fromSeconds(2), .awake) catch {};
    }

    // 3. Port probe
    println("[3/4] Probing port {d}...", .{port});
    checkPortOnAllPeers(alloc, io, db, workers, port) catch |err| {
        println("Port scan failed: {s}", .{@errorName(err)});
    };

    // 4. Export
    println("[4/4] Exporting data to JSON...", .{});
    exportToJSON(alloc, io, db, EXPORT_FILE) catch |err| {
        println("Export failed: {s}", .{@errorName(err)});
    };

    println("Finished successfully.", .{});
    g_out.flush() catch {};
}

const FlagMatch = union(enum) {
    no,
    bare,
    val: []const u8,
};

fn flagMatch(arg: []const u8, comptime short: []const u8, comptime long: []const u8) ?FlagMatch {
    if (std.mem.eql(u8, arg, short) or std.mem.eql(u8, arg, long)) return .bare;
    if (arg.len > short.len + 1 and std.mem.startsWith(u8, arg, short) and arg[short.len] == '=') {
        return .{ .val = arg[short.len + 1 ..] };
    }
    if (arg.len > long.len + 1 and std.mem.startsWith(u8, arg, long) and arg[long.len] == '=') {
        return .{ .val = arg[long.len + 1 ..] };
    }
    return null;
}

/// Returns the value of `-flag value` / `-flag=value`, advancing `i` past a
/// separate value argument.
fn nextArg(
    argv: []const [:0]const u8,
    i: *usize,
    arg: []const u8,
    comptime short: []const u8,
    comptime long: []const u8,
) ?[]const u8 {
    const m = flagMatch(arg, short, long) orelse return null;
    switch (m) {
        .no => return null,
        .val => |v| return v,
        .bare => {
            if (i.* + 1 >= argv.len) return null;
            i.* += 1;
            return argv[i.*];
        },
    }
}

// ===========================================================================
// SQLite (the one remaining C library, through the translate-c module)
// ===========================================================================

fn initDB(alloc: Allocator, path: []const u8) !*c.sqlite3 {
    const path_z = try alloc.dupeSentinel(u8, path, 0);
    defer alloc.free(path_z);

    var db: ?*c.sqlite3 = null;
    const flags = c.SQLITE_OPEN_READWRITE | c.SQLITE_OPEN_CREATE;
    if (c.sqlite3_open_v2(path_z.ptr, &db, flags, null) != SQLITE_OK) {
        return error.DbOpenFailed;
    }

    try exec(db.?, "PRAGMA busy_timeout=5000;");
    _ = exec(db.?, "PRAGMA journal_mode=WAL;") catch {};
    try exec(db.?, "PRAGMA synchronous=NORMAL;");

    const schema =
        \\CREATE TABLE IF NOT EXISTS yggcrawl (
        \\  key TEXT PRIMARY KEY,
        \\  address TEXT,
        \\  remote TEXT,
        \\  checked BOOLEAN DEFAULT FALSE,
        \\  nodeinfo TEXT,
        \\  peers TEXT,
        \\  port_status TEXT DEFAULT NULL,
        \\  port_checked INTEGER DEFAULT NULL
        \\);
        \\CREATE INDEX IF NOT EXISTS idx_checked ON yggcrawl(checked);
    ;
    try exec(db.?, schema);
    return db.?;
}

fn exec(db: *c.sqlite3, sql: []const u8) !void {
    var errmsg: [*c]u8 = null;
    const rc = c.sqlite3_exec(db, sql.ptr, null, null, &errmsg);
    if (errmsg != null) c.sqlite3_free(errmsg);
    if (rc != SQLITE_OK) {
        std.log.debug("sqlite exec rc={d}", .{rc});
        return error.SqliteExec;
    }
}

fn countKeys(db: *c.sqlite3) !i64 {
    var stmt: ?*c.sqlite3_stmt = null;
    const sql = "SELECT COUNT(*) FROM yggcrawl";
    if (c.sqlite3_prepare_v2(db, sql.ptr, @intCast(sql.len), &stmt, null) != SQLITE_OK)
        return error.Prepare;
    defer _ = c.sqlite3_finalize(stmt);
    if (c.sqlite3_step(stmt) == SQLITE_ROW) {
        return c.sqlite3_column_int64(stmt, 0);
    }
    return 0;
}

fn colText(stmt: ?*c.sqlite3_stmt, idx: c_int) []const u8 {
    const p = c.sqlite3_column_text(stmt, idx);
    if (p == null) return "";
    return std.mem.span(p);
}

fn bindText(stmt: ?*c.sqlite3_stmt, idx: c_int, text: []const u8) void {
    if (text.len == 0) {
        _ = c.sqlite3_bind_null(stmt, idx);
        return;
    }
    _ = c.sqlite3_bind_text(stmt, idx, text.ptr, @intCast(text.len), SQLITE_TRANSIENT);
}

// ===========================================================================
// Admin API (TCP + JSON over std.Io.net)
// ===========================================================================

/// Calls the Yggdrasil Admin API. Returns the response payload
/// (the `response` field when the answer is wrapped as {status,response},
/// otherwise the raw response). Memory is allocated in `alloc`.
fn callAdminAPI(
    alloc: Allocator,
    io: Io,
    endpoint: []const u8,
    request: []const u8,
    key: ?[]const u8,
    timeout_ms: i64,
) ![]u8 {
    // --- build the request body ---
    var obj = std.json.ObjectMap.empty;
    defer obj.deinit(alloc);
    var args = std.json.ObjectMap.empty;
    defer args.deinit(alloc);
    try obj.put(alloc, "keepalive", .{ .bool = false });
    try obj.put(alloc, "request", .{ .string = request });
    if (key) |k| {
        try args.put(alloc, "key", .{ .string = k });
        try obj.put(alloc, "arguments", .{ .object = args });
    }
    const body = try std.json.Stringify.valueAlloc(
        alloc,
        std.json.Value{ .object = obj },
        .{},
    );
    defer alloc.free(body);

    const raw = try tcpExchange(alloc, io, endpoint, body, timeout_ms);
    defer alloc.free(raw);

    return unwrapApiResponse(alloc, raw);
}

/// Splits `{status, response}` apart, failing on an explicit non-success status.
fn unwrapApiResponse(alloc: Allocator, raw: []const u8) ![]u8 {
    const parsed = std.json.parseFromSlice(std.json.Value, alloc, raw, .{}) catch {
        return alloc.dupe(u8, raw);
    };
    defer parsed.deinit();

    if (parsed.value == .object) {
        if (parsed.value.object.get("status")) |st| {
            if (st == .string) {
                if (!std.mem.eql(u8, st.string, "success")) return error.ApiError;
                if (parsed.value.object.get("response")) |resp| {
                    return std.json.Stringify.valueAlloc(alloc, resp, .{});
                }
            }
        }
    }
    return alloc.dupe(u8, raw);
}

/// Opens a TCP connection to `endpoint`, sends `payload`, reads until the peer
/// closes the connection or the timeout expires.
fn tcpExchange(
    alloc: Allocator,
    io: Io,
    endpoint: []const u8,
    payload: []const u8,
    timeout_ms: i64,
) ![]u8 {
    const colon = std.mem.lastIndexOfScalar(u8, endpoint, ':') orelse return error.BadEndpoint;
    var host: []const u8 = endpoint[0..colon];
    const port = try std.fmt.parseInt(u16, endpoint[colon + 1 ..], 10);
    if (host.len >= 2 and host[0] == '[' and host[host.len - 1] == ']') {
        host = host[1 .. host.len - 1];
    }
    if (host.len == 0) host = "127.0.0.1";

    var stream = try connectHost(io, alloc, host, port, timeout_ms);
    defer stream.close(io);

    try sendAll(io, &stream, payload, timeout_ms);
    return recvAll(alloc, io, &stream, timeout_ms);
}

/// Connects to a literal address, falling back to std's own name resolution
/// (which reads /etc/hosts and /etc/resolv.conf — no getaddrinfo involved).
fn connectHost(io: Io, alloc: Allocator, host: []const u8, port: u16, timeout_ms: i64) !net.Stream {
    if (net.IpAddress.parse(host, port)) |addr| {
        return connectWithTimeout(io, addr, timeout_ms);
    } else |_| {}

    var addresses: std.ArrayList(net.IpAddress) = .empty;
    defer addresses.deinit(alloc);
    try resolveHost(alloc, io, host, port, &addresses);

    var last_err: net.IpAddress.ConnectError = error.ConnectionRefused;
    for (addresses.items) |address| {
        if (connectWithTimeout(io, address, timeout_ms)) |stream| {
            return stream;
        } else |err| last_err = err;
    }
    return last_err;
}

/// Name resolution through std's resolver: literal IPs, /etc/hosts, then the
/// DNS servers from /etc/resolv.conf.
fn resolveHost(
    alloc: Allocator,
    io: Io,
    host: []const u8,
    port: u16,
    out: *std.ArrayList(net.IpAddress),
) !void {
    const name = try net.HostName.init(host);

    var results_buffer: [32]net.HostName.LookupResult = undefined;
    var queue: Io.Queue(net.HostName.LookupResult) = .init(&results_buffer);
    var lookup_future = io.async(net.HostName.lookup, .{
        name,
        io,
        &queue,
        net.HostName.LookupOptions{ .port = port },
    });
    defer lookup_future.cancel(io) catch {};

    while (queue.getOne(io)) |result| {
        switch (result) {
            .address => |address| try out.append(alloc, address),
            .canonical_name => {},
        }
    } else |err| switch (err) {
        error.Closed => {},
        error.Canceled => return error.Canceled,
    }
    try lookup_future.await(io);
}

/// A TCP connection with an explicit deadline.
///
/// `std.Io.net`'s `IpAddress.connect` panics as soon as a timeout is requested
/// ("TODO implement netConnectIpPosix with timeout"), so the non-blocking
/// connect + poll dance is spelled out here on top of the syscall layer; the
/// resulting socket is put back into blocking mode for the I/O that follows.
fn connectWithTimeout(
    io: Io,
    address: net.IpAddress,
    timeout_ms: i64,
) net.IpAddress.ConnectError!net.Stream {
    const linux = std.os.linux;

    if (builtin.os.tag != .linux) {
        // No deadline support through the Io vtable on this target.
        const addr = address;
        return addr.connect(io, .{ .mode = .stream });
    }

    const family: u32 = switch (address) {
        .ip4 => linux.AF.INET,
        .ip6 => linux.AF.INET6,
    };
    const raw_fd = linux.socket(
        family,
        linux.SOCK.STREAM | linux.SOCK.NONBLOCK | linux.SOCK.CLOEXEC,
        linux.IPPROTO.TCP,
    );
    switch (linux.errno(raw_fd)) {
        .SUCCESS => {},
        .MFILE, .NFILE => return error.ProcessFdQuotaExceeded,
        .NOBUFS, .NOMEM => return error.SystemResources,
        .AFNOSUPPORT => return error.AddressFamilyUnsupported,
        .PROTONOSUPPORT => return error.ProtocolUnsupportedBySystem,
        else => return error.Unexpected,
    }
    const fd: std.posix.socket_t = @intCast(raw_fd);
    errdefer _ = linux.close(fd);

    var storage: Threaded.PosixAddress = undefined;
    const addr_len = Threaded.addressToPosix(&address, &storage);

    switch (linux.errno(linux.connect(fd, @ptrCast(&storage), addr_len))) {
        .SUCCESS => {},
        .INPROGRESS, .AGAIN => {
            var poll_fds = [1]std.posix.pollfd{.{
                .fd = fd,
                .events = std.posix.POLL.OUT,
                .revents = 0,
            }};
            const clamped: i32 = @intCast(@min(timeout_ms, std.math.maxInt(i32)));
            const ready = std.posix.poll(&poll_fds, clamped) catch return error.Unexpected;
            if (ready == 0) return error.Timeout;

            // A finished connect() is confirmed by the socket having a peer.
            var peer: std.posix.sockaddr = undefined;
            var peer_len: std.posix.socklen_t = @sizeOf(std.posix.sockaddr);
            std.posix.getpeername(fd, &peer, &peer_len) catch return error.ConnectionRefused;
        },
        .CONNREFUSED => return error.ConnectionRefused,
        .CONNRESET => return error.ConnectionResetByPeer,
        .TIMEDOUT => return error.Timeout,
        .NETUNREACH => return error.NetworkUnreachable,
        .HOSTUNREACH => return error.HostUnreachable,
        .ACCES => return error.AccessDenied,
        .ADDRNOTAVAIL => return error.AddressUnavailable,
        else => return error.Unexpected,
    }

    // Reads and writes below rely on the socket blocking.
    _ = linux.fcntl(fd, linux.F.SETFL, 0);

    return .{ .socket = .{ .handle = fd, .address = address } };
}

fn sendAll(io: Io, stream: *const net.Stream, payload: []const u8, timeout_ms: i64) !void {
    var sent: usize = 0;
    while (sent < payload.len) {
        const data = [_][]const u8{payload[sent..]};
        const n = try (try io.operateTimeout(.{ .net_write = .{
            .socket_handle = stream.socket.handle,
            .data = &data,
        } }, timeoutOf(timeout_ms))).net_write;
        if (n == 0) return error.SendFailed;
        sent += n;
    }
}

/// Reads until end of stream. A timeout is not an error here: whatever arrived
/// by then is the answer.
fn recvAll(alloc: Allocator, io: Io, stream: *const net.Stream, timeout_ms: i64) ![]u8 {
    var buf: std.ArrayList(u8) = .empty;
    errdefer buf.deinit(alloc);

    var chunk: [16 * 1024]u8 = undefined;
    while (true) {
        var iovecs = [1][]u8{chunk[0..]};
        const result = io.operateTimeout(.{ .net_read = .{
            .socket_handle = stream.socket.handle,
            .data = &iovecs,
        } }, timeoutOf(timeout_ms)) catch break;

        const read = result.net_read catch break;
        if (read.data_len == 0) break;
        try buf.appendSlice(alloc, chunk[0..read.data_len]);
    }
    return buf.toOwnedSlice(alloc);
}

// ===========================================================================
// Crawler logic
// ===========================================================================

fn fetchAndInsertLocalPeers(alloc: Allocator, io: Io, db: *c.sqlite3, endpoint: []const u8) !void {
    const raw = try callAdminAPI(alloc, io, endpoint, "getPeers", null, ADMIN_TIMEOUT_MS);
    defer alloc.free(raw);

    const parsed = std.json.parseFromSlice(std.json.Value, alloc, raw, .{}) catch
        return error.BadPeersJson;
    defer parsed.deinit();

    const peers = blk: {
        if (parsed.value != .object) return;
        const p = parsed.value.object.get("peers") orelse return;
        if (p != .array) return;
        break :blk p.array;
    };

    var stmt: ?*c.sqlite3_stmt = null;
    const sql =
        "INSERT INTO yggcrawl (key, address, remote) VALUES (?, ?, ?) " ++
        "ON CONFLICT(key) DO UPDATE SET address=excluded.address, remote=excluded.remote";
    if (c.sqlite3_prepare_v2(db, sql.ptr, @intCast(sql.len), &stmt, null) != SQLITE_OK)
        return error.Prepare;
    defer _ = c.sqlite3_finalize(stmt);

    try exec(db, "BEGIN");
    errdefer _ = exec(db, "ROLLBACK") catch {};

    for (peers.items) |peer| {
        if (peer != .object) continue;
        _ = c.sqlite3_reset(stmt);
        bindText(stmt, 1, jsonStr(peer.object.get("key")));
        bindText(stmt, 2, jsonStr(peer.object.get("address")));
        bindText(stmt, 3, jsonStr(peer.object.get("remote")));
        _ = c.sqlite3_step(stmt);
    }
    try exec(db, "COMMIT");
}

fn jsonStr(v: ?std.json.Value) []const u8 {
    const val = v orelse return "";
    return switch (val) {
        .string => |s| s,
        else => "",
    };
}

// ---- phase: debug_remotegetpeers -----------------------------------------

const RemoteResult = struct {
    ok: bool = false,
    raw: []u8 = &.{},
    found: []const []u8 = &.{},
};

const RemoteCtx = struct {
    alloc: Allocator,
    endpoint: []const u8,
    keys: []const []const u8,
    results: []RemoteResult,
};

fn remoteWorker(io: Io, ctx: *RemoteCtx, i: usize) void {
    const k = ctx.keys[i];
    const raw = callAdminAPI(ctx.alloc, io, ctx.endpoint, "debug_remotegetpeers", k, REMOTE_TIMEOUT_MS) catch return;

    var found: std.ArrayList([]u8) = .empty;
    const parsed = std.json.parseFromSlice(std.json.Value, ctx.alloc, raw, .{}) catch {
        ctx.results[i] = .{ .ok = true, .raw = raw, .found = &.{} };
        return;
    };
    defer parsed.deinit();
    if (parsed.value == .object) {
        var vit = parsed.value.object.iterator();
        while (vit.next()) |entry| {
            const obj = entry.value_ptr.*;
            if (obj != .object) continue;
            const keys_v = obj.object.get("keys") orelse continue;
            if (keys_v != .array) continue;
            for (keys_v.array.items) |item| {
                if (item == .string) {
                    const dup = ctx.alloc.dupe(u8, item.string) catch continue;
                    found.append(ctx.alloc, dup) catch {};
                }
            }
        }
    }
    const found_owned = found.toOwnedSlice(ctx.alloc) catch &.{};
    ctx.results[i] = .{ .ok = true, .raw = raw, .found = found_owned };
}

fn freeRemoteResults(alloc: Allocator, results: []RemoteResult) void {
    for (results) |r| {
        if (r.raw.len != 0) alloc.free(r.raw);
        for (r.found) |f| alloc.free(f);
        if (r.found.len != 0) alloc.free(r.found);
    }
}

fn fetchAndUpdateRemoteGetPeers(
    alloc: Allocator,
    io: Io,
    db: *c.sqlite3,
    endpoint: []const u8,
    workers: usize,
) !void {
    var keys = try queryStrings(alloc, db, "SELECT key FROM yggcrawl WHERE peers IS NULL OR peers = ''");
    defer freeStringList(alloc, &keys);
    if (keys.items.len == 0) return;

    const results = try alloc.alloc(RemoteResult, keys.items.len);
    defer alloc.free(results);
    for (results) |*r| r.* = .{};
    defer freeRemoteResults(alloc, results);

    var ctx = RemoteCtx{
        .alloc = alloc,
        .endpoint = endpoint,
        .keys = keys.items,
        .results = results,
    };
    try runWorkers(io, &ctx, results.len, workers, remoteWorker);

    const upd = "UPDATE yggcrawl SET peers = ? WHERE key = ?";
    const ins = "INSERT INTO yggcrawl (key) VALUES (?) ON CONFLICT(key) DO NOTHING";

    try exec(db, "BEGIN");

    var upd_stmt: ?*c.sqlite3_stmt = null;
    var ins_stmt: ?*c.sqlite3_stmt = null;
    if (c.sqlite3_prepare_v2(db, upd.ptr, @intCast(upd.len), &upd_stmt, null) != SQLITE_OK)
        return error.Prepare;
    defer _ = c.sqlite3_finalize(upd_stmt);
    if (c.sqlite3_prepare_v2(db, ins.ptr, @intCast(ins.len), &ins_stmt, null) != SQLITE_OK)
        return error.Prepare;
    defer _ = c.sqlite3_finalize(ins_stmt);

    for (results, 0..) |r, i| {
        if (!r.ok) continue;
        _ = c.sqlite3_reset(upd_stmt);
        bindText(upd_stmt, 1, r.raw);
        bindText(upd_stmt, 2, keys.items[i]);
        _ = c.sqlite3_step(upd_stmt);

        for (r.found) |fk| {
            _ = c.sqlite3_reset(ins_stmt);
            bindText(ins_stmt, 1, fk);
            _ = c.sqlite3_step(ins_stmt);
        }
    }
    try exec(db, "COMMIT");
}

/// Dispatches `count` tasks of `worker` across the Io thread pool. The pool is
/// bounded by `workers`; tasks beyond that limit run eagerly on this thread, so
/// this behaves like a worker pool with an exact ceiling.
fn runWorkers(
    io: Io,
    ctx: anytype,
    count: usize,
    workers: usize,
    worker: anytype,
) !void {
    var group: Io.Group = .init;
    for (0..count) |i| {
        group.async(io, worker, .{ io, ctx, i });
        // Tasks past the pool limit ran eagerly, so bound the spawned-out work
        // by awaiting what is already in flight before adding more.
        if ((i + 1) % workers == 0) group.await(io) catch {};
    }
    group.await(io) catch {};
}

// ---- phase: getNodeInfo ---------------------------------------------------

const NodeCtx = struct {
    alloc: Allocator,
    endpoint: []const u8,
    keys: []const []const u8,
    results: []?[]u8,
};

fn nodeWorker(io: Io, ctx: *NodeCtx, i: usize) void {
    const raw = callAdminAPI(ctx.alloc, io, ctx.endpoint, "getNodeInfo", ctx.keys[i], REMOTE_TIMEOUT_MS) catch return;
    ctx.results[i] = raw;
}

fn fetchAndUpdateNodeInfo(
    alloc: Allocator,
    io: Io,
    db: *c.sqlite3,
    endpoint: []const u8,
    workers: usize,
) !void {
    var keys = try queryStrings(alloc, db, "SELECT key FROM yggcrawl WHERE checked = FALSE");
    defer freeStringList(alloc, &keys);
    if (keys.items.len == 0) return;

    const results = try alloc.alloc(?[]u8, keys.items.len);
    defer alloc.free(results);
    for (results) |*r| r.* = null;
    defer for (results) |r| {
        if (r) |x| alloc.free(x);
    };

    var ctx = NodeCtx{
        .alloc = alloc,
        .endpoint = endpoint,
        .keys = keys.items,
        .results = results,
    };
    try runWorkers(io, &ctx, results.len, workers, nodeWorker);

    const upd = "UPDATE yggcrawl SET nodeinfo = ?, checked = TRUE WHERE key = ?";
    var stmt: ?*c.sqlite3_stmt = null;
    if (c.sqlite3_prepare_v2(db, upd.ptr, @intCast(upd.len), &stmt, null) != SQLITE_OK)
        return error.Prepare;
    defer _ = c.sqlite3_finalize(stmt);

    try exec(db, "BEGIN");
    for (results, 0..) |r, i| {
        const payload = r orelse continue;
        _ = c.sqlite3_reset(stmt);
        bindText(stmt, 1, payload);
        bindText(stmt, 2, keys.items[i]);
        _ = c.sqlite3_step(stmt);
    }
    try exec(db, "COMMIT");
}

// ---- phase: port scan -----------------------------------------------------

const Task = struct {
    ip: []u8,
    keys: [][]u8,
};

const ScanCtx = struct {
    tasks: []Task,
    port: u16,
    results: []bool,
};

fn scanWorker(io: Io, ctx: *ScanCtx, i: usize) void {
    ctx.results[i] = probePort(io, ctx.tasks[i].ip, ctx.port, PROBE_TIMEOUT_MS);
}

/// True when a TCP connection to `host:port` could be established.
fn probePort(io: Io, host: []const u8, port: u16, timeout_ms: i64) bool {
    const addr = net.IpAddress.parse(host, port) catch
        net.IpAddress.resolve(io, host, port) catch return false;
    var stream = connectWithTimeout(io, addr, timeout_ms) catch return false;
    stream.close(io);
    return true;
}

fn checkPortOnAllPeers(
    alloc: Allocator,
    io: Io,
    db: *c.sqlite3,
    workers: usize,
    port: u16,
) !void {
    // build map[ip] -> [keys]
    var address_to_keys = std.array_hash_map.String(std.ArrayList([]u8)).empty;
    defer {
        var it = address_to_keys.iterator();
        while (it.next()) |e| {
            e.value_ptr.deinit(alloc);
            alloc.free(e.key_ptr.*);
        }
        address_to_keys.deinit(alloc);
    }

    {
        const sql = "SELECT key, address, peers FROM yggcrawl WHERE address != '' OR peers != ''";
        var stmt: ?*c.sqlite3_stmt = null;
        if (c.sqlite3_prepare_v2(db, sql.ptr, @intCast(sql.len), &stmt, null) != SQLITE_OK)
            return error.Prepare;
        defer _ = c.sqlite3_finalize(stmt);

        while (c.sqlite3_step(stmt) == SQLITE_ROW) {
            const key = colText(stmt, 0);
            const addr = colText(stmt, 1);
            const peers = colText(stmt, 2);

            if (addr.len != 0) try addAddrKey(alloc, &address_to_keys, addr, key);
            if (peers.len != 0) {
                // `peers` is a JSON object keyed by the peer address.
                const parsed = std.json.parseFromSlice(std.json.Value, alloc, peers, .{}) catch continue;
                defer parsed.deinit();
                if (parsed.value == .object) {
                    var it = parsed.value.object.iterator();
                    while (it.next()) |e| try addAddrKey(alloc, &address_to_keys, e.key_ptr.*, key);
                }
            }
        }
    }

    if (address_to_keys.count() == 0) return;

    // build the tasks
    const tasks = try alloc.alloc(Task, address_to_keys.count());
    defer {
        for (tasks) |t| {
            alloc.free(t.ip);
            for (t.keys) |k| alloc.free(k);
            alloc.free(t.keys);
        }
        alloc.free(tasks);
    }
    {
        var it = address_to_keys.iterator();
        var ti: usize = 0;
        while (it.next()) |e| {
            tasks[ti] = .{
                .ip = try alloc.dupe(u8, e.key_ptr.*),
                .keys = try e.value_ptr.toOwnedSlice(alloc),
            };
            ti += 1;
        }
    }

    const results = try alloc.alloc(bool, tasks.len);
    defer alloc.free(results);
    for (results) |*r| r.* = false;

    var ctx = ScanCtx{ .tasks = tasks, .port = port, .results = results };
    try runWorkers(io, &ctx, results.len, workers, scanWorker);

    // store the results
    const upd = "UPDATE yggcrawl SET port_status = ?, port_checked = ? WHERE key = ?";
    var stmt: ?*c.sqlite3_stmt = null;
    if (c.sqlite3_prepare_v2(db, upd.ptr, @intCast(upd.len), &stmt, null) != SQLITE_OK)
        return error.Prepare;
    defer _ = c.sqlite3_finalize(stmt);

    try exec(db, "BEGIN");
    for (tasks, 0..) |t, i| {
        const status: []const u8 = if (results[i]) "open" else "closed";
        for (t.keys) |k| {
            _ = c.sqlite3_reset(stmt);
            bindText(stmt, 1, status);
            _ = c.sqlite3_bind_int64(stmt, 2, port);
            bindText(stmt, 3, k);
            _ = c.sqlite3_step(stmt);
        }
    }
    try exec(db, "COMMIT");
}

fn addAddrKey(
    alloc: Allocator,
    map: *std.array_hash_map.String(std.ArrayList([]u8)),
    ip: []const u8,
    key: []const u8,
) !void {
    const gop = try map.getOrPut(alloc, ip);
    if (!gop.found_existing) {
        gop.key_ptr.* = try alloc.dupe(u8, ip);
        gop.value_ptr.* = .empty;
    }
    try gop.value_ptr.append(alloc, try alloc.dupe(u8, key));
}

// ===========================================================================
// JSON export
// ===========================================================================

fn exportToJSON(alloc: Allocator, io: Io, db: *c.sqlite3, filename: []const u8) !void {
    var arena_state = std.heap.ArenaAllocator.init(alloc);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    var kvs = std.ArrayList(std.json.Value).empty;

    {
        const sql = "SELECT address, key, nodeinfo, peers, port_status, port_checked FROM yggcrawl";
        var stmt: ?*c.sqlite3_stmt = null;
        if (c.sqlite3_prepare_v2(db, sql.ptr, @intCast(sql.len), &stmt, null) != SQLITE_OK)
            return error.Prepare;
        defer _ = c.sqlite3_finalize(stmt);

        while (c.sqlite3_step(stmt) == SQLITE_ROW) {
            // column text is only valid until the next step(): copy first.
            const addr = try a.dupe(u8, colText(stmt, 0));
            const key = try a.dupe(u8, colText(stmt, 1));
            const nodeinfo = try a.dupe(u8, colText(stmt, 2));
            const peers = try a.dupe(u8, colText(stmt, 3));
            const pstatus = try a.dupe(u8, colText(stmt, 4));
            const pchecked_null = c.sqlite3_column_type(stmt, 5) == c.SQLITE_NULL;
            const pchecked = c.sqlite3_column_int64(stmt, 5);

            var item = std.json.ObjectMap.empty;

            // address (or the first ip from peers)
            var address_val: ?[]const u8 = null;
            if (addr.len != 0) {
                address_val = addr;
            } else if (peers.len != 0) {
                if (std.json.parseFromSlice(std.json.Value, a, peers, .{}) catch null) |pd| {
                    if (pd.value == .object) {
                        var it = pd.value.object.iterator();
                        if (it.next()) |e| address_val = e.key_ptr.*;
                    }
                }
            }
            if (address_val) |av| {
                try item.put(a, "address", .{ .string = av });
            } else {
                try item.put(a, "address", .null);
            }

            // key
            if (key.len != 0) try item.put(a, "key", .{ .string = key }) else try item.put(a, "key", .null);

            // nodeinfo
            if (nodeinfo.len != 0) {
                try item.put(a, "nodeinfo", .{ .string = nodeinfo });
            } else {
                try item.put(a, "nodeinfo", .null);
            }

            // port_status
            if (pstatus.len != 0) {
                try item.put(a, "port_status", .{ .string = pstatus });
            } else {
                try item.put(a, "port_status", .null);
            }

            // port_checked
            if (pchecked_null) {
                try item.put(a, "port_checked", .null);
            } else {
                try item.put(a, "port_checked", .{ .integer = pchecked });
            }

            // name (from nodeinfo)
            var name: []const u8 = "";
            if (nodeinfo.len != 0) {
                if (std.json.parseFromSlice(std.json.Value, a, nodeinfo, .{}) catch null) |nd| {
                    if (nd.value == .object) {
                        var it = nd.value.object.iterator();
                        while (it.next()) |e| {
                            const node = e.value_ptr.*;
                            if (node != .object) continue;
                            if (node.object.get("name")) |nv| {
                                if (nv == .string and nv.string.len != 0) {
                                    name = nv.string;
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            try item.put(a, "name", .{ .string = name });

            try kvs.append(a, .{ .object = item });
        }
    }

    const owned = try kvs.toOwnedSlice(a);
    const root: std.json.Value = .{ .array = std.json.Array.fromOwnedSlice(a, owned) };
    const text = try std.json.Stringify.valueAlloc(a, root, .{ .whitespace = .indent_2 });

    try writeFileAtomic(io, filename, text);
}

fn writeFileAtomic(io: Io, filename: []const u8, content: []const u8) !void {
    var atomic = try Dir.cwd().createFileAtomic(io, filename, .{ .replace = true });
    defer atomic.deinit(io);

    var buf: [8 * 1024]u8 = undefined;
    var writer = atomic.file.writer(io, &buf);
    try writer.interface.writeAll(content);
    try writer.interface.writeAll("\n");
    try writer.interface.flush();

    try atomic.replace(io);
}

// ===========================================================================
// helpers
// ===========================================================================

fn freeStringList(alloc: Allocator, list: *std.ArrayList([]u8)) void {
    for (list.items) |s| alloc.free(s);
    list.deinit(alloc);
}

fn queryStrings(alloc: Allocator, db: *c.sqlite3, sql: []const u8) !std.ArrayList([]u8) {
    var out = std.ArrayList([]u8).empty;
    var stmt: ?*c.sqlite3_stmt = null;
    if (c.sqlite3_prepare_v2(db, sql.ptr, @intCast(sql.len), &stmt, null) != SQLITE_OK)
        return error.Prepare;
    defer _ = c.sqlite3_finalize(stmt);
    while (c.sqlite3_step(stmt) == SQLITE_ROW) {
        const s = colText(stmt, 0);
        try out.append(alloc, try alloc.dupe(u8, s));
    }
    return out;
}
