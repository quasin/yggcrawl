const std = @import("std");
const c = @import("c");
const Allocator = std.mem.Allocator;

// ---------------------------------------------------------------------------
// YggCrawl — Yggdrasil NodeInfo crawler on Zig 0.17.0.
//
// Architecture:
//   1) collect local peers through the Yggdrasil Admin API (TCP, JSON)
//   2) walk the network graph in rounds: debug_remotegetpeers + getNodeInfo
//   3) probe the target TCP port on every known address
//   4) export the results to JSON
// Storage — SQLite (through the system libsqlite3).
// ---------------------------------------------------------------------------

const DEFAULT_WORKERS: i64 = 32;
const DEFAULT_PORT: i64 = 80;
const DEFAULT_ADMIN = "127.0.0.1:9001";
const DEFAULT_DB = "./yggcrawl.db";

const SQLITE_OK = c.SQLITE_OK;
const SQLITE_ROW = c.SQLITE_ROW;
const SQLITE_DONE = c.SQLITE_DONE;

const SQLITE_TRANSIENT: c.sqlite3_destructor_type =
    @ptrFromInt(@as(usize, @bitCast(@as(isize, -1))));

var g_out: *std.Io.Writer = undefined;

fn print(comptime fmt: []const u8, args: anytype) void {
    g_out.print(fmt, args) catch {};
}

fn println(comptime fmt: []const u8, args: anytype) void {
    g_out.print(fmt ++ "\n", args) catch {};
    g_out.flush() catch {};
}

/// Simple blocking mutex on top of std.atomic.Mutex.
const Mutex = struct {
    m: std.atomic.Mutex = .unlocked,

    fn lock(self: *Mutex) void {
        while (!self.m.tryLock()) std.Thread.yield() catch {};
    }

    fn unlock(self: *Mutex) void {
        self.m.unlock();
    }
};

// ===========================================================================
// main
// ===========================================================================

pub fn main(init: std.process.Init) !void {
    const alloc = init.gpa;
    const io = init.io;

    var stdout_buf: [1 << 16]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(io, &stdout_buf);
    g_out = &stdout_writer.interface;

    const argv = try init.minimal.args.toSlice(init.arena.allocator());

    var workers: i64 = DEFAULT_WORKERS;
    var port: i64 = DEFAULT_PORT;
    var admin: []const u8 = DEFAULT_ADMIN;
    var db_path: []const u8 = DEFAULT_DB;

    var i: usize = 1;
    while (i < argv.len) : (i += 1) {
        const a: []const u8 = argv[i];
        if (flagMatch(a, "-workers", "--workers")) |m| {
            switch (m) {
                .val => |v| workers = std.fmt.parseInt(i64, v, 10) catch workers,
                .bare => if (i + 1 < argv.len) {
                    i += 1;
                    workers = std.fmt.parseInt(i64, argv[i], 10) catch workers;
                },
                .no => {},
            }
        } else if (flagMatch(a, "-port", "--port")) |m| {
            switch (m) {
                .val => |v| port = std.fmt.parseInt(i64, v, 10) catch port,
                .bare => if (i + 1 < argv.len) {
                    i += 1;
                    port = std.fmt.parseInt(i64, argv[i], 10) catch port;
                },
                .no => {},
            }
        } else if (flagMatch(a, "-admin", "--admin")) |m| {
            switch (m) {
                .val => |v| admin = v,
                .bare => if (i + 1 < argv.len) {
                    i += 1;
                    admin = argv[i];
                },
                .no => {},
            }
        } else if (flagMatch(a, "-db", "--db")) |m| {
            switch (m) {
                .val => |v| db_path = v,
                .bare => if (i + 1 < argv.len) {
                    i += 1;
                    db_path = argv[i];
                },
                .no => {},
            }
        }
    }

    if (workers < 1) workers = 1;

    println("YggCrawl - High-performance Yggdrasil crawler (SQLite)", .{});
    println("Workers: {d} | Admin API: {s} | Port to probe: {d}", .{ workers, admin, port });
    println("------------------------------------------------------------------", .{});

    const db = initDB(db_path) catch |err| {
        println("Database initialization failed: {s}", .{@errorName(err)});
        return;
    };
    defer _ = c.sqlite3_close(db);

    // 1. Local peers
    println("[1/4] Collecting local peers...", .{});
    fetchAndInsertLocalPeers(alloc, db, admin) catch |err| {
        println("Failed to fetch local peers: {s}", .{@errorName(err)});
        return;
    };

    // 2. Graph walk
    println("[2/4] Starting the network graph walk...", .{});
    var previous_count: i64 = -1;
    while (true) {
        fetchAndUpdateRemoteGetPeers(alloc, db, admin, @intCast(workers)) catch |err| {
            println("remote get peers failed: {s}", .{@errorName(err)});
        };
        fetchAndUpdateNodeInfo(alloc, db, admin, @intCast(workers)) catch |err| {
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
        std.Io.sleep(io, std.Io.Duration.fromSeconds(2), .awake) catch {};
    }

    // 3. Port probe
    println("[3/4] Probing port {d}...", .{port});
    checkPortOnAllPeers(alloc, db, @intCast(workers), @intCast(port)) catch |err| {
        println("Port scan failed: {s}", .{@errorName(err)});
    };

    // 4. Export
    println("[4/4] Exporting data to JSON...", .{});
    exportToJSON(alloc, db, "yggcrawl.json") catch |err| {
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

// ===========================================================================
// SQLite
// ===========================================================================

fn initDB(path: []const u8) !*c.sqlite3 {
    const path_z = try std.heap.c_allocator.dupeSentinel(u8, path, 0);
    defer std.heap.c_allocator.free(path_z);

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

// ===========================================================================
// Admin API (TCP + JSON)
// ===========================================================================

/// Calls the Yggdrasil Admin API. Returns the response payload
/// (the `response` field when the answer is wrapped as {status,response},
/// otherwise the raw response). Memory is allocated in `alloc`.
fn callAdminAPI(
    alloc: Allocator,
    endpoint: []const u8,
    request: []const u8,
    key: ?[]const u8,
    timeout_ms: i32,
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

    const raw = try tcpRequest(alloc, endpoint, body, timeout_ms);
    defer alloc.free(raw);

    // --- unwrap the response ---
    const parsed = std.json.parseFromSlice(std.json.Value, alloc, raw, .{}) catch {
        return alloc.dupe(u8, raw);
    };
    defer parsed.deinit();

    if (parsed.value == .object) {
        if (parsed.value.object.get("status")) |st| {
            if (st == .string) {
                if (!std.mem.eql(u8, st.string, "success")) {
                    return error.ApiError;
                }
                if (parsed.value.object.get("response")) |resp| {
                    const s = try std.json.Stringify.valueAlloc(alloc, resp, .{});
                    return s;
                }
            }
        }
    }
    return alloc.dupe(u8, raw);
}

/// Opens a TCP connection, sends `payload`, reads the response until close.
fn tcpRequest(alloc: Allocator, endpoint: []const u8, payload: []const u8, timeout_ms: i32) ![]u8 {
    const colon = std.mem.lastIndexOfScalar(u8, endpoint, ':') orelse return error.BadEndpoint;
    var host: []const u8 = endpoint[0..colon];
    const port_str = endpoint[colon + 1 ..];
    if (host.len >= 2 and host[0] == '[' and host[host.len - 1] == ']') {
        host = host[1 .. host.len - 1];
    }
    if (host.len == 0) host = "127.0.0.1";

    const host_z = try alloc.dupeSentinel(u8, host, 0);
    defer alloc.free(host_z);
    const port_z = try alloc.dupeSentinel(u8, port_str, 0);
    defer alloc.free(port_z);

    var hints = std.mem.zeroes(c.struct_addrinfo);
    hints.ai_family = c.AF_UNSPEC;
    hints.ai_socktype = c.SOCK_STREAM;

    var res: ?*c.struct_addrinfo = null;
    if (c.getaddrinfo(host_z.ptr, port_z.ptr, &hints, &res) != 0) return error.NameResolution;
    defer c.freeaddrinfo(res);

    var fd: c_int = -1;
    var it = res;
    while (it) |ai| : (it = ai.ai_next) {
        const s = c.socket(ai.ai_family, ai.ai_socktype, ai.ai_protocol);
        if (s < 0) continue;
        if (connectTimeout(s, ai.ai_addr, ai.ai_addrlen, timeout_ms)) {
            fd = s;
            break;
        }
        _ = c.close(s);
    }
    if (fd < 0) return error.Connect;

    defer _ = c.close(fd);

    // read/write timeouts
    setSockTimeout(fd, timeout_ms);

    // send
    var sent: usize = 0;
    while (sent < payload.len) {
        const n = c.send(fd, payload.ptr + sent, payload.len - sent, c.MSG_NOSIGNAL);
        if (n <= 0) return error.SendFailed;
        sent += @intCast(n);
    }

    // read until EOF
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(alloc);
    var chunk: [16384]u8 = undefined;
    while (true) {
        const n = c.recv(fd, &chunk, chunk.len, 0);
        if (n > 0) {
            try buf.appendSlice(alloc, chunk[0..@intCast(n)]);
        } else if (n == 0) {
            break;
        } else {
            const e = std.c._errno().*;
            if (e == c.EINTR) continue;
            // timeout/error — return whatever we managed to read
            break;
        }
    }
    return alloc.dupe(u8, buf.items);
}

fn connectTimeout(fd: c_int, addr: ?*const c.struct_sockaddr, addrlen: c.socklen_t, timeout_ms: i32) bool {
    const old = c.fcntl(fd, c.F_GETFL, @as(c_int, 0));
    _ = c.fcntl(fd, c.F_SETFL, @as(c_int, old | c.O_NONBLOCK));

    const rc = c.connect(fd, addr, addrlen);
    if (rc == 0) {
        _ = c.fcntl(fd, c.F_SETFL, old);
        return true;
    }

    const e = std.c._errno().*;
    if (e != c.EINPROGRESS and e != c.EWOULDBLOCK) return false;

    var pfd = c.struct_pollfd{ .fd = fd, .events = c.POLLOUT, .revents = 0 };
    const pr = c.poll(&pfd, 1, timeout_ms);
    if (pr <= 0) return false;

    var so_err: c_int = 0;
    var len: c.socklen_t = @sizeOf(c_int);
    if (c.getsockopt(fd, c.SOL_SOCKET, c.SO_ERROR, &so_err, &len) != 0) return false;
    if (so_err != 0) return false;

    _ = c.fcntl(fd, c.F_SETFL, old);
    return true;
}

fn setSockTimeout(fd: c_int, timeout_ms: i32) void {
    const tv = c.struct_timeval{
        .tv_sec = @divTrunc(timeout_ms, 1000),
        .tv_usec = @intCast(@mod(timeout_ms, 1000) * 1000),
    };
    _ = c.setsockopt(fd, c.SOL_SOCKET, c.SO_RCVTIMEO, &tv, @sizeOf(c.struct_timeval));
    _ = c.setsockopt(fd, c.SOL_SOCKET, c.SO_SNDTIMEO, &tv, @sizeOf(c.struct_timeval));
}

// ===========================================================================
// Worker pool
// ===========================================================================

fn runPool(comptime Ctx: type, alloc: Allocator, workers: usize, ctx: *Ctx, comptime workerFn: fn (*Ctx) void) void {
    if (workers == 0) return;
    const threads = alloc.alloc(std.Thread, workers) catch {
        workerFn(ctx);
        return;
    };
    defer alloc.free(threads);

    var spawned: usize = 0;
    for (0..workers) |k| {
        threads[k] = std.Thread.spawn(.{}, workerFn, .{ctx}) catch break;
        spawned += 1;
    }
    if (spawned == 0) {
        workerFn(ctx);
        return;
    }
    for (threads[0..spawned]) |t| t.join();
}

fn nextIndex(comptime Ctx: type, ctx: *Ctx) ?usize {
    ctx.mutex.lock();
    defer ctx.mutex.unlock();
    if (ctx.next >= ctx.n) return null;
    const i = ctx.next;
    ctx.next += 1;
    return i;
}

// ===========================================================================
// Crawler logic
// ===========================================================================

fn fetchAndInsertLocalPeers(alloc: Allocator, db: *c.sqlite3, endpoint: []const u8) !void {
    const raw = try callAdminAPI(alloc, endpoint, "getPeers", null, 3000);
    defer alloc.free(raw);

    const parsed = std.json.parseFromSlice(std.json.Value, alloc, raw, .{}) catch return error.BadPeersJson;
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
        const key = jsonStr(peer.object.get("key"));
        const address = jsonStr(peer.object.get("address"));
        const remote = jsonStr(peer.object.get("remote"));
        _ = c.sqlite3_reset(stmt);
        _ = c.sqlite3_bind_text(stmt, 1, key.ptr, @intCast(key.len), SQLITE_TRANSIENT);
        _ = c.sqlite3_bind_text(stmt, 2, address.ptr, @intCast(address.len), SQLITE_TRANSIENT);
        _ = c.sqlite3_bind_text(stmt, 3, remote.ptr, @intCast(remote.len), SQLITE_TRANSIENT);
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

const RemoteCtx = struct {
    alloc: Allocator,
    endpoint: []const u8,
    keys: []const []const u8,
    n: usize,
    next: usize = 0,
    mutex: Mutex = .{},
    results: []RemoteResult,

    const RemoteResult = struct {
        ok: bool = false,
        raw: []u8 = &.{},
        found: []const []u8 = &.{},
    };
};

fn remoteWorker(ctx: *RemoteCtx) void {
    while (nextIndex(RemoteCtx, ctx)) |i| {
        const k = ctx.keys[i];
        const raw = callAdminAPI(ctx.alloc, ctx.endpoint, "debug_remotegetpeers", k, 4000) catch continue;

        var found: std.ArrayList([]u8) = .empty;
        const parsed = std.json.parseFromSlice(std.json.Value, ctx.alloc, raw, .{}) catch {
            ctx.results[i] = .{ .ok = true, .raw = raw, .found = &.{} };
            continue;
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
}

fn freeRemoteResults(alloc: Allocator, results: []RemoteCtx.RemoteResult) void {
    for (results) |r| {
        if (r.raw.len != 0) alloc.free(r.raw);
        for (r.found) |f| alloc.free(f);
        if (r.found.len != 0) alloc.free(r.found);
    }
}

fn fetchAndUpdateRemoteGetPeers(alloc: Allocator, db: *c.sqlite3, endpoint: []const u8, workers: usize) !void {
    var keys = try queryStrings(alloc, db, "SELECT key FROM yggcrawl WHERE peers IS NULL OR peers = ''");
    defer freeStringList(alloc, &keys);
    if (keys.items.len == 0) return;

    const results = try alloc.alloc(RemoteCtx.RemoteResult, keys.items.len);
    defer alloc.free(results);
    for (results) |*r| r.* = .{};
    defer freeRemoteResults(alloc, results);

    var ctx = RemoteCtx{
        .alloc = alloc,
        .endpoint = endpoint,
        .keys = keys.items,
        .n = keys.items.len,
        .results = results,
    };
    runPool(RemoteCtx, alloc, workers, &ctx, remoteWorker);

    const upd = "UPDATE yggcrawl SET peers = ? WHERE key = ?";
    const ins = "INSERT INTO yggcrawl (key) VALUES (?) ON CONFLICT(key) DO NOTHING";

    try exec(db, "BEGIN");

    var upd_stmt: ?*c.sqlite3_stmt = null;
    var ins_stmt: ?*c.sqlite3_stmt = null;
    if (c.sqlite3_prepare_v2(db, upd.ptr, @intCast(upd.len), &upd_stmt, null) != SQLITE_OK) return error.Prepare;
    defer _ = c.sqlite3_finalize(upd_stmt);
    if (c.sqlite3_prepare_v2(db, ins.ptr, @intCast(ins.len), &ins_stmt, null) != SQLITE_OK) return error.Prepare;
    defer _ = c.sqlite3_finalize(ins_stmt);

    for (results, 0..) |r, i| {
        if (!r.ok) continue;
        _ = c.sqlite3_reset(upd_stmt);
        _ = c.sqlite3_bind_text(upd_stmt, 1, r.raw.ptr, @intCast(r.raw.len), SQLITE_TRANSIENT);
        _ = c.sqlite3_bind_text(upd_stmt, 2, keys.items[i].ptr, @intCast(keys.items[i].len), SQLITE_TRANSIENT);
        _ = c.sqlite3_step(upd_stmt);

        for (r.found) |fk| {
            _ = c.sqlite3_reset(ins_stmt);
            _ = c.sqlite3_bind_text(ins_stmt, 1, fk.ptr, @intCast(fk.len), SQLITE_TRANSIENT);
            _ = c.sqlite3_step(ins_stmt);
        }
    }
    try exec(db, "COMMIT");
}

// ---- phase: getNodeInfo ---------------------------------------------------

const NodeCtx = struct {
    alloc: Allocator,
    endpoint: []const u8,
    keys: []const []const u8,
    n: usize,
    next: usize = 0,
    mutex: Mutex = .{},
    results: []?[]u8,
};

fn nodeWorker(ctx: *NodeCtx) void {
    while (nextIndex(NodeCtx, ctx)) |i| {
        const raw = callAdminAPI(ctx.alloc, ctx.endpoint, "getNodeInfo", ctx.keys[i], 4000) catch continue;
        ctx.results[i] = raw;
    }
}

fn fetchAndUpdateNodeInfo(alloc: Allocator, db: *c.sqlite3, endpoint: []const u8, workers: usize) !void {
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
        .n = keys.items.len,
        .results = results,
    };
    runPool(NodeCtx, alloc, workers, &ctx, nodeWorker);

    const upd = "UPDATE yggcrawl SET nodeinfo = ?, checked = TRUE WHERE key = ?";
    var stmt: ?*c.sqlite3_stmt = null;
    if (c.sqlite3_prepare_v2(db, upd.ptr, @intCast(upd.len), &stmt, null) != SQLITE_OK) return error.Prepare;
    defer _ = c.sqlite3_finalize(stmt);

    try exec(db, "BEGIN");
    for (results, 0..) |r, i| {
        const payload = r orelse continue;
        _ = c.sqlite3_reset(stmt);
        _ = c.sqlite3_bind_text(stmt, 1, payload.ptr, @intCast(payload.len), SQLITE_TRANSIENT);
        _ = c.sqlite3_bind_text(stmt, 2, keys.items[i].ptr, @intCast(keys.items[i].len), SQLITE_TRANSIENT);
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
    alloc: Allocator,
    tasks: []Task,
    n: usize,
    next: usize = 0,
    mutex: Mutex = .{},
    port: u16,
    results: []bool,
};

fn scanWorker(ctx: *ScanCtx) void {
    while (nextIndex(ScanCtx, ctx)) |i| {
        const t = ctx.tasks[i];
        ctx.results[i] = dialTcp(t.ip, ctx.port, 3000);
    }
}

fn dialTcp(host: []const u8, port: u16, timeout_ms: i32) bool {
    var port_buf: [16]u8 = undefined;
    const ps = std.fmt.bufPrint(port_buf[0..15], "{d}", .{port}) catch return false;
    port_buf[ps.len] = 0;
    const port_z: [:0]const u8 = port_buf[0..ps.len :0];
    const host_z = std.heap.c_allocator.dupeSentinel(u8, host, 0) catch return false;
    defer std.heap.c_allocator.free(host_z);

    var hints = std.mem.zeroes(c.struct_addrinfo);
    hints.ai_family = c.AF_UNSPEC;
    hints.ai_socktype = c.SOCK_STREAM;

    var res: ?*c.struct_addrinfo = null;
    if (c.getaddrinfo(host_z.ptr, port_z.ptr, &hints, &res) != 0) return false;
    defer c.freeaddrinfo(res);

    var it = res;
    while (it) |ai| : (it = ai.ai_next) {
        const s = c.socket(ai.ai_family, ai.ai_socktype, ai.ai_protocol);
        if (s < 0) continue;
        const ok = connectTimeout(s, ai.ai_addr, ai.ai_addrlen, timeout_ms);
        _ = c.close(s);
        if (ok) return true;
    }
    return false;
}

fn checkPortOnAllPeers(alloc: Allocator, db: *c.sqlite3, workers: usize, port: u16) !void {
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
        if (c.sqlite3_prepare_v2(db, sql.ptr, @intCast(sql.len), &stmt, null) != SQLITE_OK) return error.Prepare;
        defer _ = c.sqlite3_finalize(stmt);

        while (c.sqlite3_step(stmt) == SQLITE_ROW) {
            const key = colText(stmt, 0);
            const addr = colText(stmt, 1);
            const peers = colText(stmt, 2);

            if (addr.len != 0) {
                try addAddrKey(alloc, &address_to_keys, addr, key);
            }
            if (peers.len != 0) {
                const parsed = std.json.parseFromSlice(std.json.Value, alloc, peers, .{}) catch continue;
                defer parsed.deinit();
                if (parsed.value == .object) {
                    var it = parsed.value.object.iterator();
                    while (it.next()) |e| {
                        try addAddrKey(alloc, &address_to_keys, e.key_ptr.*, key);
                    }
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

    var ctx = ScanCtx{
        .alloc = alloc,
        .tasks = tasks,
        .n = tasks.len,
        .port = port,
        .results = results,
    };
    runPool(ScanCtx, alloc, workers, &ctx, scanWorker);

    // store the results
    const upd = "UPDATE yggcrawl SET port_status = ?, port_checked = ? WHERE key = ?";
    var stmt: ?*c.sqlite3_stmt = null;
    if (c.sqlite3_prepare_v2(db, upd.ptr, @intCast(upd.len), &stmt, null) != SQLITE_OK) return error.Prepare;
    defer _ = c.sqlite3_finalize(stmt);

    try exec(db, "BEGIN");
    for (tasks, 0..) |t, i| {
        const status: []const u8 = if (results[i]) "open" else "closed";
        for (t.keys) |k| {
            _ = c.sqlite3_reset(stmt);
            _ = c.sqlite3_bind_text(stmt, 1, status.ptr, @intCast(status.len), SQLITE_TRANSIENT);
            _ = c.sqlite3_bind_int64(stmt, 2, port);
            _ = c.sqlite3_bind_text(stmt, 3, k.ptr, @intCast(k.len), SQLITE_TRANSIENT);
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

fn exportToJSON(alloc: Allocator, db: *c.sqlite3, filename: []const u8) !void {
    var arena_state = std.heap.ArenaAllocator.init(alloc);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    var kvs = std.ArrayList(std.json.Value).empty;

    {
        const sql = "SELECT address, key, nodeinfo, peers, port_status, port_checked FROM yggcrawl";
        var stmt: ?*c.sqlite3_stmt = null;
        if (c.sqlite3_prepare_v2(db, sql.ptr, @intCast(sql.len), &stmt, null) != SQLITE_OK) return error.Prepare;
        defer _ = c.sqlite3_finalize(stmt);

        while (c.sqlite3_step(stmt) == SQLITE_ROW) {
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

    try writeFileAtomic(alloc, filename, text);
}

fn writeFileAtomic(alloc: Allocator, filename: []const u8, content: []const u8) !void {
    const tmp = try std.fmt.allocPrint(alloc, "{s}.tmp", .{filename});
    defer alloc.free(tmp);
    const tmp_z = try alloc.dupeSentinel(u8, tmp, 0);
    defer alloc.free(tmp_z);
    const fn_z = try alloc.dupeSentinel(u8, filename, 0);
    defer alloc.free(fn_z);

    const f = c.fopen(tmp_z.ptr, "wb");
    if (f == null) return error.FileOpen;
    _ = c.fwrite(content.ptr, 1, content.len, f);
    _ = c.fwrite("\n", 1, 1, f);
    _ = c.fclose(f);

    if (c.rename(tmp_z.ptr, fn_z.ptr) != 0) return error.FileRename;
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
    if (c.sqlite3_prepare_v2(db, sql.ptr, @intCast(sql.len), &stmt, null) != SQLITE_OK) return error.Prepare;
    defer _ = c.sqlite3_finalize(stmt);
    while (c.sqlite3_step(stmt) == SQLITE_ROW) {
        const s = colText(stmt, 0);
        try out.append(alloc, try alloc.dupe(u8, s));
    }
    return out;
}
