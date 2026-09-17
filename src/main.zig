//! fdm — fast download manager.
//!
//! ```
//! fdm [url ...] [-o path] [-H header] [--dir path] [--batch file] [--force]
//!     [-p parallel] [-n segments] [--stall ms] [--retries n] [--db file]
//!     [--headless] [--json]
//! fdm ls [--json] [--db file]
//! ```
//!
//! `--headless` runs without the terminal front: one line per event on
//! stderr, exit when every download given on the command line has
//! finished — what a script, a cron job, or the benchmark wants. `--json`
//! makes each of those lines an object, and `fdm ls` prints the list the
//! same two ways without starting anything.
//!
//! Starts the worker on its own thread, hands it any URLs on the command
//! line, and runs the terminal front until `q`. `a` adds another URL while
//! it runs. The list lives in `--db`, which defaults to
//! `$XDG_DATA_HOME/fdm/fdm.db` — so the next run shows the same list and
//! carries on with whatever was unfinished. The worker is `download.zig`;
//! the front is `tui.zig`; this file only wires them, and the Native SDK
//! window will be wired the same way.

const std = @import("std");
const download = @import("download.zig");
const curl = @import("curl.zig");
const store = @import("store.zig");
const tui = @import("tui.zig");
const update = @import("update.zig");
const builtin = @import("builtin");

/// Where `std.log` goes: a file beside the database, never the terminal
/// the TUI owns. `nilo_job` logs a line per retry and per dead row, and a
/// line on stderr in raw mode is a corrupted screen.
pub const std_options: std.Options = .{ .logFn = logToFile };

var log_io: std.Io = undefined;
var log_file: ?std.Io.File = null;
/// Where the next line goes. Positional writes from an atomic offset are
/// what make this safe from any thread without a lock.
var log_pos: std.atomic.Value(u64) = .init(0);

fn logToFile(comptime level: std.log.Level, comptime scope: @EnumLiteral(), comptime format: []const u8, args: anytype) void {
    const file = log_file orelse return;
    var buf: [1024]u8 = undefined;
    const text = std.fmt.bufPrint(&buf, "{s} ({s}): " ++ format ++ "\n", .{ level.asText(), @tagName(scope) } ++ args) catch return;
    const at = log_pos.fetchAdd(text.len, .monotonic);
    file.writePositionalAll(log_io, text, at) catch {};
}

fn openLog(io: std.Io, arena: std.mem.Allocator, db_path: []const u8) void {
    const path = std.fmt.allocPrint(arena, "{s}.log", .{db_path}) catch return;
    const file = std.Io.Dir.cwd().createFile(io, path, .{ .truncate = false }) catch return;
    log_pos.store(file.length(io) catch 0, .monotonic);
    log_io = io;
    log_file = file;
}

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const args = try init.minimal.args.toSlice(init.arena.allocator());

    const arena = init.arena.allocator();

    var settings: download.Settings = .{};
    var adds: std.ArrayList(download.Add) = .empty;
    var outs: std.ArrayList([]const u8) = .empty;
    var headers: std.ArrayList([]const u8) = .empty;
    var db_path: ?[]const u8 = null;
    var headless = false;
    var json = false;
    var force = false;
    var listing = false;

    if (args.len > 1 and std.mem.eql(u8, args[1], "--version")) {
        std.debug.print("fdm {s}\n", .{update.version});
        return;
    }
    if (args.len > 1 and std.mem.eql(u8, args[1], "update")) return update.run(gpa, init.io);

    var i: usize = 1;
    if (args.len > 1 and std.mem.eql(u8, args[1], "ls")) {
        listing = true;
        i = 2;
    }
    while (i < args.len) : (i += 1) {
        const a = args[i];
        if (isOne(a, &.{ "-n", "-p", "--stall", "--retries", "--db", "-o", "-H", "--header", "--dir", "--batch" })) {
            if (i + 1 >= args.len) return usage();
            const v = args[i + 1];
            i += 1;
            if (std.mem.eql(u8, a, "-n")) {
                settings.segments = std.fmt.parseInt(u8, v, 10) catch return usage();
            } else if (std.mem.eql(u8, a, "-p")) {
                settings.parallel = std.fmt.parseInt(u8, v, 10) catch return usage();
            } else if (std.mem.eql(u8, a, "--stall")) {
                settings.stall_ms = std.fmt.parseInt(u32, v, 10) catch return usage();
            } else if (std.mem.eql(u8, a, "--retries")) {
                settings.retries = std.fmt.parseInt(u8, v, 10) catch return usage();
            } else if (std.mem.eql(u8, a, "-o")) {
                if (v.len == 0) return usage();
                try outs.append(arena, v);
            } else if (std.mem.eql(u8, a, "-H") or std.mem.eql(u8, a, "--header")) {
                if (std.mem.indexOfScalar(u8, v, ':') == null) return usage();
                try headers.append(arena, v);
            } else if (std.mem.eql(u8, a, "--dir")) {
                if (v.len == 0) return usage();
                settings.dir = v;
            } else if (std.mem.eql(u8, a, "--batch")) {
                try readBatch(arena, init.io, v, &adds);
            } else {
                db_path = v;
            }
        } else if (std.mem.eql(u8, a, "--headless")) {
            headless = true;
        } else if (std.mem.eql(u8, a, "--json")) {
            json = true;
        } else if (std.mem.eql(u8, a, "--force")) {
            force = true;
        } else if (a.len > 0 and a[0] == '-') {
            return usage();
        } else try adds.append(arena, try curl.parse(arena, a));
    }
    if (settings.segments == 0 or settings.parallel == 0) return usage();
    // The n-th `-o` goes with the n-th URL, as curl pairs them; one more
    // `-o` than URLs is a mistake.
    if (outs.items.len > adds.items.len) return usage();
    for (adds.items, 0..) |*a, n| {
        if (n < outs.items.len) a.out = outs.items[n];
        if (headers.items.len > 0 and a.headers.len == 0) a.headers = headers.items;
        a.force = force;
    }

    const db = db_path orelse try defaultDbPath(arena, init.environ_map);
    if (listing) return list(arena, init.io, db, json);

    // The worker creates the directory; the log can only follow it.
    const worker = try download.Worker.start(gpa, settings, db);
    defer worker.stop();
    openLog(init.io, arena, db);

    if (headless) return runHeadless(gpa, init.io, worker, adds.items, json);
    try tui.run(gpa, init.io, init.environ_map, worker, adds.items);
}

fn isOne(word: []const u8, of: []const []const u8) bool {
    for (of) |o| if (std.mem.eql(u8, word, o)) return true;
    return false;
}

/// One URL — or one `curl` line — per line; blank lines and `#` comments
/// are skipped. A line ending in `\` continues on the next, so a pasted
/// `curl` command keeps the shape the browser gave it.
fn readBatch(arena: std.mem.Allocator, io: std.Io, path: []const u8, adds: *std.ArrayList(download.Add)) !void {
    const text = try std.Io.Dir.cwd().readFileAlloc(io, path, arena, .limited(16 << 20));
    var entry: std.ArrayList(u8) = .empty;
    var lines = std.mem.splitScalar(u8, text, '\n');
    while (lines.next()) |raw| {
        const line_text = std.mem.trimEnd(u8, raw, " \t\r");
        if (entry.items.len == 0) {
            const lead = std.mem.trimStart(u8, line_text, " \t");
            if (lead.len == 0 or lead[0] == '#') continue;
        }
        if (line_text.len > 0 and line_text[line_text.len - 1] == '\\') {
            try entry.appendSlice(arena, line_text[0 .. line_text.len - 1]);
            try entry.append(arena, ' ');
            continue;
        }
        try entry.appendSlice(arena, line_text);
        try adds.append(arena, try curl.parse(arena, entry.items));
        entry = .empty;
    }
    if (std.mem.trim(u8, entry.items, " \t").len > 0) try adds.append(arena, try curl.parse(arena, entry.items));
}

/// `fdm ls`: every row, as a line or as one JSON array, and nothing
/// started. The database is read the way the worker reads it; a worker
/// in another process is a concurrent reader SQLite allows.
fn list(arena: std.mem.Allocator, io: std.Io, db_path: []const u8, json: bool) !void {
    var db = try store.open(arena, io, db_path);
    defer db.deinit();
    var run: store.Run = .initIo(arena, io);
    defer run.deinit();
    const rows = try store.all(&db, &run);

    const Line = struct { id: i64, state: store.State, name: []const u8, path: []const u8, url: []const u8, total: ?i64, bytes: i64 };
    var lines: std.ArrayList(Line) = .empty;
    for (rows) |row| {
        var bytes: i64 = 0;
        if (row.state == .done) {
            bytes = row.total orelse 0;
        } else for (try store.segmentsOf(&db, &run, row.id)) |seg| bytes += seg.done;
        try lines.append(arena, .{ .id = row.id, .state = row.state, .name = row.name, .path = row.path, .url = row.url, .total = row.total, .bytes = bytes });
    }

    var buf: [4096]u8 = undefined;
    var out = std.Io.File.stdout().writer(io, &buf);
    const w = &out.interface;
    if (json) {
        try w.print("{f}\n", .{std.json.fmt(lines.items, .{})});
    } else for (lines.items) |l| {
        if (l.total) |t| {
            try w.print("{d}\t{s}\t{d}/{d}\t{s}\t{s}\n", .{ l.id, @tagName(l.state), l.bytes, t, l.name, l.path });
        } else {
            try w.print("{d}\t{s}\t{d}\t{s}\t{s}\n", .{ l.id, @tagName(l.state), l.bytes, l.name, l.path });
        }
    }
    try w.flush();
}

/// No terminal front: the URLs go in, events come out as lines, and the
/// process ends when the last of them is done or failed. Rows restored
/// from the database are reported but not waited for; a URL refused as a
/// duplicate counts as failed, so the exit says so.
fn runHeadless(gpa: std.mem.Allocator, io: std.Io, worker: *download.Worker, adds: []const download.Add, json: bool) !void {
    var waiting: std.ArrayList(i64) = .empty;
    defer waiting.deinit(gpa);
    var pending: usize = adds.len;
    for (adds) |a| try worker.send(.{ .add = try a.dupe(worker.gpa) });

    var failed = false;
    var last_line_ms: i64 = 0;
    while (pending > 0) {
        try std.Io.sleep(io, std.Io.Duration.fromMilliseconds(100), .awake);
        const now = download.nowMs(io);
        const events = try worker.take();
        defer gpa.free(events);
        for (events) |ev| switch (ev) {
            .added => |a| if (a.state == .queued and waiting.items.len < adds.len and isOurs(adds, a.url.slice())) {
                try waiting.append(gpa, a.id);
                line(json, .{ .event = "added", .id = a.id, .url = a.url.slice(), .path = a.path.slice() }, "{d}: {s} -> {s}", .{ a.id, a.url.slice(), a.path.slice() });
            },
            .started => |st| line(json, .{ .event = "started", .id = st.id, .total = st.total, .segments = st.segments, .resumed = st.resumed, .path = st.path.slice() }, "{d}: started, {?d} bytes, {d} segments{s}", .{ st.id, st.total, st.segments, if (st.resumed) ", resumed" else "" }),
            .progress => |p| if (now - last_line_ms >= 1000) {
                last_line_ms = now;
                line(json, .{ .event = "progress", .id = p.id, .bytes = p.bytes }, "{d}: {d} bytes", .{ p.id, p.bytes });
            },
            .note => |n| line(json, .{ .event = "note", .id = n.id, .text = n.text.slice() }, "{d}: {s}", .{ n.id, n.text.slice() }),
            .done => |d| {
                line(json, .{ .event = "done", .id = d.id, .bytes = d.bytes, .elapsed_ms = d.elapsed_ms }, "{d}: done, {d} bytes in {d} ms", .{ d.id, d.bytes, d.elapsed_ms });
                if (isWaited(waiting.items, d.id)) pending -= 1;
            },
            .failed => |f| {
                line(json, .{ .event = "failed", .id = f.id, .text = f.text.slice() }, "{d}: failed: {s}", .{ f.id, f.text.slice() });
                if (isWaited(waiting.items, f.id)) {
                    pending -= 1;
                    failed = true;
                }
            },
            .duplicate => |dup| {
                line(json, .{ .event = "duplicate", .of = dup.of, .url = dup.url.slice() }, "{s}: already in the list as {d}; --force adds it anyway", .{ dup.url.slice(), dup.of });
                pending -= 1;
                failed = true;
            },
            .fatal => |t| {
                line(json, .{ .event = "fatal", .text = t.slice() }, "fatal: {s}", .{t.slice()});
                return error.Fatal;
            },
            else => {},
        };
    }
    if (failed) return error.DownloadFailed;
}

/// One event on stderr: the object when `--json`, the sentence otherwise.
fn line(json: bool, object: anytype, comptime format: []const u8, args: anytype) void {
    if (json) {
        std.debug.print("{f}\n", .{std.json.fmt(object, .{})});
    } else {
        std.debug.print(format ++ "\n", args);
    }
}

fn isOurs(adds: []const download.Add, url: []const u8) bool {
    for (adds) |a| if (std.mem.eql(u8, a.url, url)) return true;
    return false;
}

fn isWaited(ids: []const i64, id: i64) bool {
    for (ids) |i| if (i == id) return true;
    return false;
}

/// `$XDG_DATA_HOME/fdm/fdm.db` wherever that is set; otherwise where the
/// platform keeps a program's data — `~/.local/share`, `~/Library/Application
/// Support`, `%LOCALAPPDATA%`.
fn defaultDbPath(arena: std.mem.Allocator, env: *std.process.Environ.Map) ![]const u8 {
    if (env.get("XDG_DATA_HOME")) |xdg| return std.fs.path.join(arena, &.{ xdg, "fdm", "fdm.db" });
    switch (builtin.os.tag) {
        .windows => {
            const base = env.get("LOCALAPPDATA") orelse env.get("APPDATA") orelse return error.NoHome;
            return std.fs.path.join(arena, &.{ base, "fdm", "fdm.db" });
        },
        .macos => {
            const home = env.get("HOME") orelse return error.NoHome;
            return std.fs.path.join(arena, &.{ home, "Library", "Application Support", "fdm", "fdm.db" });
        },
        else => {
            const home = env.get("HOME") orelse return error.NoHome;
            return std.fs.path.join(arena, &.{ home, ".local", "share", "fdm", "fdm.db" });
        },
    }
}

fn usage() error{Usage} {
    std.debug.print(
        \\usage: fdm [url ...] [-o path] [-H header] [--dir path] [--batch file] [--force]
        \\           [-p parallel] [-n segments] [--stall ms] [--retries n] [--db file] [--headless] [--json]
        \\       fdm ls [--json] [--db file]
        \\       fdm update | fdm --version
        \\
    , .{});
    return error.Usage;
}

test {
    _ = download;
    _ = curl;
    _ = tui;
    _ = @import("store.zig");
    _ = @import("dns.zig");
    _ = update;
}
