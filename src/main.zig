//! fdm — fast download manager.
//!
//! ```
//! fdm [url ...] [-p parallel] [-n segments] [--stall ms] [--retries n] [--db file] [--headless]
//! ```
//!
//! `--headless` runs without the terminal front: one line per event on
//! stderr, exit when every download given on the command line has
//! finished — what a script, a cron job, or the benchmark wants.
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
const tui = @import("tui.zig");

/// Where `std.log` goes: a file beside the database, never the terminal
/// the TUI owns. `nilo_job` logs a line per retry and per dead row, and a
/// line on stderr in raw mode is a corrupted screen.
pub const std_options: std.Options = .{ .logFn = logToFile };

var log_fd: ?std.os.linux.fd_t = null;

fn logToFile(comptime level: std.log.Level, comptime scope: @EnumLiteral(), comptime format: []const u8, args: anytype) void {
    const fd = log_fd orelse return;
    var buf: [1024]u8 = undefined;
    const line = std.fmt.bufPrint(&buf, "{s} ({s}): " ++ format ++ "\n", .{ level.asText(), @tagName(scope) } ++ args) catch return;
    var rest: []const u8 = line;
    while (rest.len > 0) {
        const rc = std.os.linux.write(fd, rest.ptr, rest.len);
        if (std.os.linux.errno(rc) != .SUCCESS) return;
        rest = rest[rc..];
    }
}

fn openLog(arena: std.mem.Allocator, db_path: []const u8) void {
    const path = std.fmt.allocPrintSentinel(arena, "{s}.log", .{db_path}, 0) catch return;
    const rc = std.os.linux.open(path, .{ .ACCMODE = .WRONLY, .CREAT = true, .APPEND = true }, 0o644);
    if (std.os.linux.errno(rc) == .SUCCESS) log_fd = @intCast(rc);
}

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const args = try init.minimal.args.toSlice(init.arena.allocator());

    var settings: download.Settings = .{};
    var urls: std.ArrayList([]const u8) = .empty;
    defer urls.deinit(gpa);
    var db_path: ?[]const u8 = null;
    var headless = false;

    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        const a = args[i];
        if (std.mem.eql(u8, a, "-n") or std.mem.eql(u8, a, "-p") or std.mem.eql(u8, a, "--stall") or std.mem.eql(u8, a, "--retries") or std.mem.eql(u8, a, "--db")) {
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
            } else {
                db_path = v;
            }
        } else if (std.mem.eql(u8, a, "--headless")) {
            headless = true;
        } else if (a.len > 0 and a[0] == '-') {
            return usage();
        } else try urls.append(gpa, a);
    }
    if (settings.segments == 0 or settings.parallel == 0) return usage();

    const arena = init.arena.allocator();
    const db = db_path orelse try defaultDbPath(arena, init.environ_map);

    // The worker creates the directory; the log can only follow it.
    const worker = try download.Worker.start(gpa, settings, db);
    defer worker.stop();
    openLog(arena, db);

    if (headless) return runHeadless(gpa, init.io, worker, urls.items);
    try tui.run(gpa, init.io, init.environ_map, worker, urls.items);
}

/// No terminal front: the URLs go in, events come out as lines, and the
/// process ends when the last of them is done or failed. Rows restored
/// from the database are reported but not waited for.
fn runHeadless(gpa: std.mem.Allocator, io: std.Io, worker: *download.Worker, urls: []const []const u8) !void {
    var waiting: std.ArrayList(i64) = .empty;
    defer waiting.deinit(gpa);
    var pending: usize = urls.len;
    for (urls) |u| try worker.send(.{ .add = try worker.gpa.dupe(u8, u) });

    var failed = false;
    var last_line_ms: i64 = 0;
    while (pending > 0) {
        try std.Io.sleep(io, std.Io.Duration.fromMilliseconds(100), .awake);
        const now = download.nowMs(io);
        const events = try worker.take();
        defer gpa.free(events);
        for (events) |ev| switch (ev) {
            .added => |a| if (a.state == .queued and waiting.items.len < urls.len) {
                try waiting.append(gpa, a.id);
                std.debug.print("{d}: {s} -> {s}\n", .{ a.id, a.url.slice(), a.path.slice() });
            },
            .started => |st| std.debug.print("{d}: started, {?d} bytes, {d} segments{s}\n", .{ st.id, st.total, st.segments, if (st.resumed) ", resumed" else "" }),
            .progress => |p| if (now - last_line_ms >= 1000) {
                last_line_ms = now;
                std.debug.print("{d}: {d} bytes\n", .{ p.id, p.bytes });
            },
            .note => |n| std.debug.print("{d}: {s}\n", .{ n.id, n.text.slice() }),
            .done => |d| {
                std.debug.print("{d}: done, {d} bytes in {d} ms\n", .{ d.id, d.bytes, d.elapsed_ms });
                if (isWaited(waiting.items, d.id)) pending -= 1;
            },
            .failed => |f| {
                std.debug.print("{d}: failed: {s}\n", .{ f.id, f.text.slice() });
                if (isWaited(waiting.items, f.id)) {
                    pending -= 1;
                    failed = true;
                }
            },
            .fatal => |t| {
                std.debug.print("fatal: {s}\n", .{t.slice()});
                return error.Fatal;
            },
            else => {},
        };
    }
    if (failed) return error.DownloadFailed;
}

fn isWaited(ids: []const i64, id: i64) bool {
    for (ids) |i| if (i == id) return true;
    return false;
}

/// `$XDG_DATA_HOME/fdm/fdm.db`, or `~/.local/share/fdm/fdm.db`.
fn defaultDbPath(arena: std.mem.Allocator, env: *std.process.Environ.Map) ![]const u8 {
    if (env.get("XDG_DATA_HOME")) |xdg| return std.fs.path.join(arena, &.{ xdg, "fdm", "fdm.db" });
    const home = env.get("HOME") orelse return error.NoHome;
    return std.fs.path.join(arena, &.{ home, ".local", "share", "fdm", "fdm.db" });
}

fn usage() error{Usage} {
    std.debug.print(
        \\usage: fdm [url ...] [-p parallel] [-n segments] [--stall ms] [--retries n] [--db file] [--headless]
        \\
    , .{});
    return error.Usage;
}

test {
    _ = download;
    _ = tui;
    _ = @import("store.zig");
}
