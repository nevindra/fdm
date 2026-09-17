//! The worker: every download runs here, on a thread of its own with its own
//! `std.Io.Threaded`, and the rest of the program talks to it through two
//! queues — commands in, events out.
//!
//! **That seam is the whole design.** A Native SDK app is Elm-shaped: a
//! worker thread the app owns posts bytes through `fx.openChannel` and they
//! arrive as `Msg`s in `update`. A TUI is a loop that drains a queue and
//! redraws. Both are ~50 lines over this file, and neither reaches into it:
//! nothing here knows what a terminal or a window is.
//!
//! What the spike settled, and this file keeps
//! ([README](../README.md#what-was-found)):
//!
//! - N `Range` requests through one `fetch.Client`, each writing its slice
//!   at its own offset through a positional `File.Writer`.
//! - A 200 where a 206 was asked for is refused before a byte lands.
//! - A segment that stops moving is cancelled from outside with
//!   `Future.cancel`, which `std.Io.Threaded` delivers as a signal that
//!   interrupts the blocking `recv`, and resumed from the last byte that
//!   reached the file — `fw.pos`, never what entered the writer's buffer.

const std = @import("std");
const fetch = @import("nilo_fetch");

const Io = std.Io;

/// Text that travels in an event without an owner: copied in, fixed size,
/// truncated if it must be. A URL or a message, never a body.
pub const Text = struct {
    buf: [max]u8 = undefined,
    len: usize = 0,

    pub const max = 256;

    pub fn from(s: []const u8) Text {
        var t: Text = .{};
        t.len = @min(s.len, max);
        @memcpy(t.buf[0..t.len], s[0..t.len]);
        return t;
    }

    pub fn fmt(comptime f: []const u8, args: anytype) Text {
        var t: Text = .{};
        const s = std.fmt.bufPrint(&t.buf, f, args) catch t.buf[0..];
        t.len = s.len;
        return t;
    }

    pub fn slice(t: *const Text) []const u8 {
        return t.buf[0..t.len];
    }
};

pub const Settings = struct {
    segments: u8 = 4,
    /// Milliseconds a segment may go without a byte before it is cancelled.
    stall_ms: u32 = 10_000,
    /// How many times one segment may be restarted after a stall or an error.
    retries: u8 = 3,
    /// Below this, splitting costs more handshakes than it saves.
    min_segment: u64 = 1 << 20,
};

/// What the UI asks for. `add` hands over its strings: they are allocated
/// with the worker's allocator by the caller and freed by the worker when
/// the download is gone.
pub const Command = union(enum) {
    add: struct { id: u32, url: []const u8, out: ?[]const u8 },
    cancel: u32,
    quit,
};

/// What the worker reports. One `started` per download, `progress` while
/// it moves, `note` for anything a person would want to see, and exactly
/// one of `done` / `failed` / `cancelled` at the end.
pub const Event = union(enum) {
    started: struct { id: u32, name: Text, total: ?u64, segments: u8 },
    progress: struct { id: u32, bytes: u64 },
    note: struct { id: u32, text: Text },
    done: struct { id: u32, bytes: u64, elapsed_ms: i64 },
    failed: struct { id: u32, text: Text },
    cancelled: struct { id: u32 },
};

/// A lock that needs no `Io`. Zig 0.16's `std.Io.Mutex.lock` takes one and
/// the UI thread has none, so this spins — which is fine only because
/// nothing under it waits: every critical section here is one append or
/// one swap of a list. The same rule `nilo_cache` keeps for the same reason.
const Lock = struct {
    held: std.atomic.Value(bool) = .init(false),

    fn lock(l: *Lock) void {
        while (l.held.cmpxchgWeak(false, true, .acquire, .monotonic) != null) std.atomic.spinLoopHint();
    }

    fn unlock(l: *Lock) void {
        l.held.store(false, .release);
    }
};

pub const Worker = struct {
    gpa: std.mem.Allocator,
    settings: Settings,
    mutex: Lock = .{},
    commands: std.ArrayList(Command) = .empty,
    events: std.ArrayList(Event) = .empty,
    thread: std.Thread = undefined,

    /// `gpa` has to be thread-safe: the UI allocates command strings with
    /// it and the worker frees them.
    pub fn start(gpa: std.mem.Allocator, settings: Settings) !*Worker {
        const w = try gpa.create(Worker);
        w.* = .{ .gpa = gpa, .settings = settings };
        w.thread = try std.Thread.spawn(.{}, threadMain, .{w});
        return w;
    }

    /// Ask the worker to finish — every download is cancelled — and wait.
    pub fn stop(w: *Worker) void {
        w.send(.quit) catch {};
        w.thread.join();
        for (w.commands.items) |c| w.freeCommand(c);
        w.commands.deinit(w.gpa);
        w.events.deinit(w.gpa);
        const gpa = w.gpa;
        gpa.destroy(w);
    }

    pub fn send(w: *Worker, cmd: Command) !void {
        w.mutex.lock();
        defer w.mutex.unlock();
        try w.commands.append(w.gpa, cmd);
    }

    /// Everything reported since the last call. The caller owns the slice
    /// and frees it with the worker's allocator.
    pub fn take(w: *Worker) ![]Event {
        w.mutex.lock();
        defer w.mutex.unlock();
        return w.events.toOwnedSlice(w.gpa);
    }

    fn post(w: *Worker, ev: Event) void {
        w.mutex.lock();
        defer w.mutex.unlock();
        w.events.append(w.gpa, ev) catch {};
    }

    fn takeCommands(w: *Worker) []Command {
        w.mutex.lock();
        defer w.mutex.unlock();
        return w.commands.toOwnedSlice(w.gpa) catch &.{};
    }

    fn freeCommand(w: *Worker, c: Command) void {
        switch (c) {
            .add => |a| {
                w.gpa.free(a.url);
                if (a.out) |o| w.gpa.free(o);
            },
            else => {},
        }
    }
};

// ----------------------------------------------------------------- thread

fn threadMain(w: *Worker) void {
    const gpa = w.gpa;

    // The same Io a Native SDK worker thread makes for itself.
    var threaded: Io.Threaded = .init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var client: fetch.Client = .init(gpa, .{
        .max_in_flight = 32,
        // The deadline cannot fire without an Engine, so zero is honest; the
        // stall watchdog in `Download.run` is the bound instead.
        .timeout_ms = 0,
        // A refused body is dropped rather than drained: the probe leaves a
        // whole file unread when the server ignores its Range.
        .max_drain = 4 << 10,
    });
    defer client.deinit();
    client.nilo_start(io, .off) catch return;

    var downloads: std.ArrayList(*Download) = .empty;
    defer downloads.deinit(gpa);
    var quitting = false;

    while (true) {
        const cmds = w.takeCommands();
        defer gpa.free(cmds);
        for (cmds) |cmd| switch (cmd) {
            .add => |a| {
                const d = Download.create(gpa, w, &client, io, a.id, a.url, a.out) catch {
                    w.freeCommand(cmd);
                    w.post(.{ .failed = .{ .id = a.id, .text = .from("out of memory") } });
                    continue;
                };
                d.future = io.concurrent(Download.run, .{d}) catch {
                    d.destroy();
                    w.post(.{ .failed = .{ .id = a.id, .text = .from("no thread for it") } });
                    continue;
                };
                downloads.append(gpa, d) catch {
                    d.cancel.store(true, .release);
                    d.future.?.await(io);
                    d.destroy();
                    w.post(.{ .failed = .{ .id = a.id, .text = .from("out of memory") } });
                    continue;
                };
            },
            .cancel => |id| for (downloads.items) |d| {
                if (d.id == id) d.cancel.store(true, .release);
            },
            .quit => {
                quitting = true;
                for (downloads.items) |d| d.cancel.store(true, .release);
            },
        };

        // Reap what has finished. The task sets `finished` last, so the
        // await here never waits.
        var i: usize = 0;
        while (i < downloads.items.len) {
            const d = downloads.items[i];
            if (d.finished.load(.acquire)) {
                d.future.?.await(io);
                d.destroy();
                _ = downloads.swapRemove(i);
            } else i += 1;
        }

        if (quitting and downloads.items.len == 0) return;
        Io.sleep(io, Io.Duration.fromMilliseconds(100), .awake) catch return;
    }
}

// --------------------------------------------------------------- download

const Download = struct {
    gpa: std.mem.Allocator,
    worker: *Worker,
    client: *fetch.Client,
    io: Io,
    id: u32,
    url: []const u8,
    out: ?[]const u8,
    cancel: std.atomic.Value(bool) = .init(false),
    finished: std.atomic.Value(bool) = .init(false),
    future: ?Io.Future(void) = null,
    /// A reason better than the error's name, when the code that failed
    /// had one — an HTTP status, say. Read once, by `run`.
    why: ?Text = null,

    fn create(gpa: std.mem.Allocator, w: *Worker, client: *fetch.Client, io: Io, id: u32, url: []const u8, out: ?[]const u8) !*Download {
        const d = try gpa.create(Download);
        d.* = .{ .gpa = gpa, .worker = w, .client = client, .io = io, .id = id, .url = url, .out = out };
        return d;
    }

    fn destroy(d: *Download) void {
        const gpa = d.gpa;
        gpa.free(d.url);
        if (d.out) |o| gpa.free(o);
        gpa.destroy(d);
    }

    fn run(d: *Download) void {
        defer d.finished.store(true, .release);
        d.runInner() catch |err| {
            if (err == error.Cancelled) {
                d.worker.post(.{ .cancelled = .{ .id = d.id } });
            } else {
                d.worker.post(.{ .failed = .{ .id = d.id, .text = d.why orelse .fmt("{t}", .{err}) } });
            }
        };
    }

    fn runInner(d: *Download) !void {
        const io = d.io;
        const w = d.worker;
        const settings = w.settings;

        const info = try probe(d);
        const out_name = d.out orelse nameFromUrl(d.url);

        // Split only when the server can serve slices and the file is big
        // enough for a slice to be worth its handshake.
        const total = info.total;
        const count: usize = if (info.ranged and total != null and total.? >= settings.min_segment * 2)
            @intCast(@min(@as(u64, settings.segments), total.? / settings.min_segment))
        else
            1;
        const ranged = info.ranged and count > 1;

        w.post(.{ .started = .{ .id = d.id, .name = .from(out_name), .total = total, .segments = @intCast(count) } });

        const file = try Io.Dir.cwd().createFile(io, out_name, .{ .truncate = true });
        defer file.close(io);
        if (total) |n| try file.setLength(io, n);

        const segments = try d.gpa.alloc(Segment, count);
        defer d.gpa.free(segments);
        for (segments, 0..) |*s, i| {
            const per = if (total) |n| n / count else std.math.maxInt(u64);
            s.* = .{
                .index = i,
                .start = per * i,
                .end = if (i + 1 == count) (total orelse std.math.maxInt(u64)) else per * (i + 1),
            };
        }

        const started = nowMs(io);
        try d.supervise(file, segments, ranged);

        var bytes: u64 = 0;
        for (segments) |*s| bytes += s.done.load(.monotonic);
        if (total) |n| if (bytes != n) return error.ShortDownload;
        w.post(.{ .done = .{ .id = d.id, .bytes = bytes, .elapsed_ms = nowMs(io) - started } });
    }

    /// Starts every segment, restarts the ones that fail, and cancels the
    /// ones that stop moving — which is the one thing a deadline would do
    /// and, without an Engine, nothing else here does.
    fn supervise(d: *Download, file: Io.File, segments: []Segment, ranged: bool) !void {
        const io = d.io;
        const w = d.worker;
        const settings = w.settings;
        var last_reported: u64 = std.math.maxInt(u64);

        // **No task outlives this frame.** `segments` is freed by the
        // caller the moment this returns, and a segment task writes into
        // it; so every way out — done, cancelled, out of retries, a spawn
        // that failed — cancels whatever is still running and waits for
        // it. On a future that already finished, `cancel` is an `await`
        // that returns at once.
        defer for (segments) |*seg| if (seg.future) |*f| {
            f.cancel(io);
            seg.future = null;
        };

        while (true) {
            const now = nowMs(io);
            var pending: usize = 0;
            var bytes: u64 = 0;

            if (d.cancel.load(.acquire)) return error.Cancelled;

            for (segments) |*seg| {
                bytes += seg.done.load(.monotonic);
                switch (seg.state.load(.acquire)) {
                    .ok => if (seg.future) |*f| {
                        f.await(io); // finished already: returns at once
                        seg.future = null;
                    },
                    .idle, .failed => {
                        if (seg.future) |*f| {
                            f.await(io);
                            seg.future = null;
                        }
                        if (seg.state.load(.acquire) == .failed) {
                            w.post(.{ .note = .{ .id = d.id, .text = .fmt("segment {d}: attempt {d} failed: {t}", .{ seg.index, seg.attempts, seg.err }) } });
                        }
                        if (seg.attempts > settings.retries) return seg.err;
                        seg.attempts += 1;
                        seg.last_seen = seg.done.load(.monotonic);
                        seg.last_moved_ms = now;
                        seg.state.store(.running, .release);
                        seg.future = try io.concurrent(Segment.run, .{ seg, d.client, file, io, d.url, ranged });
                        pending += 1;
                    },
                    .running => {
                        pending += 1;
                        const seen = seg.done.load(.monotonic);
                        if (seen != seg.last_seen) {
                            seg.last_seen = seen;
                            seg.last_moved_ms = now;
                        } else if (now - seg.last_moved_ms > settings.stall_ms) {
                            seg.future.?.cancel(io);
                            seg.future = null;
                            w.post(.{ .note = .{ .id = d.id, .text = .fmt("segment {d}: no bytes for {d}ms, cancelled and retrying", .{ seg.index, now - seg.last_moved_ms }) } });
                            seg.state.store(.idle, .release);
                        }
                    },
                }
            }

            if (pending == 0) return;
            if (bytes != last_reported) {
                last_reported = bytes;
                w.post(.{ .progress = .{ .id = d.id, .bytes = bytes } });
            }
            try Io.sleep(io, Io.Duration.fromMilliseconds(100), .awake);
        }
    }
};

// ------------------------------------------------------------------ probe

const Probe = struct { total: ?u64, ranged: bool };

/// One byte, asked for with a Range. A 206 says the server can slice and
/// how big the whole is; a 200 says it cannot, and the body it started
/// sending is dropped rather than drained.
fn probe(d: *Download) !Probe {
    var transfer: [4096]u8 = undefined;
    var redirect: [2048]u8 = undefined;
    var ex: fetch.Exchange = .idle;
    defer ex.end();

    const head = try ex.begin(d.client, .{
        .method = .GET,
        .url = d.url,
        .headers = &.{.{ .name = "range", .value = "bytes=0-0" }},
        .redirect_buffer = &redirect,
        .transfer_buffer = &transfer,
    });
    switch (head.status) {
        .partial_content => {
            const cr = head.header("content-range") orelse return error.NoContentRange;
            return .{ .total = try totalFromContentRange(cr), .ranged = true };
        },
        .ok => return .{ .total = head.content_length, .ranged = false },
        else => {
            d.why = .fmt("HTTP {d} {s}", .{ @intFromEnum(head.status), head.status.phrase() orelse "" });
            return error.BadStatus;
        },
    }
}

/// `bytes 0-0/12345` → 12345; `bytes 0-0/*` → unknown.
fn totalFromContentRange(value: []const u8) !?u64 {
    const slash = std.mem.lastIndexOfScalar(u8, value, '/') orelse return error.BadContentRange;
    const tail = std.mem.trim(u8, value[slash + 1 ..], " ");
    if (std.mem.eql(u8, tail, "*")) return null;
    return try std.fmt.parseInt(u64, tail, 10);
}

// --------------------------------------------------------------- segments

const Segment = struct {
    index: usize,
    start: u64,
    /// Exclusive. `maxInt` when the length is unknown, which is also the
    /// unranged case.
    end: u64,
    /// Bytes that have reached the file, counted from `start`. The task
    /// stores; the supervisor reads.
    done: std.atomic.Value(u64) = .init(0),
    /// `.running` until the task writes its verdict, which it does before
    /// returning so the supervisor can `await` without ever blocking.
    state: std.atomic.Value(State) = .init(.idle),
    err: anyerror = error.None,
    attempts: u8 = 0,
    future: ?Io.Future(void) = null,
    last_seen: u64 = 0,
    last_moved_ms: i64 = 0,

    const State = enum(u8) { idle, running, ok, failed };

    fn len(s: *const Segment) ?u64 {
        return if (s.end == std.math.maxInt(u64)) null else s.end - s.start;
    }

    /// The task: one Range request for what this segment still lacks,
    /// written at its offset. Its verdict goes into the segment, not the
    /// return value, so that the supervisor can see it without blocking.
    fn run(seg: *Segment, client: *fetch.Client, file: Io.File, io: Io, url: []const u8, ranged: bool) void {
        seg.state.store(.running, .release);
        runInner(seg, client, file, io, url, ranged) catch |err| {
            seg.err = err;
            seg.state.store(.failed, .release);
            return;
        };
        seg.state.store(.ok, .release);
    }

    fn runInner(seg: *Segment, client: *fetch.Client, file: Io.File, io: Io, url: []const u8, ranged: bool) !void {
        // Both buffers live on this task's stack for the life of the
        // transfer. The writer's buffer is not optional: `std.Io.net`'s
        // stream writes straight into it and asserts on an empty one.
        var transfer: [64 << 10]u8 = undefined;
        var wbuf: [64 << 10]u8 = undefined;
        var redirect: [2048]u8 = undefined;
        var range_buf: [64]u8 = undefined;

        const from = seg.start + seg.done.load(.monotonic);
        const range = try std.fmt.bufPrint(&range_buf, "bytes={d}-{d}", .{ from, seg.end -| 1 });
        const headers: []const std.http.Header = if (ranged) &.{.{ .name = "range", .value = range }} else &.{};

        var ex: fetch.Exchange = .idle;
        defer ex.end();
        const head = try ex.begin(client, .{
            .method = .GET,
            .url = url,
            .headers = headers,
            .redirect_buffer = &redirect,
            .transfer_buffer = &transfer,
        });

        if (ranged) {
            // A 200 here is the whole file; writing it at `from` would
            // corrupt everything after it. Refuse before a byte lands.
            if (head.status != .partial_content) return error.RangeIgnored;
            if (head.content_length) |n| if (n != seg.end - from) return error.LengthMismatch;
        } else if (head.status != .ok) return error.BadStatus;

        var fw = file.writer(io, &wbuf);
        fw.pos = from;

        // `done` is `fw.pos`, which only moves when a positional write has
        // returned — never what `stream` said it consumed. On every way
        // out, what is buffered is flushed if it can be, and what is in
        // the file is what the next attempt resumes from.
        defer {
            fw.interface.flush() catch {};
            seg.done.store(fw.pos - seg.start, .monotonic);
        }
        const reader = ex.reader orelse unreachable;
        while (true) {
            _ = reader.stream(&fw.interface, .unlimited) catch |err| switch (err) {
                error.EndOfStream => break,
                else => return err,
            };
            seg.done.store(fw.pos - seg.start, .monotonic);
        }
        try fw.interface.flush();
        seg.done.store(fw.pos - seg.start, .monotonic);

        if (seg.len()) |want| if (fw.pos - seg.start != want) return error.ShortBody;
    }
};

// ------------------------------------------------------------------ misc

pub fn nowMs(io: Io) i64 {
    return @intCast(@divTrunc(Io.Timestamp.now(io, .awake).nanoseconds, std.time.ns_per_ms));
}

pub fn nameFromUrl(url: []const u8) []const u8 {
    const no_query = url[0 .. std.mem.indexOfScalar(u8, url, '?') orelse url.len];
    const slash = std.mem.lastIndexOfScalar(u8, no_query, '/') orelse return "download";
    const name = no_query[slash + 1 ..];
    return if (name.len == 0) "download" else name;
}

test "the name is the last path segment without the query" {
    try std.testing.expectEqualStrings("a.bin", nameFromUrl("https://x.y/p/a.bin?sig=1"));
    try std.testing.expectEqualStrings("download", nameFromUrl("https://x.y/"));
}

test "a content-range total is parsed, and a star is unknown" {
    try std.testing.expectEqual(@as(?u64, 12345), try totalFromContentRange("bytes 0-0/12345"));
    try std.testing.expectEqual(@as(?u64, null), try totalFromContentRange("bytes 0-0/*"));
}
