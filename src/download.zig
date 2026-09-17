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
//! **The database is the list.** Every download is a row in `store.zig`'s
//! SQLite file before it is anything else, its id is the row's, and the
//! segments' progress is written there once a second and on every way out.
//! So a restart shows the same list, and a download that was running when
//! the process died resumes from the last byte each segment had written —
//! after asking the server again and checking that its `ETag` and length
//! are what they were, because a file that changed underneath a resume is
//! a corrupt download that looks complete.
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
const store = @import("store.zig");

const Io = std.Io;

pub const State = store.State;

/// Text that travels in an event without an owner: copied in, fixed size,
/// truncated if it must be. A URL or a message, never a body.
pub const Text = struct {
    buf: [max]u8 = undefined,
    len: usize = 0,

    pub const max = 512;

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

/// What the UI asks for. `add` hands over its string: allocated with the
/// worker's allocator by the caller and freed by the worker.
pub const Command = union(enum) {
    add: []const u8,
    cancel: i64,
    /// Run a failed or cancelled download again — from where it got to, if
    /// the server still has the same file.
    restart: i64,
    quit,
};

/// What the worker reports. One `added` per row — on `add`, and for every
/// row in the database when the worker starts — then `started`, `progress`
/// while it moves, `note` for anything a person would want to see, and
/// exactly one of `done` / `failed` / `cancelled` at the end of each run.
pub const Event = union(enum) {
    added: struct { id: i64, url: Text, name: Text, state: State, total: ?u64, bytes: u64, segments: u8 },
    started: struct { id: i64, name: Text, total: ?u64, segments: u8, resumed: bool },
    progress: struct { id: i64, bytes: u64 },
    note: struct { id: i64, text: Text },
    done: struct { id: i64, bytes: u64, elapsed_ms: i64 },
    failed: struct { id: i64, text: Text },
    cancelled: struct { id: i64 },
    /// The worker cannot run at all — the database would not open.
    fatal: Text,
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
    db_path: []const u8,
    mutex: Lock = .{},
    commands: std.ArrayList(Command) = .empty,
    events: std.ArrayList(Event) = .empty,
    thread: std.Thread = undefined,

    /// `gpa` has to be thread-safe: the UI allocates command strings with
    /// it and the worker frees them.
    pub fn start(gpa: std.mem.Allocator, settings: Settings, db_path: []const u8) !*Worker {
        const w = try gpa.create(Worker);
        errdefer gpa.destroy(w);
        w.* = .{ .gpa = gpa, .settings = settings, .db_path = try gpa.dupe(u8, db_path) };
        errdefer gpa.free(w.db_path);
        w.thread = try std.Thread.spawn(.{}, threadMain, .{w});
        return w;
    }

    /// Ask the worker to finish — every download is cancelled, and its
    /// progress written down — and wait.
    pub fn stop(w: *Worker) void {
        w.send(.quit) catch {};
        w.thread.join();
        for (w.commands.items) |c| w.freeCommand(c);
        w.commands.deinit(w.gpa);
        w.events.deinit(w.gpa);
        const gpa = w.gpa;
        gpa.free(w.db_path);
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
            .add => |url| w.gpa.free(url),
            else => {},
        }
    }
};

// ----------------------------------------------------------------- thread

/// Everything a download task needs that is shared: the worker, the client,
/// the database, the Io. One of these for the life of the thread.
const Shared = struct {
    worker: *Worker,
    client: *fetch.Client,
    db: *store.Db,
    io: Io,
};

fn threadMain(w: *Worker) void {
    const gpa = w.gpa;

    // The same Io a Native SDK worker thread makes for itself.
    var threaded: Io.Threaded = .init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var db = store.open(gpa, io, w.db_path) catch |err| {
        w.post(.{ .fatal = .fmt("cannot open {s}: {t}", .{ w.db_path, err }) });
        return;
    };
    defer db.deinit();

    var client: fetch.Client = .init(gpa, .{
        .max_in_flight = 32,
        // The deadline cannot fire without an Engine, so zero is honest; the
        // stall watchdog in `Download.supervise` is the bound instead.
        .timeout_ms = 0,
        // A refused body is dropped rather than drained: the probe leaves a
        // whole file unread when the server ignores its Range.
        .max_drain = 4 << 10,
    });
    defer client.deinit();
    client.nilo_start(io, .off) catch return;

    var shared: Shared = .{ .worker = w, .client = &client, .db = &db, .io = io };
    var run: store.Run = .initIo(gpa, io);
    defer run.deinit();

    var downloads: std.ArrayList(*Download) = .empty;
    defer downloads.deinit(gpa);

    // The list is whatever the database has. Anything that was on its way
    // when the process last stopped goes again.
    restore(&shared, &run, &downloads);

    var quitting = false;
    while (true) {
        const cmds = w.takeCommands();
        defer gpa.free(cmds);
        for (cmds) |cmd| switch (cmd) {
            .add => |url| {
                defer w.freeCommand(cmd);
                const id = insert(&shared, &run, url) catch |err| {
                    w.post(.{ .fatal = .fmt("cannot add {s}: {t}", .{ url, err }) });
                    continue;
                };
                launch(&shared, &downloads, id);
            },
            .restart => |id| launch(&shared, &downloads, id),
            .cancel => |id| for (downloads.items) |d| {
                if (d.id == id) d.cancel.store(true, .release);
            },
            .quit => {
                quitting = true;
                // Stopped, not cancelled: the row stays `running`, so the
                // next start picks it up where it was.
                for (downloads.items) |d| d.stop.store(true, .release);
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

/// Report every row, and start the ones that were not finished.
fn restore(s: *Shared, run: *store.Run, downloads: *std.ArrayList(*Download)) void {
    defer run.reset();
    const rows = store.all(s.db, run) catch |err| {
        s.worker.post(.{ .fatal = .fmt("cannot read the list: {t}", .{err}) });
        return;
    };
    for (rows) |row| {
        var bytes: u64 = 0;
        if (store.segmentsOf(s.db, run, row.id)) |segs| {
            for (segs) |seg| bytes += @intCast(seg.done);
        } else |_| {}
        s.worker.post(.{ .added = .{
            .id = row.id,
            .url = .from(row.url),
            .name = .from(row.name),
            .state = row.state,
            .total = if (row.total) |t| @intCast(t) else null,
            .bytes = bytes,
            .segments = @intCast(@min(row.segments, 255)),
        } });
        if (row.state == .queued or row.state == .running) launch(s, downloads, row.id);
    }
}

/// A new row for a URL, written down before anything is fetched.
fn insert(s: *Shared, run: *store.Run, url: []const u8) !i64 {
    defer run.reset();
    const name = nameFromUrl(url);
    var cwd_buf: [std.fs.max_path_bytes]u8 = undefined;
    const cwd_len = try std.process.currentPath(s.io, &cwd_buf);
    const path = try std.fs.path.join(run.arena(), &.{ cwd_buf[0..cwd_len], name });
    const row = try store.add(s.db, run, url, name, path, nowMs(s.io));
    s.worker.post(.{ .added = .{
        .id = row.id,
        .url = .from(url),
        .name = .from(name),
        .state = .queued,
        .total = null,
        .bytes = 0,
        .segments = 0,
    } });
    return row.id;
}

fn launch(s: *Shared, downloads: *std.ArrayList(*Download), id: i64) void {
    // One task per row at a time: a restart of something still running is
    // a no-op rather than a second writer on the same file.
    for (downloads.items) |d| if (d.id == id) return;
    const gpa = s.worker.gpa;
    const d = gpa.create(Download) catch return;
    d.* = .{ .shared = s, .id = id };
    d.future = s.io.concurrent(Download.run, .{d}) catch {
        gpa.destroy(d);
        s.worker.post(.{ .failed = .{ .id = id, .text = .from("no thread for it") } });
        return;
    };
    downloads.append(gpa, d) catch {
        d.cancel.store(true, .release);
        d.future.?.await(s.io);
        gpa.destroy(d);
        s.worker.post(.{ .failed = .{ .id = id, .text = .from("out of memory") } });
    };
}

// --------------------------------------------------------------- download

const Download = struct {
    shared: *Shared,
    id: i64,
    /// The person said stop: the row becomes `cancelled` and waits for `r`.
    cancel: std.atomic.Value(bool) = .init(false),
    /// The process is leaving: the row stays `running` and resumes next time.
    stop: std.atomic.Value(bool) = .init(false),
    finished: std.atomic.Value(bool) = .init(false),
    future: ?Io.Future(void) = null,
    /// A reason better than the error's name, when the code that failed
    /// had one — an HTTP status, say. Read once, by `run`.
    why: ?Text = null,

    fn destroy(d: *Download) void {
        d.shared.worker.gpa.destroy(d);
    }

    fn run(d: *Download) void {
        defer d.finished.store(true, .release);
        const gpa = d.shared.worker.gpa;
        var scope: store.Run = .initIo(gpa, d.shared.io);
        defer scope.deinit();

        d.runInner(&scope) catch |err| {
            scope.reset();
            if (err == error.Stopped) {
                // Progress is written; the state is left as it was.
            } else if (err == error.Cancelled) {
                store.setState(d.shared.db, &scope, d.id, .cancelled, null) catch {};
                d.shared.worker.post(.{ .cancelled = .{ .id = d.id } });
            } else {
                const why = d.why orelse Text.fmt("{t}", .{err});
                store.setState(d.shared.db, &scope, d.id, .failed, why.slice()) catch {};
                d.shared.worker.post(.{ .failed = .{ .id = d.id, .text = why } });
            }
        };
    }

    fn runInner(d: *Download, scope: *store.Run) !void {
        const s = d.shared;
        const gpa = s.worker.gpa;
        const io = s.io;
        const w = s.worker;
        const settings = w.settings;

        // The row is the truth about this download; copy what the task
        // needs, since the scope's arena is reset between statements.
        const row = (try s.db.find(store.Download, scope, d.id)) orelse return error.Gone;
        const url = try gpa.dupe(u8, row.url);
        defer gpa.free(url);
        const path = try gpa.dupe(u8, row.path);
        defer gpa.free(path);
        const name: Text = .from(row.name);
        const stored_total = row.total;
        const stored_etag: ?Text = if (row.etag) |e| .from(e) else null;

        const prior = try store.segmentsOf(s.db, scope, d.id);
        var segments: std.ArrayList(Segment) = .empty;
        defer segments.deinit(gpa);
        for (prior) |p| try segments.append(gpa, .{
            .row_id = p.id,
            .index = @intCast(p.idx),
            .start = @intCast(p.start),
            .end = @intCast(p.stop),
            .done = .init(@intCast(p.done)),
            .saved = @intCast(p.done),
        });
        scope.reset();

        try store.setState(s.db, scope, d.id, .running, null);
        scope.reset();

        const info = try probe(d, url);

        // Resume only when the server still has the same file and the one
        // on disk is the size the plan expects; otherwise plan afresh.
        const file = try Io.Dir.cwd().createFile(io, path, .{ .truncate = false, .read = true });
        defer file.close(io);
        const on_disk = try file.length(io);
        const same_object = optionalEql(stored_total, info.total) and
            optionalTextEql(stored_etag, info.etag) and
            (stored_etag != null or info.total != null);
        const resumable = segments.items.len > 0 and same_object and info.ranged and
            info.total != null and on_disk == info.total.?;

        const total = info.total;
        var ranged = false;
        if (resumable) {
            ranged = segments.items.len > 1;
        } else {
            // Split only when the server can serve slices and the file is
            // big enough for a slice to be worth its handshake.
            const count: usize = if (info.ranged and total != null and total.? >= settings.min_segment * 2)
                @intCast(@min(@as(u64, settings.segments), total.? / settings.min_segment))
            else
                1;
            ranged = info.ranged and count > 1;

            var starts: [256]i64 = undefined;
            var stops: [256]i64 = undefined;
            for (0..count) |i| {
                const per = if (total) |n| n / count else std.math.maxInt(u64);
                starts[i] = @intCast(per * i);
                stops[i] = @intCast(if (i + 1 == count) (total orelse std.math.maxInt(u64)) else per * (i + 1));
            }
            const rows = try store.plan(s.db, scope, d.id, if (total) |t| @intCast(t) else null, if (info.etag) |e| e.slice() else null, starts[0..count], stops[0..count]);
            segments.clearRetainingCapacity();
            for (rows) |p| try segments.append(gpa, .{
                .row_id = p.id,
                .index = @intCast(p.idx),
                .start = @intCast(p.start),
                .end = @intCast(p.stop),
            });
            scope.reset();
            try file.setLength(io, total orelse 0);
        }

        w.post(.{ .started = .{ .id = d.id, .name = name, .total = total, .segments = @intCast(segments.items.len), .resumed = resumable } });

        const started = nowMs(io);
        const outcome = d.supervise(scope, file, url, segments.items, ranged);
        // Whatever happened, what each segment has is written down — this
        // runs after `supervise`'s defer has stopped every task, so the
        // numbers are final.
        persist(d, scope, segments.items);
        try outcome;

        var bytes: u64 = 0;
        for (segments.items) |*seg| bytes += seg.done.load(.monotonic);
        if (total) |n| if (bytes != n) return error.ShortDownload;
        try store.setState(s.db, scope, d.id, .done, null);
        scope.reset();
        w.post(.{ .done = .{ .id = d.id, .bytes = bytes, .elapsed_ms = nowMs(io) - started } });
    }

    /// Starts every segment, restarts the ones that fail, and cancels the
    /// ones that stop moving — which is the one thing a deadline would do
    /// and, without an Engine, nothing else here does.
    fn supervise(d: *Download, scope: *store.Run, file: Io.File, url: []const u8, segments: []Segment, ranged: bool) !void {
        const s = d.shared;
        const io = s.io;
        const w = s.worker;
        const settings = w.settings;
        var last_reported: u64 = std.math.maxInt(u64);
        var last_saved_ms: i64 = nowMs(io);

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

            if (d.stop.load(.acquire)) return error.Stopped;
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
                        // A segment restored with nothing left to do.
                        if (seg.len()) |want| if (seg.done.load(.monotonic) >= want) {
                            seg.state.store(.ok, .release);
                            continue;
                        };
                        seg.attempts += 1;
                        seg.last_seen = seg.done.load(.monotonic);
                        seg.last_moved_ms = now;
                        seg.state.store(.running, .release);
                        seg.future = try io.concurrent(Segment.run, .{ seg, s.client, file, io, url, ranged });
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
            if (now - last_saved_ms >= 1000) {
                last_saved_ms = now;
                persist(d, scope, segments);
            }
            try Io.sleep(io, Io.Duration.fromMilliseconds(100), .awake);
        }
    }

    /// Write down what each segment has, for the ones that moved. A write
    /// that fails is a note rather than a failed download: the bytes are
    /// on disk either way, and the next save gets another chance.
    fn persist(d: *Download, scope: *store.Run, segments: []Segment) void {
        for (segments) |*seg| {
            const done = seg.done.load(.monotonic);
            if (done == seg.saved) continue;
            store.saveDone(d.shared.db, scope, seg.row_id, @intCast(done)) catch |err| {
                d.shared.worker.post(.{ .note = .{ .id = d.id, .text = .fmt("progress not saved: {t}", .{err}) } });
                continue;
            };
            seg.saved = done;
        }
        scope.reset();
    }
};

fn optionalEql(a: ?i64, b: ?u64) bool {
    if (a == null and b == null) return true;
    if (a == null or b == null) return false;
    return a.? == @as(i64, @intCast(b.?));
}

fn optionalTextEql(a: ?Text, b: ?Text) bool {
    if (a == null and b == null) return true;
    if (a == null or b == null) return false;
    return std.mem.eql(u8, a.?.slice(), b.?.slice());
}

// ------------------------------------------------------------------ probe

const Probe = struct { total: ?u64, ranged: bool, etag: ?Text };

/// One byte, asked for with a Range. A 206 says the server can slice and
/// how big the whole is; a 200 says it cannot, and the body it started
/// sending is dropped rather than drained. Either way the answer carries
/// what identifies the object, for the next run to compare against.
fn probe(d: *Download, url: []const u8) !Probe {
    var transfer: [4096]u8 = undefined;
    var redirect: [2048]u8 = undefined;
    var ex: fetch.Exchange = .idle;
    defer ex.end();

    const head = try ex.begin(d.shared.client, .{
        .method = .GET,
        .url = url,
        .headers = &.{.{ .name = "range", .value = "bytes=0-0" }},
        .redirect_buffer = &redirect,
        .transfer_buffer = &transfer,
    });
    const etag: ?Text = if (head.header("etag")) |e| .from(e) else if (head.header("last-modified")) |m| .from(m) else null;
    switch (head.status) {
        .partial_content => {
            const cr = head.header("content-range") orelse return error.NoContentRange;
            return .{ .total = try totalFromContentRange(cr), .ranged = true, .etag = etag };
        },
        .ok => return .{ .total = head.content_length, .ranged = false, .etag = etag },
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
    /// Its row in `segments`, where `done` is written down.
    row_id: i64,
    index: usize,
    start: u64,
    /// Exclusive. `maxInt` when the length is unknown, which is also the
    /// unranged case.
    end: u64,
    /// Bytes that have reached the file, counted from `start`. The task
    /// stores; the supervisor reads.
    done: std.atomic.Value(u64) = .init(0),
    /// What the database last heard.
    saved: u64 = 0,
    /// `.running` until the task writes its verdict, which it does before
    /// returning so the supervisor can `await` without ever blocking.
    state: std.atomic.Value(SegState) = .init(.idle),
    err: anyerror = error.None,
    attempts: u8 = 0,
    future: ?Io.Future(void) = null,
    last_seen: u64 = 0,
    last_moved_ms: i64 = 0,

    const SegState = enum(u8) { idle, running, ok, failed };

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
