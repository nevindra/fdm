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
//! **The database is the list, and the queue.** Every download is a row in
//! `store.zig`'s SQLite file before it is anything else, its id is the
//! row's, and the segments' progress is written there once a second and on
//! every way out. So a restart shows the same list, and a download that was
//! running when the process died resumes from the last byte each segment
//! had written — after asking the server again and checking that its
//! `ETag` and length are what they were, because a file that changed
//! underneath a resume is a corrupt download that looks complete.
//!
//! Which downloads run, how many at once and in what order is `nilo_job`'s:
//! a `Fetch` row per download in the same file, `parallel` workers claiming
//! them in order, and a whole-download retry with backoff on top of the
//! per-segment one inside. Quitting cancels the workers, and a worker that
//! is cancelled hands its row back to the queue — so "resume on the next
//! start" is the queue's ordinary behaviour rather than a path of its own.
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
const job = @import("nilo_job");
const store = @import("store.zig");
const dns = @import("dns.zig");
const disk = @import("disk.zig");

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
    /// How many downloads run at once; the rest wait their turn in order.
    parallel: u8 = 3,
    /// Connections per download. Sixteen because that is where the
    /// per-connection ceiling most CDNs apply stops being the limit and
    /// the pipe becomes it — and the number Surge settled on for the same
    /// reason. `-n` overrides it; the comparison in `bench/` is what
    /// should move the default.
    segments: u8 = 16,
    /// Milliseconds a segment may go without a byte before it is cancelled.
    stall_ms: u32 = 10_000,
    /// How many times one segment may be restarted after a stall or an error.
    retries: u8 = 3,
    /// Below this, splitting costs more handshakes than it saves.
    min_segment: u64 = 1 << 20,
    /// A connection that finished its chunk takes the next one sized to
    /// this many seconds at the rate it just showed, so a slow connection
    /// is never holding more than this much of the file and a fast one is
    /// not asking every second. What each request costs is one round trip
    /// idle, which at 200 ms is 2.5% of eight seconds.
    chunk_secs: f64 = 8,
    /// A running segment that needs at least this many seconds more at
    /// its current rate may have the second half of what it has left taken
    /// by a segment that has finished its own. Seconds rather than bytes,
    /// because what a steal saves is time: half a megabyte on a connection
    /// at 300 KB/s is worth a handshake, and four megabytes at 10 MB/s is
    /// not.
    steal_min_secs: f64 = 2,
    /// Where a download lands when `Add.out` does not say; null is the
    /// directory the process started in. Has to outlive the worker.
    dir: ?[]const u8 = null,
    /// Milliseconds a failed segment waits before its next attempt. Zero
    /// is back to back, which is right for a dropped connection and wrong
    /// for a server that is briefly refusing everyone.
    retry_wait_ms: u32 = 0,
    /// At start, queue the downloads that were paused too — for a person
    /// who quit to reboot rather than to stop.
    auto_resume: bool = false,
    /// A running segment under this fraction of the others' mean rate is
    /// reconnected. Zero turns the reconnect off.
    slow_fraction: f64 = 0.3,
    /// How many consecutive health checks (two seconds apart) a segment
    /// has to be slow for before it is reconnected.
    slow_checks: u8 = 1,
    /// At most this many reconnects in one health check.
    slow_per_check: u8 = 255,
};

/// One download as the person asked for it. The strings are allocated with
/// the worker's allocator by the caller and freed by the worker — `dupe`
/// makes such a copy, `free` is what the worker calls.
pub const Add = struct {
    url: []const u8,
    /// `Name: value`, one per entry, sent with the probe and every segment.
    /// `Authorization` is one of them; the worker knows where it goes.
    headers: []const []const u8 = &.{},
    /// Where it goes, when the person said: a file, or a directory when it
    /// ends in a separator or is one already. Null is `Settings.dir`.
    out: ?[]const u8 = null,
    /// Queue it although a row with the same URL or path is in the list.
    force: bool = false,
    /// Lowercase hex the finished file has to hash to; a mismatch is a
    /// failure and the file is kept for looking at.
    sha256: ?[]const u8 = null,

    pub fn dupe(a: Add, gpa: std.mem.Allocator) !Add {
        var copy: Add = .{ .url = try gpa.dupe(u8, a.url), .force = a.force };
        errdefer copy.free(gpa);
        if (a.out) |o| copy.out = try gpa.dupe(u8, o);
        if (a.sha256) |h| copy.sha256 = try gpa.dupe(u8, h);
        const headers = try gpa.alloc([]const u8, a.headers.len);
        for (headers) |*h| h.* = "";
        copy.headers = headers;
        for (a.headers, headers) |from, *to| to.* = try gpa.dupe(u8, from);
        return copy;
    }

    pub fn free(a: Add, gpa: std.mem.Allocator) void {
        gpa.free(a.url);
        if (a.out) |o| gpa.free(o);
        if (a.sha256) |h| gpa.free(h);
        for (a.headers) |h| gpa.free(h);
        gpa.free(a.headers);
    }
};

/// What the UI asks for.
pub const Command = union(enum) {
    add: Add,
    cancel: i64,
    /// Run a failed or cancelled download again — from where it got to, if
    /// the server still has the same file.
    restart: i64,
    /// A new URL for a paused or failed download — a signed link that
    /// expired, a mirror that went away — and then `restart`. The `Add`'s
    /// headers replace the row's when there are any; its `out`, `force`
    /// and `sha256` are ignored.
    refresh: struct { id: i64, add: Add },
    /// Forget the row — and, when asked, the file on disk with it.
    delete: struct { id: i64, file: bool },
    quit,
};

/// What the worker reports. One `added` per row — on `add`, and for every
/// row in the database when the worker starts — then `started`, `progress`
/// while it moves, `note` for anything a person would want to see, and
/// exactly one of `done` / `failed` / `cancelled` at the end of each run.
/// The most segments one download reports on individually. A download
/// may have more; the rest are summed into `bytes`.
pub const max_shown_segments = 24;

/// One segment as the UI sees it. `len` may change while it runs — a
/// segment that had half of its remainder taken is shorter than it was.
pub const SegView = struct { len: u64, done: u64 };

pub const Event = union(enum) {
    added: struct { id: i64, url: Text, name: Text, path: Text, state: State, total: ?u64, bytes: u64, segments: u8 },
    /// Waiting its turn — on `add`, `r`, and at start for what was unfinished.
    queued: struct { id: i64 },
    /// `name` and `path` may differ from `added`'s: the server's
    /// `Content-Disposition` names a file the URL did not.
    started: struct { id: i64, name: Text, path: Text, total: ?u64, segments: u8, resumed: bool },
    progress: struct { id: i64, bytes: u64, seg: [max_shown_segments]SegView, seg_count: u8 },
    note: struct { id: i64, text: Text },
    done: struct { id: i64, bytes: u64, elapsed_ms: i64 },
    failed: struct { id: i64, text: Text },
    cancelled: struct { id: i64 },
    removed: struct { id: i64 },
    /// Not added: row `of` already has this URL or this path. `Add.force`
    /// is the answer when it was meant.
    duplicate: struct { of: i64, url: Text },
    /// The row's URL changed; `queued` follows.
    refreshed: struct { id: i64, url: Text },
    /// A `refresh` that could not be done — no such row, or one still
    /// running. Nothing about the row changed.
    refused: struct { id: i64, text: Text },
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
            .add => |a| a.free(w.gpa),
            .refresh => |r| r.add.free(w.gpa),
            else => {},
        }
    }
};

// ----------------------------------------------------------------- thread

/// The job: fetch one download, by row id. **The payload is the id and
/// nothing else** — the row is the truth and is read back at the start of
/// `run`, which is what makes a claim after a crash the same call as the
/// first one.
///
/// `timeout_ms` cannot fire without an Engine, so here it is only the
/// lease: how long a claimed row is left alone. A day, because a download
/// may take that long, and `store.releaseStale` is what covers the crash
/// case that a shorter lease would otherwise cover.
const Fetch = struct {
    pub const nilo_job = "fetch";
    pub const timeout_ms = 24 * 60 * 60 * 1000;
    /// Whole-download retries, over and above the per-segment ones inside:
    /// a server that was down for a minute gets three more chances,
    /// spread out.
    pub const retry: job.Retry = .{ .times = 3, .backoff = .{ .exponential = .{ .from_ms = 2_000, .to_ms = 60_000 } } };
    /// A 404 does not get better by waiting; nor does a full disk or a
    /// file that hashed to the wrong thing.
    pub const final = error{ BadStatus, NoSpaceLeft, ChecksumMismatch };

    download_id: i64,

    pub fn run(self: Fetch, scope: *store.Run, shared: *Shared) !void {
        _ = scope;
        return Download.runJob(shared, self.download_id);
    }
};

const Jobs = job.Jobs(.{
    .kinds = .{Fetch},
    .store = store.JobTable,
    .deps = struct { shared: *Shared },
});

/// Everything a download task needs that is shared: the worker, the client,
/// the database, the Io, and which downloads are running right now. One of
/// these for the life of the thread.
const Shared = struct {
    worker: *Worker,
    client: *fetch.Client,
    db: *store.Db,
    jobs: *Jobs,
    io: Io,
    /// The tasks in flight, so a `cancel` can reach the one it names.
    lock: Lock = .{},
    active: std.ArrayList(*Download) = .empty,

    fn register(s: *Shared, d: *Download) !void {
        s.lock.lock();
        defer s.lock.unlock();
        try s.active.append(s.worker.gpa, d);
    }

    fn unregister(s: *Shared, d: *Download) void {
        s.lock.lock();
        defer s.lock.unlock();
        for (s.active.items, 0..) |a, i| if (a == d) {
            _ = s.active.swapRemove(i);
            return;
        };
    }

    /// Flag the running task for `id`, if there is one.
    fn cancelActive(s: *Shared, id: i64) bool {
        s.lock.lock();
        defer s.lock.unlock();
        for (s.active.items) |a| if (a.id == id) {
            a.cancel.store(true, .release);
            return true;
        };
        return false;
    }
};

fn threadMain(w: *Worker) void {
    const gpa = w.gpa;

    // The same Io a Native SDK worker thread makes for itself, with one
    // vtable slot swapped so a host is resolved once a download rather
    // than once a connection (`dns.zig` says why).
    var threaded: Io.Threaded = .init(gpa, .{});
    defer threaded.deinit();
    const io = dns.wrap(threaded.io());

    var db = store.open(gpa, io, w.db_path) catch |err| {
        w.post(.{ .fatal = .fmt("cannot open {s}: {t}", .{ w.db_path, err }) });
        return;
    };
    defer db.deinit();

    var client: fetch.Client = .init(gpa, .{
        // Three downloads of sixteen segments, and a probe or two.
        .max_in_flight = 64,
        // The deadline cannot fire without an Engine, so zero is honest; the
        // stall watchdog in `Download.supervise` is the bound instead.
        .timeout_ms = 0,
        // A refused body is dropped rather than drained: the probe leaves a
        // whole file unread when the server ignores its Range.
        .max_drain = 4 << 10,
    });
    defer client.deinit();
    client.nilo_start(io, .off) catch return;

    var table = store.JobTable.open(&db);
    var shared: Shared = .{ .worker = w, .client = &client, .db = &db, .jobs = undefined, .io = io };
    defer shared.active.deinit(gpa);
    var jobs: Jobs = .open(gpa, &table, .{ .shared = &shared }, .{
        .workers = w.settings.parallel,
        // The latency between `add` and the first byte is this plus the
        // command loop's sleep: a tenth of a second, against the half
        // second it was, for one small query per worker per tick.
        .poll_ms = 100,
    });
    shared.jobs = &jobs;
    jobs.nilo_start(io, .off) catch return;

    var run: store.Run = .initIo(gpa, io);
    defer run.deinit();

    // The list is whatever the database has. Anything that was on its way
    // when the process last stopped is queued again.
    restore(&shared, &run);

    var serving = io.concurrent(Jobs.serveOn, .{ &jobs, io }) catch {
        w.post(.{ .fatal = .from("no thread for the queue") });
        return;
    };

    while (true) {
        const cmds = w.takeCommands();
        defer gpa.free(cmds);
        for (cmds) |cmd| switch (cmd) {
            .add => |a| {
                defer w.freeCommand(cmd);
                insert(&shared, &run, a) catch |err| {
                    w.post(.{ .fatal = .fmt("cannot add {s}: {t}", .{ a.url, err }) });
                };
            },
            .restart => |id| enqueue(&shared, &run, id) catch |err| {
                w.post(.{ .failed = .{ .id = id, .text = .fmt("cannot queue: {t}", .{err}) } });
            },
            .refresh => |r| {
                defer w.freeCommand(cmd);
                refresh(&shared, &run, r.id, r.add) catch |err| {
                    w.post(.{ .refused = .{ .id = r.id, .text = .fmt("not refreshed: {t}", .{err}) } });
                };
            },
            .cancel => |id| {
                // The row says cancelled either way; a job that has not
                // been claimed yet reads that and does nothing. A running
                // one is told, and reports itself when it has stopped.
                store.setState(&db, &run, id, .cancelled, null) catch {};
                run.reset();
                if (!shared.cancelActive(id)) w.post(.{ .cancelled = .{ .id = id } });
            },
            .delete => |del| {
                // Stop it first if it is running; the task then finds no row
                // to write its verdict into, which is fine. A queued job for
                // it is dropped with the row, and one already claimed reads
                // a missing row and returns.
                _ = shared.cancelActive(del.id);
                remove(&shared, &run, del.id, del.file) catch |err| {
                    w.post(.{ .note = .{ .id = del.id, .text = .fmt("not deleted: {t}", .{err}) } });
                    continue;
                };
                w.post(.{ .removed = .{ .id = del.id } });
            },
            .quit => {
                // The workers are cancelled mid-run: each task writes its
                // progress down on the way out and hands its row back to
                // the queue, so the next start picks it up.
                serving.cancel(io) catch {};
                return;
            },
        };
        Io.sleep(io, Io.Duration.fromMilliseconds(50), .awake) catch return;
    }
}

/// Report every row, and queue the ones that were not finished.
fn restore(s: *Shared, run: *store.Run) void {
    defer run.reset();
    _ = store.releaseStale(s.db, run) catch |err| {
        s.worker.post(.{ .fatal = .fmt("cannot reset the queue: {t}", .{err}) });
    };
    const rows = store.all(s.db, run) catch |err| {
        s.worker.post(.{ .fatal = .fmt("cannot read the list: {t}", .{err}) });
        return;
    };
    for (rows) |row| {
        var bytes: u64 = 0;
        if (store.segmentsOf(s.db, run, row.id)) |segs| {
            for (segs) |seg| bytes += @intCast(seg.done);
        } else |_| {}
        const unfinished = row.state == .queued or row.state == .running or
            (s.worker.settings.auto_resume and row.state == .cancelled);
        s.worker.post(.{ .added = .{
            .id = row.id,
            .url = .from(row.url),
            .name = .from(row.name),
            .path = .from(row.path),
            .state = if (unfinished) .queued else row.state,
            .total = if (row.total) |t| @intCast(t) else null,
            .bytes = bytes,
            .segments = @intCast(@min(row.segments, 255)),
        } });
        if (unfinished) enqueue(s, run, row.id) catch |err| {
            s.worker.post(.{ .failed = .{ .id = row.id, .text = .fmt("cannot queue: {t}", .{err}) } });
        };
    }
}

/// A new row for a URL, written down and queued before anything is fetched.
/// Unless a row already has the URL or the path, which is reported and not
/// added — the same file twice is two writers on one path.
fn insert(s: *Shared, run: *store.Run, a: Add) !void {
    defer run.reset();
    const arena = run.arena();
    const url = a.url;
    const target = try decidePath(s, arena, a);
    const name = std.fs.path.basename(target.path);
    if (!a.force) if (try store.duplicate(s.db, run, url, target.path)) |had| {
        s.worker.post(.{ .duplicate = .{ .of = had.id, .url = .from(url) } });
        return;
    };
    const headers: ?[]const u8 = if (a.headers.len == 0) null else try std.mem.join(arena, "\n", a.headers);
    const row = try store.add(s.db, run, url, name, target.path, headers, target.named, a.sha256, nowMs(s.io));
    s.worker.post(.{ .added = .{
        .id = row.id,
        .url = .from(url),
        .name = .from(name),
        .path = .from(target.path),
        .state = .queued,
        .total = null,
        .bytes = 0,
        .segments = 0,
    } });
    try enqueue(s, run, row.id);
}

/// The row keeps its file, its segments and what the old server said the
/// object was; only the link moves, and the next run's probe decides
/// whether the new one has the same file. A running row is refused —
/// pause it first — because its segments are on the old URL.
fn refresh(s: *Shared, run: *store.Run, id: i64, a: Add) !void {
    defer run.reset();
    const row = (try s.db.find(store.Download, run, id)) orelse return error.NoSuchDownload;
    if (row.state == .running or row.state == .queued) return error.StillRunning;
    const headers: ?[]const u8 = if (a.headers.len == 0) null else try std.mem.join(run.arena(), "\n", a.headers);
    try store.refresh(s.db, run, id, a.url, headers);
    s.worker.post(.{ .refreshed = .{ .id = id, .url = .from(a.url) } });
    try enqueue(s, run, id);
}

/// Where the file goes, absolute — a resume from another directory has to
/// find it. `out` is a file, and then the name is the person's; or a
/// directory, when it ends in a separator or is one already, and then the
/// name is the URL's until the server offers a better one. No `out` is
/// `Settings.dir`, or the directory the process started in.
fn decidePath(s: *Shared, arena: std.mem.Allocator, a: Add) !struct { path: []const u8, named: bool } {
    var cwd_buf: [std.fs.max_path_bytes]u8 = undefined;
    const cwd = cwd_buf[0..try std.process.currentPath(s.io, &cwd_buf)];
    const from_url = nameFromUrl(a.url);
    if (a.out) |out| {
        const is_dir = std.fs.path.isSep(out[out.len - 1]) or blk: {
            const st = Io.Dir.cwd().statFile(s.io, out, .{}) catch break :blk false;
            break :blk st.kind == .directory;
        };
        // `resolve` rather than `join`: an absolute `out` stands alone.
        const path = if (is_dir)
            try std.fs.path.resolve(arena, &.{ cwd, out, from_url })
        else
            try std.fs.path.resolve(arena, &.{ cwd, out });
        return .{ .path = path, .named = !is_dir };
    }
    const dir = s.worker.settings.dir orelse cwd;
    return .{ .path = try std.fs.path.resolve(arena, &.{ cwd, dir, from_url }), .named = false };
}

/// The row, its segments and its queued job go; the file only when asked.
fn remove(s: *Shared, run: *store.Run, id: i64, file: bool) !void {
    defer run.reset();
    const row = (try s.db.find(store.Download, run, id)) orelse return;
    if (file) Io.Dir.cwd().deleteFile(s.io, row.path) catch |err| switch (err) {
        error.FileNotFound => {},
        else => return err,
    };
    try store.remove(s.db, run, id);
}

/// One job per row: the key makes a second push while the first is queued
/// or running a no-op rather than a second writer on the same file.
fn enqueue(s: *Shared, run: *store.Run, id: i64) !void {
    defer run.reset();
    var key: [32]u8 = undefined;
    const unique = try std.fmt.bufPrint(&key, "dl:{d}", .{id});
    try store.setState(s.db, run, id, .queued, null);
    _ = try s.jobs.push(run, Fetch{ .download_id = id }, .{ .unique = unique });
    s.worker.post(.{ .queued = .{ .id = id } });
}

// --------------------------------------------------------------- download

const Download = struct {
    shared: *Shared,
    id: i64,
    /// The person said stop: the row becomes `cancelled` and waits for `r`.
    cancel: std.atomic.Value(bool) = .init(false),
    /// A reason better than the error's name, when the code that failed
    /// had one — an HTTP status, say. Read once, by `runJob`.
    why: ?Text = null,
    /// Where the part of the file no segment covers yet begins; `total`
    /// once everything is handed out. The plan covers only the front, and
    /// a connection that finishes its chunk takes the next from here —
    /// so a slow connection holds a little and a fast one keeps going,
    /// which is what a split by sixteen decided once cannot do. Only when
    /// this reaches the end does a finished connection steal instead.
    frontier: u64 = 0,
    /// Length of the file, when known.
    total: ?u64 = null,
    /// The whole download's rate, one sample a second, the last ten kept:
    /// what `reconnectSlow` reads to tell a link that is full from a
    /// connection that is slow.
    totals: [10]f64 = @splat(0),
    totals_n: usize = 0,
    total_rate: f64 = 0,
    prev_total_rate: f64 = 0,
    /// The last steal or reconnect, and the total then: two seconds later
    /// the total is read again, and if it fell the link was the limit
    /// after all — a burst allowance that ran out, a policer — and both
    /// stop for ten seconds while the peak is forgotten.
    acted_ms: i64 = 0,
    acted_total: f64 = 0,
    hold_until_ms: i64 = 0,

    /// Whether adding or replacing a connection could carry more: the
    /// download ran under 0.75× its usual second of the last ten, for two
    /// seconds running — or under half of it, for one. On a shared link the total sits at the link's
    /// rate however the connections divide it, and a new connection —
    /// a steal, a reconnect — costs a handshake, discards what was in
    /// flight, and moves nothing; on a 100 Mbit link with sixteen
    /// connections that was five seconds in twenty. On a host that caps
    /// each connection the total falls as segments finish or an edge
    /// goes bad, and that is when both are worth their handshake.
    fn linkHasRoom(d: *const Download, now: i64) bool {
        if (now < d.hold_until_ms or d.totals_n < 3) return false;
        const usual = d.usualRate();
        return d.total_rate < 0.75 * usual and (d.prev_total_rate < 0.75 * usual or d.total_rate < 0.5 * usual);
    }

    /// The median second of the last ten: what the download usually
    /// gets. Not the best one, because a burst allowance hands out one
    /// second at ten times the rate that follows, and everything after
    /// it would look like room.
    fn usualRate(d: *const Download) f64 {
        const n = @min(d.totals_n, d.totals.len);
        var sorted: [10]f64 = undefined;
        @memcpy(sorted[0..n], d.totals[0..n]);
        std.mem.sort(f64, sorted[0..n], {}, std.sort.asc(f64));
        return sorted[n / 2];
    }

    /// What `Fetch.run` is. The verdict goes two places: the `downloads`
    /// row, for the list, and the return value, for the queue — which
    /// retries an error, buries a `final` one, and hands a `Canceled` row
    /// back so the next start takes it.
    fn runJob(shared: *Shared, id: i64) !void {
        const gpa = shared.worker.gpa;
        var scope: store.Run = .initIo(gpa, shared.io);
        defer scope.deinit();

        // A row cancelled while the job waited its turn: nothing to do.
        const row = (try shared.db.find(store.Download, &scope, id)) orelse return;
        const skip = row.state == .cancelled or row.state == .done;
        scope.reset();
        if (skip) return;

        var d: Download = .{ .shared = shared, .id = id };
        try shared.register(&d);
        defer shared.unregister(&d);

        d.runInner(&scope) catch |err| {
            scope.reset();
            if (err == error.Canceled) {
                // The process is leaving. Progress is written; the row
                // stays `running` here and goes back to `queued` there.
                return err;
            }
            if (err == error.Cancelled) {
                store.setState(shared.db, &scope, id, .cancelled, null) catch {};
                shared.worker.post(.{ .cancelled = .{ .id = id } });
                return;
            }
            const why = d.why orelse Text.fmt("{t}", .{err});
            store.setState(shared.db, &scope, id, .failed, why.slice()) catch {};
            shared.worker.post(.{ .failed = .{ .id = id, .text = why } });
            return err;
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
        var path = try gpa.dupe(u8, row.path);
        defer gpa.free(path);
        var name: Text = .from(row.name);
        const named = row.named;
        const stored_total = row.total;
        const stored_etag: ?Text = if (row.etag) |e| .from(e) else null;
        const header_lines = try gpa.dupe(u8, row.headers orelse "");
        defer gpa.free(header_lines);
        const want_sha256: ?Text = if (row.sha256) |h| .from(h) else null;
        const headers = try Headers.parse(gpa, header_lines);
        defer headers.free(gpa);

        // **Each segment is its own allocation**, because a segment task
        // holds a pointer to it for as long as it runs and the list grows
        // while tasks run — a stolen half is a new segment.
        const prior = try store.segmentsOf(s.db, scope, d.id);
        var segments: std.ArrayList(*Segment) = .empty;
        defer {
            for (segments.items) |seg| gpa.destroy(seg);
            segments.deinit(gpa);
        }
        for (prior) |p| try segments.append(gpa, try Segment.create(gpa, p));
        scope.reset();

        try store.setState(s.db, scope, d.id, .running, null);
        scope.reset();

        // The probe is the one request nothing else covers: a segment that
        // cannot connect is tried again by the supervisor, so the probe gets
        // the same allowance. Only for failures before an answer — a
        // resolver that gave up, a connection refused — since an answer the
        // server chose is `why` and does not improve by asking again.
        var location_buf: [2048]u8 = undefined;
        const info = blk: {
            var attempt: u32 = 1;
            while (true) : (attempt += 1) {
                break :blk probe(d, url, headers, &location_buf) catch |err| {
                    if (d.why != null or err == error.Canceled or err == error.Cancelled) return err;
                    if (attempt > settings.retries) return err;
                    w.post(.{ .note = .{ .id = d.id, .text = .fmt("probe attempt {d} failed: {t}, retrying", .{ attempt, err }) } });
                    try Io.sleep(io, Io.Duration.fromMilliseconds(1000), .awake);
                    continue;
                };
            }
        };

        // The server's name for it, taken once: before the file exists, and
        // never over a name the person chose.
        if (!named and segments.items.len == 0) if (info.filename) |offered| if (!std.mem.eql(u8, offered.slice(), name.slice())) {
            const dir = std.fs.path.dirname(path) orelse ".";
            const moved = try std.fs.path.join(gpa, &.{ dir, offered.slice() });
            store.rename(s.db, scope, d.id, offered.slice(), moved) catch |err| {
                gpa.free(moved);
                return err;
            };
            scope.reset();
            gpa.free(path);
            path = moved;
            name = offered;
            w.post(.{ .note = .{ .id = d.id, .text = .fmt("named {s} by the server", .{offered.slice()}) } });
        };

        // Resume only when the server still has the same file and the one
        // on disk is the size the plan expects; otherwise plan afresh. The
        // directory is made on the way: `--dir` and `-o` may name one that
        // is not there yet.
        if (std.fs.path.dirname(path)) |dir| try Io.Dir.cwd().createDirPath(io, dir);
        const file = try Io.Dir.cwd().createFile(io, path, .{ .truncate = false, .read = true });
        defer file.close(io);
        const on_disk = try file.length(io);
        const same_object = optionalEql(stored_total, info.total) and
            optionalTextEql(stored_etag, info.etag) and
            (stored_etag != null or info.total != null);
        const resumable = segments.items.len > 0 and same_object and info.ranged and
            info.total != null and on_disk == info.total.?;

        // One comparison before a byte lands: what is still to come
        // against what the filesystem has. A disk that fills mid-file is
        // caught below as `NoSpaceLeft`; this catches the one that was
        // already too small, without three retries and a steal first.
        if (info.total) |total| {
            var have: u64 = 0;
            if (resumable) for (segments.items) |seg| {
                have += seg.have();
            };
            const need = total - @min(total, have);
            if (disk.free(std.fs.path.dirname(path) orelse ".")) |room| if (room < need) {
                d.why = .fmt("needs {d} MB, {d} MB free", .{ need >> 20, room >> 20 });
                return error.NoSpaceLeft;
            };
        }

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

            // The first chunks cover a quarter of the file between them,
            // a megabyte each at least; the rest is handed out as
            // connections finish, sized to what each one showed. One
            // segment when the server cannot slice, or the length is
            // unknown — to `maxInt(i64)` in the row, the column being
            // signed, which `Segment.create` reads back as "no end".
            var starts: [256]i64 = undefined;
            var stops: [256]i64 = undefined;
            const per: u64 = if (count == 1) 0 else @max(settings.min_segment, total.? / (count * 4));
            for (0..count) |i| {
                starts[i] = @intCast(per * i);
                stops[i] = if (count == 1) (if (total) |n| @intCast(n) else std.math.maxInt(i64)) else @intCast(@min(per * (i + 1), total.?));
            }
            const rows = try store.plan(s.db, scope, d.id, if (total) |t| @intCast(t) else null, if (info.etag) |e| e.slice() else null, starts[0..count], stops[0..count]);
            for (segments.items) |seg| gpa.destroy(seg);
            segments.clearRetainingCapacity();
            for (rows) |p| try segments.append(gpa, try Segment.create(gpa, p));
            scope.reset();
            try file.setLength(io, total orelse 0);
        }
        d.total = total;
        d.frontier = if (total) |n| n else std.math.maxInt(u64);
        if (ranged) for (segments.items) |seg| {
            if (seg == segments.items[0]) d.frontier = 0;
            d.frontier = @max(d.frontier, seg.end.load(.acquire));
        };

        w.post(.{ .started = .{ .id = d.id, .name = name, .path = .from(path), .total = total, .segments = @intCast(segments.items.len), .resumed = resumable } });

        const started = nowMs(io);
        // The segments go where the probe ended up, not where it started.
        const fetch_url = info.location orelse url;
        if (info.location) |l| w.post(.{ .note = .{ .id = d.id, .text = .fmt("redirected to {s}", .{l}) } });
        const outcome = d.supervise(scope, file, fetch_url, headers, &segments, ranged);
        // Whatever happened, what each segment has is written down — this
        // runs after `supervise`'s defer has stopped every task, so the
        // numbers are final.
        persist(d, scope, segments.items);
        try outcome;

        var bytes: u64 = 0;
        for (segments.items) |seg| bytes += seg.have();
        if (total) |n| if (bytes != n) return error.ShortDownload;
        if (want_sha256) |want| {
            const got = try sha256Of(file, io);
            if (!std.ascii.eqlIgnoreCase(want.slice(), &got)) {
                d.why = .fmt("sha256 mismatch: got {s}", .{got});
                return error.ChecksumMismatch;
            }
            w.post(.{ .note = .{ .id = d.id, .text = .from("sha256 verified") } });
        }
        // The file's time is when it was published, not when it arrived —
        // so an archive sorts where it belongs. Not worth failing over.
        if (info.last_modified) |lm| if (parseHttpDate(lm.slice())) |secs| {
            file.setTimestamps(io, .{ .modify_timestamp = .{ .new = .{ .nanoseconds = @as(i96, secs) * std.time.ns_per_s } } }) catch |err| {
                w.post(.{ .note = .{ .id = d.id, .text = .fmt("modification time not set: {t}", .{err}) } });
            };
        };
        try store.setState(s.db, scope, d.id, .done, null);
        scope.reset();
        w.post(.{ .done = .{ .id = d.id, .bytes = bytes, .elapsed_ms = nowMs(io) - started } });
    }

    /// Starts every segment, restarts the ones that fail, and cancels the
    /// ones that stop moving — which is the one thing a deadline would do
    /// and, without an Engine, nothing else here does. Two more things,
    /// both about connections not being equal, which is what a CDN is:
    ///
    /// - **A slow segment is reconnected.** Once a second every running
    ///   segment's rate is sampled; one that has run at least three seconds
    ///   and is under 0.3× the mean of the others, with a megabyte or more
    ///   to go, is cancelled and resumes from where it was on a fresh
    ///   connection — which as often as not lands on a different edge. Not
    ///   a failed attempt: it does not count towards `retries`.
    /// - **A finished segment takes half of what is left of the longest
    ///   running one** when that would take `steal_min_secs` or more, so the download
    ///   does not end at the pace of its slowest connection. The victim is
    ///   not cancelled: its `end` is an atomic the task reads on every
    ///   chunk, so it stops at the new boundary and keeps its connection.
    ///   The boundary is put half a megabyte ahead of where the victim is,
    ///   so it has not passed it by the time the store is written; if it
    ///   somehow has, the new segment re-downloads a little, which is
    ///   waste rather than corruption.
    fn supervise(d: *Download, scope: *store.Run, file: Io.File, url: []const u8, headers: Headers, segments: *std.ArrayList(*Segment), ranged: bool) !void {
        const s = d.shared;
        const io = s.io;
        const w = s.worker;
        const gpa = w.gpa;
        const settings = w.settings;
        var last_reported: u64 = std.math.maxInt(u64);
        var last_sampled_bytes: u64 = 0;
        var last_saved_ms: i64 = nowMs(io);
        var last_sample_ms: i64 = last_saved_ms;
        var last_health_ms: i64 = last_saved_ms;

        // **No task outlives this frame.** The segments are freed by the
        // caller the moment this returns, and a segment task writes into
        // its own; so every way out — done, cancelled, out of retries, a
        // spawn that failed — cancels whatever is still running and waits
        // for it. On a future that already finished, `cancel` is an
        // `await` that returns at once.
        defer for (segments.items) |seg| if (seg.future) |*f| {
            f.cancel(io);
            seg.future = null;
        };

        while (true) {
            const now = nowMs(io);
            var pending: usize = 0;
            var running: usize = 0;
            var bytes: u64 = 0;

            if (d.cancel.load(.acquire)) return error.Cancelled;

            const sampling = now - last_sample_ms >= 1000;
            if (sampling) last_sample_ms = now;

            for (segments.items) |seg| {
                bytes += seg.have();
                switch (seg.state.load(.acquire)) {
                    .ok => if (seg.future) |*f| {
                        f.await(io); // finished already: returns at once
                        seg.future = null;
                        seg.finished_ms = now;
                    },
                    .idle, .failed => {
                        if (seg.future) |*f| {
                            f.await(io);
                            seg.future = null;
                        }
                        if (seg.state.load(.acquire) == .failed) {
                            // A full disk is not a segment's fault, and
                            // the next segment would only fill it more.
                            if (seg.err == error.NoSpaceLeft) return seg.err;
                            seg.failures += 1;
                            seg.wait_until_ms = now + settings.retry_wait_ms;
                            w.post(.{ .note = .{ .id = d.id, .text = .fmt("segment {d}: attempt {d} failed: {t}", .{ seg.index, seg.attempts, seg.err }) } });
                            seg.state.store(.idle, .release);
                        }
                        if (seg.failures > settings.retries) return seg.err;
                        // Nothing left to do — restored complete, or its
                        // remainder was taken.
                        if (seg.remaining() == 0) {
                            seg.state.store(.ok, .release);
                            continue;
                        }
                        if (now < seg.wait_until_ms) {
                            pending += 1;
                            continue;
                        }
                        seg.attempts += 1;
                        seg.last_seen = seg.have();
                        seg.last_moved_ms = now;
                        seg.started_ms = now;
                        seg.sample_bytes = seg.have();
                        seg.rate = 0;
                        seg.state.store(.running, .release);
                        seg.future = try io.concurrent(Segment.run, .{ seg, s.client, file, io, url, headers, ranged });
                        pending += 1;
                        running += 1;
                    },
                    .running => {
                        pending += 1;
                        running += 1;
                        const seen = seg.have();
                        if (seen != seg.last_seen) {
                            seg.last_seen = seen;
                            seg.last_moved_ms = now;
                        } else if (now - seg.last_moved_ms > settings.stall_ms) {
                            seg.future.?.cancel(io);
                            seg.future = null;
                            seg.failures += 1;
                            seg.wait_until_ms = now + settings.retry_wait_ms;
                            w.post(.{ .note = .{ .id = d.id, .text = .fmt("segment {d}: no bytes for {d}ms, cancelled and retrying", .{ seg.index, now - seg.last_moved_ms }) } });
                            seg.state.store(.idle, .release);
                        }
                        if (sampling) {
                            seg.rate = @as(f64, @floatFromInt(seen -| seg.sample_bytes));
                            seg.sample_bytes = seen;
                        }
                    },
                }
            }

            // NextChunk: a free slot and file not yet handed out. Sized to
            // `chunk_secs` at the rate the segment that just finished
            // showed, which is the connection most likely to carry it.
            while (ranged and running < settings.segments and d.frontier < d.total.?) {
                try d.extend(scope, segments, now, file, url, headers);
                pending += 1;
                running += 1;
            }

            if (pending == 0) return;

            if (sampling) {
                d.prev_total_rate = d.total_rate;
                d.total_rate = @as(f64, @floatFromInt(bytes -| last_sampled_bytes));
                last_sampled_bytes = bytes;
                d.totals[d.totals_n % d.totals.len] = d.total_rate;
                d.totals_n += 1;
                // What the last action did to the total, once it has had
                // two seconds to show.
                if (d.acted_ms != 0 and now - d.acted_ms >= 2000) {
                    if (d.total_rate < 0.85 * d.acted_total) {
                        d.hold_until_ms = now + 10_000;
                        d.totals_n = 0;
                        w.post(.{ .note = .{ .id = d.id, .text = .fmt("{d} KB/s after the last reconnect or steal, {d} KB/s before: the link is the limit, holding", .{ @as(u64, @intFromFloat(d.total_rate / 1024)), @as(u64, @intFromFloat(d.acted_total / 1024)) }) } });
                    }
                    d.acted_ms = 0;
                }
            }

            // HealthCheck, every two seconds.
            if (now - last_health_ms >= 2000) {
                last_health_ms = now;
                if (d.linkHasRoom(now) and d.reconnectSlow(segments.items, now)) d.acted(now);
            }

            // StealWork: a free slot, nothing left to hand out, somebody
            // with enough left, and a link with room for one more.
            if (ranged and running < settings.segments and d.frontier >= d.total.? and d.linkHasRoom(now)) {
                if (d.steal(scope, segments, now)) |made| {
                    if (made) |seg| {
                        try segments.append(gpa, seg);
                        d.acted(now);
                    }
                } else |err| {
                    w.post(.{ .note = .{ .id = d.id, .text = .fmt("steal failed: {t}", .{err}) } });
                }
            }

            if (bytes != last_reported) {
                last_reported = bytes;
                // The running ones first, then the newest of the rest:
                // with chunks handed out as connections finish there are
                // more segments than bars, and the finished ones say least.
                var view: [max_shown_segments]SegView = @splat(.{ .len = 0, .done = 0 });
                var shown: usize = 0;
                for (segments.items) |seg| if (shown < max_shown_segments and seg.state.load(.acquire) == .running) {
                    view[shown] = .{ .len = seg.len() orelse 0, .done = seg.have() };
                    shown += 1;
                };
                var back = segments.items.len;
                while (shown < max_shown_segments and back > 0) {
                    back -= 1;
                    const seg = segments.items[back];
                    if (seg.state.load(.acquire) == .running) continue;
                    view[shown] = .{ .len = seg.len() orelse 0, .done = seg.have() };
                    shown += 1;
                }
                w.post(.{ .progress = .{ .id = d.id, .bytes = bytes, .seg = view, .seg_count = @intCast(shown) } });
            }
            if (now - last_saved_ms >= 1000) {
                last_saved_ms = now;
                persist(d, scope, segments.items);
            }
            // Fifty milliseconds: the gap between a chunk ending and the
            // next one starting on that connection is this plus a round
            // trip, and the loop is a few atomics.
            try Io.sleep(io, Io.Duration.fromMilliseconds(50), .awake);
        }
    }

    /// One more segment off the frontier, started at once. The size is
    /// what the connection that last finished did in `chunk_secs`, within
    /// a megabyte and a sixteenth of the file — at the first, the plan's
    /// own chunk.
    fn extend(d: *Download, scope: *store.Run, segments: *std.ArrayList(*Segment), now: i64, file: Io.File, url: []const u8, headers: Headers) !void {
        const s = d.shared;
        const settings = s.worker.settings;
        const total = d.total.?;
        var rate: f64 = 0;
        var latest: i64 = 0;
        var first_len: u64 = 0;
        for (segments.items) |seg| {
            if (seg == segments.items[0]) first_len = seg.len() orelse 0;
            if (seg.state.load(.acquire) == .ok and seg.finished_ms >= latest and seg.finished_ms > seg.started_ms) {
                latest = seg.finished_ms;
                rate = @as(f64, @floatFromInt(seg.have())) / (@as(f64, @floatFromInt(seg.finished_ms - seg.started_ms)) / 1000.0);
            }
        }
        // Never more than a sixteenth of the file, nor a thirty-second of
        // what is left to hand out — so the last chunks are small and the
        // connections finish together rather than one of them last with
        // eight seconds of work. A quarter of a megabyte at the least: a
        // request's round trip is worth that much.
        const unassigned = total - d.frontier;
        const cap = @max(settings.min_segment, total / 16);
        const want: u64 = if (rate > 0) @intFromFloat(rate * settings.chunk_secs) else first_len;
        const size = @min(@max(256 << 10, @min(want, cap, unassigned / (2 * @as(u64, settings.segments)))), unassigned);
        const row = try store.extend(s.db, scope, d.id, @intCast(segments.items.len), @intCast(d.frontier), @intCast(d.frontier + size));
        scope.reset();
        const made = try Segment.create(s.worker.gpa, row);
        errdefer s.worker.gpa.destroy(made);
        try segments.append(s.worker.gpa, made);
        d.frontier += size;
        made.attempts = 1;
        made.last_seen = 0;
        made.last_moved_ms = now;
        made.started_ms = now;
        made.state.store(.running, .release);
        made.future = try s.io.concurrent(Segment.run, .{ made, s.client, file, s.io, url, headers, true });
    }

    /// The first action of a burst is the one judged; the ones that follow
    /// within two seconds are the same decision.
    fn acted(d: *Download, now: i64) void {
        if (d.acted_ms != 0) return;
        d.acted_ms = now;
        d.acted_total = d.total_rate;
    }

    /// Cancel a running segment that is far behind the others, so its next
    /// attempt gets a new connection. True when one was. Bounded: not within five seconds of
    /// its last reconnect, not more than four times, and never one that has
    /// under a megabyte to go — that finishes sooner than it reconnects.
    ///
    /// Called only when `linkHasRoom`: on a shared link the one at
    /// 80 KB/s is slow because the others are fast.
    fn reconnectSlow(d: *Download, segments: []const *Segment, now: i64) bool {
        // The yardstick: every segment that ran for three seconds or more,
        // including ones that finished in the last ten — the last segment
        // standing has nothing else to be compared with.
        var sum: f64 = 0;
        var n: usize = 0;
        for (segments) |seg| {
            switch (seg.state.load(.acquire)) {
                .running => if (now - seg.started_ms < 3000) continue,
                .ok => if (now - seg.finished_ms > 10_000 or seg.rate <= 0) continue,
                else => continue,
            }
            sum += seg.rate;
            n += 1;
        }
        if (n < 2) return false;
        const mean = sum / @as(f64, @floatFromInt(n));
        if (mean <= 0) return false;
        const settings = d.shared.worker.settings;
        if (settings.slow_fraction <= 0) return false;
        var reconnected: u8 = 0;
        for (segments) |seg| {
            if (seg.state.load(.acquire) != .running or now - seg.started_ms < 3000) continue;
            if (seg.rate >= settings.slow_fraction * mean) {
                seg.slow_checks = 0;
                continue;
            }
            // Not one that finishes sooner than a reconnect would: under
            // three seconds to go at the mean rate.
            if (@as(f64, @floatFromInt(seg.remaining())) / mean < 3) continue;
            if (seg.restarts >= 4 or now - seg.last_restart_ms < 5000) continue;
            seg.slow_checks +|= 1;
            if (seg.slow_checks < settings.slow_checks) continue;
            if (reconnected == settings.slow_per_check) break;
            reconnected += 1;
            seg.slow_checks = 0;
            seg.future.?.cancel(d.shared.io);
            seg.future = null;
            seg.restarts += 1;
            seg.last_restart_ms = now;
            seg.state.store(.idle, .release);
            d.shared.worker.post(.{ .note = .{ .id = d.id, .text = .fmt("segment {d}: {d} KB/s against {d} KB/s, reconnecting", .{ seg.index, @as(u64, @intFromFloat(seg.rate / 1024)), @as(u64, @intFromFloat(mean / 1024)) }) } });
        }
        return reconnected > 0;
    }

    /// Give a new segment the second half of what the running segment with
    /// the most left still has to do, or null when nobody has enough.
    fn steal(d: *Download, scope: *store.Run, segments: *std.ArrayList(*Segment), now: i64) !?*Segment {
        const settings = d.shared.worker.settings;
        // The victim is the one that would take longest to finish on its
        // own, judged by its own rate — a connection at 300 KB/s with
        // half a megabyte left is a worse place to be than one at 10 MB/s
        // with four. A segment under two seconds old has no rate yet.
        var victim: ?*Segment = null;
        var victim_secs: f64 = 0;
        var rate_sum: f64 = 0;
        var rate_n: f64 = 0;
        for (segments.items) |seg| {
            if (seg.state.load(.acquire) != .running or now - seg.started_ms < 2000) continue;
            rate_sum += seg.rate;
            rate_n += 1;
            const secs = @as(f64, @floatFromInt(seg.remaining())) / @max(seg.rate, 1);
            if (secs < settings.steal_min_secs) continue;
            if (victim == null or secs > victim_secs) {
                victim = seg;
                victim_secs = secs;
            }
        }
        const v = victim orelse return null;

        // Ahead of the victim by a margin, so the boundary is still in
        // front of it once written: a fifth of a second at its rate, and
        // never under what one chunk holds.
        const margin: u64 = @max(128 << 10, @as(u64, @intFromFloat(v.rate * 0.2)));
        const pos = v.start + v.have() + margin;
        const old_end = v.end.load(.acquire);
        if (pos >= old_end) return null;

        // Split so that both finish together, by what each can be
        // expected to do: the victim at its own rate, the thief at what a
        // connection here has been getting — the running mean, or the
        // usual second's share when the mean is what is left of a slow
        // tail. Equal rates is half; a 300 KB/s victim against a 1 MB/s
        // thief hands over three quarters.
        const expected = @max(if (rate_n > 0) rate_sum / rate_n else 0, d.usualRate() / @as(f64, @floatFromInt(settings.segments)), 1);
        const left = @as(f64, @floatFromInt(old_end - pos));
        const take: u64 = @intFromFloat(left * expected / (expected + @max(v.rate, 1)));
        if (take < (256 << 10)) return null;
        const mid = old_end - take;

        const row = try store.split(d.shared.db, scope, d.id, v.row_id, @intCast(mid), @intCast(segments.items.len), @intCast(old_end));
        defer scope.reset();
        v.end.store(mid, .release);
        const made = try Segment.create(d.shared.worker.gpa, row);
        d.shared.worker.post(.{ .note = .{ .id = d.id, .text = .fmt("segment {d} takes {d} KB from segment {d} at {d} KB/s", .{ made.index, (old_end - mid) >> 10, v.index, @as(u64, @intFromFloat(v.rate / 1024)) }) } });
        return made;
    }

    /// Write down what each segment has, for the ones that moved. A write
    /// that fails is a note rather than a failed download: the bytes are
    /// on disk either way, and the next save gets another chance.
    fn persist(d: *Download, scope: *store.Run, segments: []const *Segment) void {
        for (segments) |seg| {
            const done = seg.have();
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

const Probe = struct {
    total: ?u64,
    ranged: bool,
    etag: ?Text,
    /// `Last-Modified` as sent, for the file's own time once it is whole.
    last_modified: ?Text,
    /// The name `Content-Disposition` offered, when it did and it was a
    /// bare file name.
    filename: ?Text,
    /// Where the redirects ended, when they went anywhere. The segments
    /// ask there directly: `mirrors.kernel.org` answers every request
    /// with a 301 to its edge and lets about eight handshakes a second
    /// through, so sixteen segments each following it themselves spent
    /// five seconds arriving and left curl, which follows once, well
    /// ahead. Points into the buffer the caller handed `probe`.
    location: ?[]const u8,
};

/// One byte, asked for with a Range. A 206 says the server can slice and
/// how big the whole is; a 200 says it cannot, and the body it started
/// sending is dropped rather than drained. Either way the answer carries
/// what identifies the object, for the next run to compare against.
fn probe(d: *Download, url: []const u8, headers: Headers, location_buf: *[2048]u8) !Probe {
    var transfer: [4096]u8 = undefined;
    var redirect: [2048]u8 = undefined;
    var hbuf: [Headers.max + 1]std.http.Header = undefined;
    var ex: fetch.Exchange = .idle;
    defer ex.end();

    const head = try ex.begin(d.shared.client, .{
        .method = .GET,
        .url = url,
        .headers = headers.with(.{ .name = "range", .value = "bytes=0-0" }, &hbuf),
        .authorization = headers.authorization,
        .host = headers.host,
        .user_agent = headers.user_agent,
        .redirect_buffer = &redirect,
        .transfer_buffer = &transfer,
    });
    const last_modified: ?Text = if (head.header("last-modified")) |m| .from(m) else null;
    const etag: ?Text = if (head.header("etag")) |e| .from(e) else last_modified;
    var name_buf: [Text.max]u8 = undefined;
    const filename: ?Text = if (head.header("content-disposition")) |cd|
        (if (filenameFromDisposition(cd, &name_buf)) |n| .from(n) else null)
    else
        null;
    // `std.http.Client` rewrites the request's URI as it follows each
    // redirect, into `redirect` above — so it is copied out here, while
    // that buffer is still alive.
    const location: ?[]const u8 = blk: {
        var w: Io.Writer = .fixed(location_buf);
        ex.req.uri.format(&w) catch break :blk null;
        const final = w.buffered();
        break :blk if (std.mem.eql(u8, final, url)) null else final;
    };
    switch (head.status) {
        .partial_content => {
            const cr = head.header("content-range") orelse return error.NoContentRange;
            return .{ .total = try totalFromContentRange(cr), .ranged = true, .etag = etag, .last_modified = last_modified, .filename = filename, .location = location };
        },
        .ok => return .{ .total = head.content_length, .ranged = false, .etag = etag, .last_modified = last_modified, .filename = filename, .location = location },
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
    /// Exclusive, and atomic: a steal moves it while the task runs, and
    /// the task reads it before every chunk. `maxInt` when the length is
    /// unknown, which is also the unranged case.
    end: std.atomic.Value(u64),
    /// Bytes that have reached the file, counted from `start`. The task
    /// stores; the supervisor reads. May briefly exceed the length after a
    /// steal moved `end` behind it, which is why `have` clamps.
    done: std.atomic.Value(u64) = .init(0),
    /// What the database last heard.
    saved: u64 = 0,
    /// `.running` until the task writes its verdict, which it does before
    /// returning so the supervisor can `await` without ever blocking.
    state: std.atomic.Value(SegState) = .init(.idle),
    err: anyerror = error.None,
    /// Every start, for showing; and the ones that count against `retries`.
    attempts: u8 = 0,
    failures: u8 = 0,
    /// Reconnects for being slow, which count against nothing but a cap.
    restarts: u8 = 0,
    /// Not before this, after a failure: `Settings.retry_wait_ms`.
    wait_until_ms: i64 = 0,
    /// Consecutive health checks this segment was under the bar.
    slow_checks: u8 = 0,
    last_restart_ms: i64 = 0,
    future: ?Io.Future(void) = null,
    last_seen: u64 = 0,
    last_moved_ms: i64 = 0,
    started_ms: i64 = 0,
    finished_ms: i64 = 0,
    /// Bytes per second over the last sample, for the health check. Kept
    /// after the segment finishes, so a segment that is the last one left
    /// is still measured against the ones that just ended.
    rate: f64 = 0,
    sample_bytes: u64 = 0,

    const SegState = enum(u8) { idle, running, ok, failed };

    fn create(gpa: std.mem.Allocator, row: store.Segment) !*Segment {
        const seg = try gpa.create(Segment);
        seg.* = .{
            .row_id = row.id,
            .index = @intCast(row.idx),
            .start = @intCast(row.start),
            .end = .init(if (row.stop == std.math.maxInt(i64)) std.math.maxInt(u64) else @intCast(row.stop)),
            .done = .init(@intCast(row.done)),
            .saved = @intCast(row.done),
        };
        return seg;
    }

    fn len(s: *const Segment) ?u64 {
        const end = s.end.load(.acquire);
        return if (end == std.math.maxInt(u64)) null else end - s.start;
    }

    /// What is in the file for this segment, never more than its length.
    fn have(s: *const Segment) u64 {
        const done = s.done.load(.monotonic);
        return if (s.len()) |n| @min(done, n) else done;
    }

    fn remaining(s: *const Segment) u64 {
        return if (s.len()) |n| n - s.have() else std.math.maxInt(u64);
    }

    /// The task: one Range request for what this segment still lacks,
    /// written at its offset. Its verdict goes into the segment, not the
    /// return value, so that the supervisor can see it without blocking.
    fn run(seg: *Segment, client: *fetch.Client, file: Io.File, io: Io, url: []const u8, headers: Headers, ranged: bool) void {
        seg.state.store(.running, .release);
        runInner(seg, client, file, io, url, headers, ranged) catch |err| {
            seg.err = err;
            seg.state.store(.failed, .release);
            return;
        };
        seg.state.store(.ok, .release);
    }

    fn runInner(seg: *Segment, client: *fetch.Client, file: Io.File, io: Io, url: []const u8, headers: Headers, ranged: bool) !void {
        // Both buffers live on this task's stack for the life of the
        // transfer. The writer's buffer is not optional: `std.Io.net`'s
        // stream writes straight into it and asserts on an empty one.
        var transfer: [64 << 10]u8 = undefined;
        var wbuf: [64 << 10]u8 = undefined;
        var redirect: [2048]u8 = undefined;
        var range_buf: [64]u8 = undefined;
        var hbuf: [Headers.max + 1]std.http.Header = undefined;

        const from = seg.start + seg.have();
        // Read once: a steal can move `end` between the request and its
        // answer, and the answer is measured against what was asked. The
        // read loop below follows the atomic and stops at the new boundary.
        const asked_end = seg.end.load(.acquire);
        const range = try std.fmt.bufPrint(&range_buf, "bytes={d}-{d}", .{ from, asked_end -| 1 });
        const sent = headers.with(if (ranged) .{ .name = "range", .value = range } else null, &hbuf);

        var ex: fetch.Exchange = .idle;
        defer ex.end();
        const head = try ex.begin(client, .{
            .method = .GET,
            .url = url,
            .headers = sent,
            .authorization = headers.authorization,
            .host = headers.host,
            .user_agent = headers.user_agent,
            .redirect_buffer = &redirect,
            .transfer_buffer = &transfer,
        });

        if (ranged) {
            // A 200 here is the whole file; writing it at `from` would
            // corrupt everything after it. Refuse before a byte lands.
            if (head.status != .partial_content) return error.RangeIgnored;
            if (head.content_length) |n| if (n != asked_end - from) return error.LengthMismatch;
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
        // Never past `end`, read fresh each time: a steal may have moved it
        // closer since the request was made. What the server still sends
        // after that is dropped with the connection.
        const reader = ex.reader orelse unreachable;
        while (true) {
            const end = seg.end.load(.acquire);
            const want: u64 = end - @min(end, fw.logicalPos());
            if (want == 0) break;
            _ = reader.stream(&fw.interface, .limited64(want)) catch |err| switch (err) {
                error.EndOfStream => break,
                // The file's own error is the one worth reporting — a
                // full disk is `NoSpaceLeft` there and `WriteFailed` here.
                error.WriteFailed => return fw.err orelse err,
                else => return err,
            };
            seg.done.store(fw.pos - seg.start, .monotonic);
        }
        fw.interface.flush() catch |err| return fw.err orelse err;
        seg.done.store(fw.pos - seg.start, .monotonic);

        if (seg.len()) |want| if (fw.pos - seg.start < want) return error.ShortBody;
    }
};

// ---------------------------------------------------------------- headers

/// What goes out with every request for one download, read off the row.
/// Three names are kept apart because `std.http.Client` writes them itself
/// and would otherwise send them twice; the rest go verbatim, in order.
const Headers = struct {
    extra: []const std.http.Header = &.{},
    authorization: ?[]const u8 = null,
    host: ?[]const u8 = null,
    user_agent: ?[]const u8 = null,

    /// More than a browser's "Copy as cURL" produces.
    const max = 32;

    /// `Name: value` per line. The result points into `lines`, which has
    /// to outlive it; a line without a colon is skipped.
    fn parse(gpa: std.mem.Allocator, lines: []const u8) !Headers {
        var list: std.ArrayList(std.http.Header) = .empty;
        errdefer list.deinit(gpa);
        var h: Headers = .{};
        var it = std.mem.splitScalar(u8, lines, '\n');
        while (it.next()) |line| {
            const colon = std.mem.indexOfScalar(u8, line, ':') orelse continue;
            const name = std.mem.trim(u8, line[0..colon], " \t\r");
            const value = std.mem.trim(u8, line[colon + 1 ..], " \t\r");
            if (name.len == 0) continue;
            if (std.ascii.eqlIgnoreCase(name, "authorization")) {
                h.authorization = value;
            } else if (std.ascii.eqlIgnoreCase(name, "host")) {
                h.host = value;
            } else if (std.ascii.eqlIgnoreCase(name, "user-agent")) {
                h.user_agent = value;
            } else if (std.ascii.eqlIgnoreCase(name, "connection") or
                std.ascii.eqlIgnoreCase(name, "accept-encoding") or
                std.ascii.eqlIgnoreCase(name, "content-length"))
            {
                // The client decides these: the body is read uncompressed,
                // the connection is kept, and there is no body to measure.
                continue;
            } else {
                if (list.items.len == max) return error.TooManyHeaders;
                try list.append(gpa, .{ .name = name, .value = value });
            }
        }
        h.extra = try list.toOwnedSlice(gpa);
        return h;
    }

    fn free(h: Headers, gpa: std.mem.Allocator) void {
        gpa.free(h.extra);
    }

    /// `first`, then the rest, in `buf` — which holds `max + 1`.
    fn with(h: Headers, first: ?std.http.Header, buf: []std.http.Header) []const std.http.Header {
        var n: usize = 0;
        if (first) |f| {
            buf[n] = f;
            n += 1;
        }
        for (h.extra) |e| {
            buf[n] = e;
            n += 1;
        }
        return buf[0..n];
    }
};

/// The name in `attachment; filename="a.bin"` or in
/// `filename*=UTF-8''a%20b.bin`, the starred form first when both are
/// there. A bare name only: a server does not get to choose the directory,
/// so anything with a separator, or `.` or `..`, is no name at all.
fn filenameFromDisposition(value: []const u8, buf: []u8) ?[]const u8 {
    var plain: ?[]const u8 = null;
    var starred: ?[]const u8 = null;
    var params = std.mem.splitScalar(u8, value, ';');
    while (params.next()) |param| {
        const eq = std.mem.indexOfScalar(u8, param, '=') orelse continue;
        const key = std.mem.trim(u8, param[0..eq], " \t");
        const raw = std.mem.trim(u8, param[eq + 1 ..], " \t");
        if (std.ascii.eqlIgnoreCase(key, "filename*")) {
            // charset'lang'percent-encoded
            const q = std.mem.indexOf(u8, raw, "''") orelse continue;
            starred = raw[q + 2 ..];
        } else if (std.ascii.eqlIgnoreCase(key, "filename")) {
            plain = std.mem.trim(u8, raw, "\"");
        }
    }
    const chosen = starred orelse plain orelse return null;
    if (chosen.len > buf.len) return null;
    var n: usize = 0;
    var i: usize = 0;
    while (i < chosen.len) : (i += 1) {
        if (chosen[i] == '%' and i + 2 < chosen.len) {
            buf[n] = std.fmt.parseInt(u8, chosen[i + 1 .. i + 3], 16) catch {
                buf[n] = '%';
                n += 1;
                continue;
            };
            i += 2;
        } else buf[n] = chosen[i];
        n += 1;
    }
    const name = buf[0..n];
    if (name.len == 0 or std.mem.eql(u8, name, ".") or std.mem.eql(u8, name, "..")) return null;
    for (name) |c| if (c == '/' or c == '\\' or c == 0) return null;
    return name;
}

/// `Tue, 15 Nov 1994 12:45:26 GMT` as seconds since the epoch — the one
/// form HTTP/1.1 sends. Anything else is null, which is not worth failing
/// a download over.
fn parseHttpDate(s: []const u8) ?i64 {
    var it = std.mem.tokenizeAny(u8, s, " ,:");
    _ = it.next() orelse return null; // weekday
    const day = std.fmt.parseInt(i64, it.next() orelse return null, 10) catch return null;
    const mon = monthOf(it.next() orelse return null) orelse return null;
    const year = std.fmt.parseInt(i64, it.next() orelse return null, 10) catch return null;
    const hour = std.fmt.parseInt(i64, it.next() orelse return null, 10) catch return null;
    const min = std.fmt.parseInt(i64, it.next() orelse return null, 10) catch return null;
    const sec = std.fmt.parseInt(i64, it.next() orelse return null, 10) catch return null;
    if (day < 1 or day > 31 or hour > 23 or min > 59 or sec > 60) return null;
    return daysFromCivil(year, mon, day) * 86_400 + hour * 3600 + min * 60 + sec;
}

fn monthOf(name: []const u8) ?i64 {
    const months = [_][]const u8{ "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec" };
    for (months, 1..) |m, i| if (std.ascii.eqlIgnoreCase(m, name)) return @intCast(i);
    return null;
}

/// Days since 1970-01-01 for a proleptic Gregorian date; Howard Hinnant's
/// `days_from_civil`, which is what every libc does.
fn daysFromCivil(year: i64, month: i64, day: i64) i64 {
    const y = if (month <= 2) year - 1 else year;
    const era = @divFloor(y, 400);
    const yoe = y - era * 400;
    const mp = if (month > 2) month - 3 else month + 9;
    const doy = @divFloor(153 * mp + 2, 5) + day - 1;
    const doe = yoe * 365 + @divFloor(yoe, 4) - @divFloor(yoe, 100) + doy;
    return era * 146_097 + doe - 719_468;
}

// ---------------------------------------------------------------- verify

/// The whole file, hashed from the front, as lowercase hex. One pass
/// after the network is done, so it is disk-bound and costs nothing per
/// byte received.
fn sha256Of(file: Io.File, io: Io) ![64]u8 {
    var buf: [64 << 10]u8 = undefined;
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    var at: u64 = 0;
    while (true) {
        const n = try file.readPositionalAll(io, &buf, at);
        hasher.update(buf[0..n]);
        at += n;
        if (n < buf.len) break;
    }
    var digest: [32]u8 = undefined;
    hasher.final(&digest);
    return std.fmt.bytesToHex(digest, .lower);
}

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

test "headers: authorization, host and user-agent are set apart, the client's own are dropped, the rest kept in order" {
    const gpa = std.testing.allocator;
    const h = try Headers.parse(gpa, "Cookie: a=b\nAuthorization: Bearer t\nHost: x.y\nConnection: close\nuser-agent: Mozilla/5.0\nnocolon\nReferer: https://x.y/\n");
    defer h.free(gpa);
    try std.testing.expectEqualStrings("Bearer t", h.authorization.?);
    try std.testing.expectEqualStrings("x.y", h.host.?);
    try std.testing.expectEqualStrings("Mozilla/5.0", h.user_agent.?);
    try std.testing.expectEqual(@as(usize, 2), h.extra.len);
    try std.testing.expectEqualStrings("Cookie", h.extra[0].name);
    try std.testing.expectEqualStrings("a=b", h.extra[0].value);
    try std.testing.expectEqualStrings("Referer", h.extra[1].name);

    var buf: [Headers.max + 1]std.http.Header = undefined;
    const sent = h.with(.{ .name = "range", .value = "bytes=0-0" }, &buf);
    try std.testing.expectEqual(@as(usize, 3), sent.len);
    try std.testing.expectEqualStrings("range", sent[0].name);
    try std.testing.expectEqual(@as(usize, 2), h.with(null, &buf).len);
}

test "content-disposition: a plain name, a starred one over it, and no path" {
    var buf: [Text.max]u8 = undefined;
    try std.testing.expectEqualStrings("a.bin", filenameFromDisposition("attachment; filename=\"a.bin\"", &buf).?);
    try std.testing.expectEqualStrings("a.bin", filenameFromDisposition("inline; filename=a.bin", &buf).?);
    try std.testing.expectEqualStrings("a b.bin", filenameFromDisposition("attachment; filename=\"x.bin\"; filename*=UTF-8''a%20b.bin", &buf).?);
    try std.testing.expect(filenameFromDisposition("attachment; filename=\"../etc/passwd\"", &buf) == null);
    try std.testing.expect(filenameFromDisposition("attachment; filename*=UTF-8''..%2Fx", &buf) == null);
    try std.testing.expect(filenameFromDisposition("attachment", &buf) == null);
    try std.testing.expect(filenameFromDisposition("attachment; filename=\"\"", &buf) == null);
}

test "an http date is seconds since the epoch, and anything else is nothing" {
    try std.testing.expectEqual(@as(?i64, 784_903_526), parseHttpDate("Tue, 15 Nov 1994 12:45:26 GMT"));
    try std.testing.expectEqual(@as(?i64, 0), parseHttpDate("Thu, 01 Jan 1970 00:00:00 GMT"));
    try std.testing.expectEqual(@as(?i64, 1_709_164_800), parseHttpDate("Thu, 29 Feb 2024 00:00:00 GMT"));
    try std.testing.expectEqual(@as(?i64, null), parseHttpDate("yesterday"));
    try std.testing.expectEqual(@as(?i64, null), parseHttpDate("Tue, 15 Nov 1994"));
}

test "a content-range total is parsed, and a star is unknown" {
    try std.testing.expectEqual(@as(?u64, 12345), try totalFromContentRange("bytes 0-0/12345"));
    try std.testing.expectEqual(@as(?u64, null), try totalFromContentRange("bytes 0-0/*"));
}

test "the link has room when the total falls under the usual second, and a burst is not the usual" {
    var d: Download = .{ .shared = undefined, .id = 0 };
    // Three seconds are needed before anything is judged.
    d.totals[0] = 100;
    d.totals_n = 1;
    d.total_rate = 10;
    try std.testing.expect(!d.linkHasRoom(0));
    // A burst second, then the rate the link sustains: the usual is the
    // median, so a steady 10 after a 100 is not room.
    for ([_]f64{ 100, 10, 10, 10 }) |t| {
        d.totals[d.totals_n % d.totals.len] = t;
        d.totals_n += 1;
    }
    d.prev_total_rate = 10;
    d.total_rate = 10;
    try std.testing.expectEqual(@as(f64, 10), d.usualRate());
    try std.testing.expect(!d.linkHasRoom(0));
    // Down to a quarter for one second: room. Down to 0.7 for one: not
    // yet; for two: room.
    d.total_rate = 2.5;
    try std.testing.expect(d.linkHasRoom(0));
    d.total_rate = 7;
    try std.testing.expect(!d.linkHasRoom(0));
    d.prev_total_rate = 7;
    try std.testing.expect(d.linkHasRoom(0));
    // And not while holding.
    d.hold_until_ms = 5000;
    try std.testing.expect(!d.linkHasRoom(4999));
    try std.testing.expect(d.linkHasRoom(5000));
}
