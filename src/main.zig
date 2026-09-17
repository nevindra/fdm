//! fdm — the headless spike.
//!
//! One question, asked with code: can a download manager be built on
//! `nilo_fetch` running on `std.Io.Threaded`, with no nilo Engine anywhere?
//! That is the environment a Native SDK worker thread has (its
//! `channel-monitor` example spins up exactly this Io on its own thread), so
//! if it holds here it holds under a window.
//!
//! Three things have to be true, and each is checked rather than assumed:
//!
//! 1. **Segments.** N `Range` requests in flight through one `fetch.Client`,
//!    each writing its slice of the file at its own offset with a positional
//!    `File.Writer`, so no two tasks share a seek position.
//! 2. **The server actually honoured the Range.** A 200 where a 206 was
//!    asked for is the whole body, and appending it would corrupt the file.
//!    Every segment refuses that; the probe decides up front whether ranges
//!    are worth asking for at all.
//! 3. **A stalled socket can be cancelled from outside.** Under `.off`
//!    limits nilo's own deadline never fires — that needs the Engine — so
//!    the watchdog here is the main task: it reads each segment's byte
//!    counter, and a segment that has not moved for `--stall` milliseconds
//!    gets `Future.cancel`, which `std.Io.Threaded` delivers as a signal
//!    that interrupts the blocking `recv`. Whether that comes back is the
//!    finding this file exists to produce, so the log says when it happens.
//!
//! Progress is a counter per segment rather than a callback: `Exchange.pipe`
//! has none, and looping `reader.stream` by hand is what `pipe` does anyway.

const std = @import("std");
const fetch = @import("nilo_fetch");

const Io = std.Io;

const Options = struct {
    url: []const u8,
    out: ?[]const u8 = null,
    segments: u8 = 4,
    /// Milliseconds a segment may go without a byte before it is cancelled.
    stall_ms: u32 = 10_000,
    /// How many times one segment may be restarted after a stall or an error.
    retries: u8 = 3,
    /// Below this, splitting costs more handshakes than it saves.
    min_segment: u64 = 1 << 20,
};

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const args = try init.minimal.args.toSlice(init.arena.allocator());
    const opts = parseArgs(args) orelse {
        std.debug.print(
            \\usage: fdm <url> [-o file] [-n segments] [--stall ms] [--retries n]
            \\
        , .{});
        return error.Usage;
    };

    // The same Io a Native SDK worker thread would make for itself.
    var threaded: Io.Threaded = .init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var client: fetch.Client = .init(gpa, .{
        .max_in_flight = 16,
        // The deadline cannot fire without an Engine, so zero is honest; the
        // stall watchdog below is the bound instead.
        .timeout_ms = 0,
        // A refused body is dropped rather than drained: the probe leaves a
        // whole file unread when the server ignores its Range.
        .max_drain = 4 << 10,
    });
    defer client.deinit();
    try client.nilo_start(io, .off);

    const info = try probe(&client, opts.url);
    const out_name = opts.out orelse nameFromUrl(opts.url);
    std.debug.print("{s}\n  total: {?d} bytes, ranges: {s}\n  -> {s}\n", .{
        opts.url, info.total, if (info.ranged) "yes" else "no", out_name,
    });

    const file = try Io.Dir.cwd().createFile(io, out_name, .{ .truncate = true });
    defer file.close(io);

    // Split only when the server can serve slices and the file is big enough
    // for a slice to be worth its handshake.
    const total = info.total;
    const count: usize = if (info.ranged and total != null and total.? >= opts.min_segment * 2)
        @intCast(@min(@as(u64, opts.segments), total.? / opts.min_segment))
    else
        1;
    const ranged = info.ranged and count > 1;
    if (total) |n| try file.setLength(io, n);

    const segments = try gpa.alloc(Segment, count);
    defer gpa.free(segments);
    for (segments, 0..) |*s, i| {
        const per = if (total) |n| n / count else std.math.maxInt(u64);
        s.* = .{
            .index = i,
            .start = per * i,
            .end = if (i + 1 == count) (total orelse std.math.maxInt(u64)) else per * (i + 1),
        };
    }

    const started = nowMs(io);
    try supervise(io, &client, file, opts, segments, ranged);
    const elapsed = nowMs(io) - started;

    var bytes: u64 = 0;
    var attempts: u32 = 0;
    for (segments) |*s| {
        bytes += s.done.load(.monotonic);
        attempts += s.attempts;
    }
    const secs = @as(f64, @floatFromInt(@max(elapsed, 1))) / 1000.0;
    std.debug.print("\ndone: {d} bytes in {d:.2}s = {d:.1} MB/s, {d} segments, {d} attempts\n", .{
        bytes, secs, @as(f64, @floatFromInt(bytes)) / secs / 1e6, count, attempts,
    });
    if (total) |n| if (bytes != n) {
        std.debug.print("short: expected {d}\n", .{n});
        return error.ShortDownload;
    };
}

// ------------------------------------------------------------------ probe

const Probe = struct { total: ?u64, ranged: bool };

/// One byte, asked for with a Range. A 206 says the server can slice and
/// how big the whole is; a 200 says it cannot, and the body it started
/// sending is dropped rather than drained.
fn probe(client: *fetch.Client, url: []const u8) !Probe {
    var transfer: [4096]u8 = undefined;
    var redirect: [2048]u8 = undefined;
    var ex: fetch.Exchange = .idle;
    defer ex.end();

    const head = try ex.begin(client, .{
        .method = .GET,
        .url = url,
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
            std.debug.print("probe: {d} {s}\n", .{ @intFromEnum(head.status), head.status.phrase() orelse "" });
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
    /// Bytes handed to the file writer so far, counted from `start`. The
    /// task adds; the supervisor reads.
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
};

/// The task: one Range request for what this segment still lacks, written
/// at its offset. Its verdict goes into the segment, not the return value,
/// so that the supervisor can see it without blocking.
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
    // Both buffers live on this task's stack for the life of the transfer.
    // 64 KiB each is the "bigger is fewer trips" end of nilo's own note on
    // `transfer_buffer`; a real manager would size these against how many
    // segments it means to hold open. The writer's buffer is not optional:
    // `std.Io.net`'s stream writes straight into it and asserts on an
    // empty one.
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
        // A 200 here is the whole file; writing it at `from` would corrupt
        // everything after it. Refuse before a byte lands.
        if (head.status != .partial_content) return error.RangeIgnored;
        if (head.content_length) |n| if (n != seg.end - from) return error.LengthMismatch;
    } else if (head.status != .ok) return error.BadStatus;

    var fw = file.writer(io, &wbuf);
    fw.pos = from;

    // **`done` is `fw.pos`, which only moves when a positional write has
    // returned — never what `stream` said it consumed.** The first version
    // counted bytes as they entered the writer's buffer; a cancelled task
    // then lost whatever was buffered, the counter said otherwise, and the
    // retry resumed past a hole: 20,000,000 bytes and the wrong hash. On
    // every way out, what is buffered is flushed if it can be, and what is
    // in the file is what the next attempt resumes from.
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

// ------------------------------------------------------------- supervisor

/// The main task. Starts every segment, restarts the ones that fail, and
/// cancels the ones that stop moving — which is the one thing a deadline
/// would do and, without an Engine, nothing else here does.
fn supervise(io: Io, client: *fetch.Client, file: Io.File, opts: Options, segments: []Segment, ranged: bool) !void {
    var last_report_ms: i64 = 0;
    var last_report_bytes: u64 = 0;

    while (true) {
        const now = nowMs(io);
        var pending: usize = 0;
        var bytes: u64 = 0;

        for (segments) |*seg| {
            bytes += seg.done.load(.monotonic);
            switch (seg.state.load(.acquire)) {
                .ok => {
                    // Finished: the await returns at once and frees the
                    // future, which is the Threaded allocation behind it.
                    if (seg.future) |*f| {
                        f.await(io);
                        seg.future = null;
                    }
                },
                .idle, .failed => {
                    if (seg.future) |*f| {
                        f.await(io); // finished already: returns at once
                        seg.future = null;
                    }
                    if (seg.state.load(.acquire) == .failed) {
                        std.debug.print("\nsegment {d}: attempt {d} failed: {t}\n", .{ seg.index, seg.attempts, seg.err });
                    }
                    if (seg.attempts > opts.retries) {
                        std.debug.print("segment {d}: giving up after {d} attempts\n", .{ seg.index, seg.attempts });
                        return seg.err;
                    }
                    seg.attempts += 1;
                    seg.last_seen = seg.done.load(.monotonic);
                    seg.last_moved_ms = now;
                    seg.state.store(.running, .release);
                    seg.future = try io.concurrent(run, .{ seg, client, file, io, opts.url, ranged });
                    pending += 1;
                },
                .running => {
                    pending += 1;
                    const seen = seg.done.load(.monotonic);
                    if (seen != seg.last_seen) {
                        seg.last_seen = seen;
                        seg.last_moved_ms = now;
                    } else if (now - seg.last_moved_ms > opts.stall_ms) {
                        // The finding. If this line is followed by
                        // "cancelled", `std.Io.Threaded` interrupted a
                        // blocking recv from another thread and the
                        // watchdog design holds. If nothing follows it, it
                        // does not, and that has to be known before a
                        // window is put on top.
                        std.debug.print("\nsegment {d}: no bytes for {d}ms at {d}/{?d}, cancelling...", .{
                            seg.index, now - seg.last_moved_ms, seen, seg.len(),
                        });
                        const t0 = nowMs(io);
                        seg.future.?.cancel(io);
                        seg.future = null;
                        std.debug.print(" cancelled in {d}ms ({t})\n", .{ nowMs(io) - t0, seg.err });
                        // Whatever verdict the task wrote, the supervisor's
                        // is that it should run again from where it got to.
                        seg.state.store(.idle, .release);
                    }
                },
            }
        }

        if (pending == 0) return;

        if (now - last_report_ms >= 500) {
            const dt = @as(f64, @floatFromInt(@max(now - last_report_ms, 1))) / 1000.0;
            const rate = @as(f64, @floatFromInt(bytes -| last_report_bytes)) / dt / 1e6;
            last_report_ms = now;
            last_report_bytes = bytes;
            report(segments, bytes, rate);
        }

        try Io.sleep(io, Io.Duration.fromMilliseconds(100), .awake);
    }
}

fn report(segments: []Segment, bytes: u64, rate: f64) void {
    var total: u64 = 0;
    var known = true;
    for (segments) |*s| {
        if (s.len()) |n| total += n else known = false;
    }
    std.debug.print("\r  {d} bytes", .{bytes});
    if (known and total > 0) {
        std.debug.print(" / {d} ({d:.1}%)", .{ total, 100.0 * @as(f64, @floatFromInt(bytes)) / @as(f64, @floatFromInt(total)) });
    }
    std.debug.print("  {d:.1} MB/s  [", .{rate});
    for (segments) |*s| {
        const c: u8 = switch (s.state.load(.acquire)) {
            .ok => '#',
            .running => '~',
            .failed => 'x',
            .idle => '.',
        };
        std.debug.print("{c}", .{c});
    }
    std.debug.print("]   ", .{});
}

// ------------------------------------------------------------------ misc

fn nowMs(io: Io) i64 {
    return @intCast(@divTrunc(Io.Timestamp.now(io, .awake).nanoseconds, std.time.ns_per_ms));
}

fn nameFromUrl(url: []const u8) []const u8 {
    const no_query = url[0 .. std.mem.indexOfScalar(u8, url, '?') orelse url.len];
    const slash = std.mem.lastIndexOfScalar(u8, no_query, '/') orelse return "download";
    const name = no_query[slash + 1 ..];
    return if (name.len == 0) "download" else name;
}

fn parseArgs(args: []const [:0]const u8) ?Options {
    if (args.len < 2) return null;
    var opts: Options = .{ .url = args[1] };
    var i: usize = 2;
    while (i < args.len) : (i += 1) {
        const a = args[i];
        const next = if (i + 1 < args.len) args[i + 1] else return null;
        if (std.mem.eql(u8, a, "-o")) {
            opts.out = next;
        } else if (std.mem.eql(u8, a, "-n")) {
            opts.segments = std.fmt.parseInt(u8, next, 10) catch return null;
        } else if (std.mem.eql(u8, a, "--stall")) {
            opts.stall_ms = std.fmt.parseInt(u32, next, 10) catch return null;
        } else if (std.mem.eql(u8, a, "--retries")) {
            opts.retries = std.fmt.parseInt(u8, next, 10) catch return null;
        } else return null;
        i += 1;
    }
    if (opts.segments == 0) return null;
    return opts;
}
