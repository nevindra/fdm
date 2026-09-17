//! The terminal front: a loop that drains the worker's events into a model
//! and redraws it. It is the same shape the Native SDK app will have —
//! `Item` is the `Model`, `apply` is `update`, `draw` is the view — and it
//! talks to `download.zig` only through `Command` and `Event`.
//!
//! Raw mode, the alternate screen and a `poll` on stdin with a 50 ms
//! timeout; no curses. Linux-only, like the rest of the program.

const std = @import("std");
const download = @import("download.zig");

const linux = std.os.linux;
const posix = std.posix;
const Text = download.Text;

pub const Item = struct {
    id: i64,
    url: Text,
    name: Text,
    total: ?u64 = null,
    bytes: u64 = 0,
    segments: u8 = 0,
    state: download.State = .queued,
    note: Text = .{},
    elapsed_ms: i64 = 0,
    // For the rate: what was seen a moment ago, and when.
    rate_bytes: u64 = 0,
    rate_ms: i64 = 0,
    rate: f64 = 0,
};

pub const Model = struct {
    gpa: std.mem.Allocator,
    items: std.ArrayList(Item) = .empty,
    selected: usize = 0,
    /// The URL being typed after `a`, or null when nothing is.
    input: ?std.ArrayList(u8) = null,
    /// What the worker last said that was about nobody in particular.
    status: Text = .{},

    pub fn deinit(m: *Model) void {
        m.items.deinit(m.gpa);
        if (m.input) |*in| in.deinit(m.gpa);
    }

    pub fn find(m: *Model, id: i64) ?*Item {
        for (m.items.items) |*it| if (it.id == id) return it;
        return null;
    }

    /// `update`: one event into the model.
    pub fn apply(m: *Model, ev: download.Event, now_ms: i64) void {
        switch (ev) {
            .added => |a| {
                // The worker owns the list; a row it reports twice is one row.
                if (m.find(a.id) != null) return;
                m.items.append(m.gpa, .{
                    .id = a.id,
                    .url = a.url,
                    .name = a.name,
                    .state = a.state,
                    .total = a.total,
                    .bytes = a.bytes,
                    .segments = a.segments,
                }) catch return;
                if (a.state == .queued) m.selected = m.items.items.len - 1;
            },
            .queued => |q| if (m.find(q.id)) |it| {
                it.state = .queued;
                it.note = .{};
                it.rate = 0;
            },
            .started => |s| if (m.find(s.id)) |it| {
                it.name = s.name;
                it.total = s.total;
                it.segments = s.segments;
                it.state = .running;
                it.note = if (s.resumed) .from("resumed") else .{};
                it.rate_ms = now_ms;
                it.rate_bytes = it.bytes;
            },
            .progress => |p| if (m.find(p.id)) |it| {
                it.bytes = p.bytes;
                // A rate over the last second or so, not since the start.
                if (now_ms - it.rate_ms >= 1000) {
                    const dt = @as(f64, @floatFromInt(now_ms - it.rate_ms)) / 1000.0;
                    it.rate = @as(f64, @floatFromInt(p.bytes -| it.rate_bytes)) / dt;
                    it.rate_bytes = p.bytes;
                    it.rate_ms = now_ms;
                }
            },
            .note => |n| if (m.find(n.id)) |it| {
                it.note = n.text;
            },
            .done => |d| if (m.find(d.id)) |it| {
                it.bytes = d.bytes;
                it.elapsed_ms = d.elapsed_ms;
                it.state = .done;
                it.rate = 0;
            },
            .failed => |f| if (m.find(f.id)) |it| {
                it.state = .failed;
                it.note = f.text;
                it.rate = 0;
            },
            .cancelled => |c| if (m.find(c.id)) |it| {
                it.state = .cancelled;
                it.rate = 0;
            },
            .fatal => |t| m.status = t,
        }
    }
};

// ------------------------------------------------------------------- run

/// The loop. Returns when the person quits.
pub fn run(gpa: std.mem.Allocator, worker: *download.Worker, urls: []const []const u8) !void {
    var m: Model = .{ .gpa = gpa };
    defer m.deinit();

    for (urls) |u| try add(&m, worker, u);

    var term = try Terminal.enter();
    defer term.leave();

    var frame: std.Io.Writer.Allocating = .init(gpa);
    defer frame.deinit();

    var t: i64 = 0;
    var last_size: ?posix.winsize = null;
    while (true) {
        const events = try worker.take();
        defer gpa.free(events);
        for (events) |ev| m.apply(ev, t);

        var buf: [64]u8 = undefined;
        const n = try term.read(&buf, 50);
        t += 50;
        var keys = buf[0..n];
        while (keys.len > 0) {
            const used = try handleKey(&m, worker, keys) orelse return;
            keys = keys[used..];
        }

        const size = term.size();
        // Repaint from a clean screen when the window changed; otherwise
        // overdraw in place so the terminal does not flicker.
        frame.clearRetainingCapacity();
        const w = &frame.writer;
        if (last_size == null or size.col != last_size.?.col or size.row != last_size.?.row) try w.writeAll("\x1b[2J");
        last_size = size;
        try draw(&m, w, size);
        try term.writeAll(frame.written());
    }
}

/// Hand a URL to the worker. The item appears when the worker reports the
/// row it made for it.
fn add(m: *Model, worker: *download.Worker, url: []const u8) !void {
    _ = m;
    const trimmed = std.mem.trim(u8, url, " \t\r\n");
    if (trimmed.len == 0) return;
    // The worker frees this.
    const owned = try worker.gpa.dupe(u8, trimmed);
    errdefer worker.gpa.free(owned);
    try worker.send(.{ .add = owned });
}

/// One key (or escape sequence) into the model. Returns how many bytes it
/// consumed, or null to quit.
fn handleKey(m: *Model, worker: *download.Worker, keys: []const u8) !?usize {
    const k = keys[0];

    if (m.input) |*in| {
        switch (k) {
            '\r', '\n' => {
                const url = try in.toOwnedSlice(m.gpa);
                defer m.gpa.free(url);
                in.deinit(m.gpa);
                m.input = null;
                try add(m, worker, url);
            },
            0x1b => {
                in.deinit(m.gpa);
                m.input = null;
                // Swallow a whole escape sequence if one followed.
                return escapeLen(keys);
            },
            0x7f, 0x08 => _ = in.pop(),
            0x03 => return null,
            else => if (k >= 0x20) try in.append(m.gpa, k),
        }
        return 1;
    }

    switch (k) {
        'q', 0x03 => return null,
        'a' => m.input = .empty,
        'j' => m.selected = @min(m.selected + 1, m.items.items.len -| 1),
        'k' => m.selected -|= 1,
        'c' => if (m.items.items.len > 0) {
            const it = &m.items.items[m.selected];
            if (it.state == .running or it.state == .queued) try worker.send(.{ .cancel = it.id });
        },
        'r' => if (m.items.items.len > 0) {
            const it = m.items.items[m.selected];
            if (it.state == .failed or it.state == .cancelled) try worker.send(.{ .restart = it.id });
        },
        0x1b => {
            // Arrow keys arrive as ESC [ A / ESC [ B.
            if (keys.len >= 3 and keys[1] == '[') switch (keys[2]) {
                'A' => m.selected -|= 1,
                'B' => m.selected = @min(m.selected + 1, m.items.items.len -| 1),
                else => {},
            };
            return escapeLen(keys);
        },
        else => {},
    }
    return 1;
}

fn escapeLen(keys: []const u8) usize {
    if (keys.len >= 3 and keys[1] == '[') return 3;
    return 1;
}

// ------------------------------------------------------------------ draw

fn draw(m: *Model, w: *std.Io.Writer, size: posix.winsize) !void {
    const cols: usize = @max(size.col, 40);
    const rows: usize = @max(size.row, 6);

    try w.writeAll("\x1b[H"); // home
    try line(w, cols, "fdm  —  [a]dd  [c]ancel  [r]esume  [j/k] move  [q]uit", .{});
    try w.writeAll("\r\n");

    // Every download gets two lines: what and how far, then its note.
    const list_rows = rows - 4;
    var used: usize = 0;
    const first = if (m.selected * 2 >= list_rows) m.selected * 2 - list_rows + 2 else 0;
    for (m.items.items[@min(first / 2, m.items.items.len)..], first / 2..) |*it, idx| {
        if (used + 2 > list_rows) break;
        const mark: []const u8 = if (idx == m.selected) "\x1b[7m>" else " ";
        try w.print("{s} ", .{mark});
        try itemLine(w, it, cols - 2);
        try w.writeAll("\x1b[0m\r\n");
        try line(w, cols, "    {s}", .{it.note.slice()});
        try w.writeAll("\r\n");
        used += 2;
    }
    while (used < list_rows) : (used += 1) try w.writeAll("\x1b[2K\r\n");

    if (m.input) |in| {
        try line(w, cols, "add url: {s}_", .{in.items});
    } else if (m.status.len > 0) {
        try line(w, cols, "{s}", .{m.status.slice()});
    } else {
        var running: usize = 0;
        var rate: f64 = 0;
        for (m.items.items) |*it| if (it.state == .running) {
            running += 1;
            rate += it.rate;
        };
        try line(w, cols, "{d} downloads, {d} running, {d:.1} MB/s", .{ m.items.items.len, running, rate / 1e6 });
    }
    try w.writeAll("\r\n");
}

/// One row of an item: name, bar, numbers, fitted to `cols`.
fn itemLine(w: *std.Io.Writer, it: *const Item, cols: usize) !void {
    var buf: [512]u8 = undefined;
    var fbs: std.Io.Writer = .fixed(&buf);
    const name_w = @min(it.name.len, 32);
    try fbs.print("{s:<32} ", .{it.name.slice()[0..name_w]});

    switch (it.state) {
        .queued => try fbs.writeAll("queued"),
        .running => {
            if (it.total) |total| {
                const frac = if (total == 0) 1.0 else @as(f64, @floatFromInt(it.bytes)) / @as(f64, @floatFromInt(total));
                try bar(&fbs, frac, 20);
                try fbs.print(" {d:>5.1}%  ", .{frac * 100});
                try human(&fbs, it.bytes);
                try fbs.writeAll(" / ");
                try human(&fbs, total);
            } else {
                try fbs.writeAll("[   unknown length   ]  ");
                try human(&fbs, it.bytes);
            }
            try fbs.print("  {d:.1} MB/s  {d} seg", .{ it.rate / 1e6, it.segments });
        },
        .done => {
            try fbs.writeAll("done  ");
            try human(&fbs, it.bytes);
            // A row restored from the database finished in an earlier run,
            // and nobody wrote down how long it took.
            if (it.elapsed_ms > 0) {
                const secs = @as(f64, @floatFromInt(it.elapsed_ms)) / 1000.0;
                try fbs.print(" in {d:.1}s  {d:.1} MB/s", .{ secs, @as(f64, @floatFromInt(it.bytes)) / secs / 1e6 });
            }
        },
        .failed => try fbs.writeAll("failed"),
        .cancelled => {
            try fbs.writeAll("cancelled at ");
            try human(&fbs, it.bytes);
        },
    }
    const s = fbs.buffered();
    try w.writeAll(s[0..@min(s.len, cols)]);
    try w.writeAll("\x1b[K");
}

fn bar(w: *std.Io.Writer, frac: f64, width: usize) !void {
    const filled: usize = @intFromFloat(@round(@min(@max(frac, 0), 1) * @as(f64, @floatFromInt(width))));
    try w.writeAll("[");
    for (0..width) |i| try w.writeAll(if (i < filled) "#" else "-");
    try w.writeAll("]");
}

fn human(w: *std.Io.Writer, n: u64) !void {
    const f: f64 = @floatFromInt(n);
    if (n >= 1 << 30) return w.print("{d:.2} GB", .{f / (1 << 30)});
    if (n >= 1 << 20) return w.print("{d:.1} MB", .{f / (1 << 20)});
    if (n >= 1 << 10) return w.print("{d:.0} KB", .{f / (1 << 10)});
    return w.print("{d} B", .{n});
}

/// A formatted line, cut to `cols`, with the rest of the row cleared.
fn line(w: *std.Io.Writer, cols: usize, comptime f: []const u8, args: anytype) !void {
    var buf: [512]u8 = undefined;
    const s = std.fmt.bufPrint(&buf, f, args) catch buf[0..];
    try w.writeAll(s[0..@min(s.len, cols)]);
    try w.writeAll("\x1b[K");
}

// -------------------------------------------------------------- terminal

const Terminal = struct {
    saved: posix.termios,

    fn enter() !Terminal {
        const saved = try posix.tcgetattr(posix.STDIN_FILENO);
        var raw = saved;
        raw.lflag.ECHO = false;
        raw.lflag.ICANON = false;
        raw.lflag.ISIG = false; // Ctrl-C is a key here, handled as quit
        raw.lflag.IEXTEN = false;
        raw.iflag.ICRNL = false;
        raw.iflag.IXON = false;
        raw.cc[@intFromEnum(posix.V.MIN)] = 0;
        raw.cc[@intFromEnum(posix.V.TIME)] = 0;
        try posix.tcsetattr(posix.STDIN_FILENO, .FLUSH, raw);
        var t: Terminal = .{ .saved = saved };
        try t.writeAll("\x1b[?1049h\x1b[?25l\x1b[2J"); // alternate screen, hide cursor
        return t;
    }

    fn leave(t: *Terminal) void {
        t.writeAll("\x1b[?25h\x1b[?1049l") catch {};
        posix.tcsetattr(posix.STDIN_FILENO, .FLUSH, t.saved) catch {};
    }

    /// Up to `buf.len` bytes of input, waiting at most `timeout_ms`.
    fn read(_: *Terminal, buf: []u8, timeout_ms: i32) !usize {
        var fds = [_]posix.pollfd{.{ .fd = posix.STDIN_FILENO, .events = posix.POLL.IN, .revents = 0 }};
        const ready = try posix.poll(&fds, timeout_ms);
        if (ready == 0) return 0;
        return posix.read(posix.STDIN_FILENO, buf) catch |err| switch (err) {
            error.WouldBlock => 0,
            else => err,
        };
    }

    fn writeAll(_: *Terminal, bytes: []const u8) !void {
        var rest = bytes;
        while (rest.len > 0) {
            const rc = linux.write(posix.STDOUT_FILENO, rest.ptr, rest.len);
            switch (linux.errno(rc)) {
                .SUCCESS => rest = rest[rc..],
                .INTR, .AGAIN => {},
                else => return error.WriteFailed,
            }
        }
    }

    fn size(_: *Terminal) posix.winsize {
        var ws: posix.winsize = .{ .row = 24, .col = 80, .xpixel = 0, .ypixel = 0 };
        _ = linux.ioctl(posix.STDOUT_FILENO, posix.T.IOCGWINSZ, @intFromPtr(&ws));
        if (ws.col == 0) ws.col = 80;
        if (ws.row == 0) ws.row = 24;
        return ws;
    }
};
