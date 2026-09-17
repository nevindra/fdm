//! The terminal front, on libvaxis: a loop that drains the worker's events
//! into a model and redraws it. It is the same shape the Native SDK app
//! would have — `Item` is the `Model`, `apply` is `update`, `draw` is the
//! view — and it talks to `download.zig` only through `Command` and
//! `Event`.
//!
//! Two panes when the terminal is wide enough: the list on the left, with
//! a filter tab per state; the network graph and the selected download's
//! details — URL, path, ETA, one bar per segment, its own log — on the
//! right. One pane when it is not. Every colour is in `theme.zig`.
//!
//! libvaxis owns the terminal: raw mode, the alternate screen, the kitty
//! keyboard protocol, resize, and a cell diff so a frame that changed one
//! number writes one number. A tick thread posts into its queue ten times
//! a second, and that is when the worker's events are taken.

const std = @import("std");
const vaxis = @import("vaxis");
const download = @import("download.zig");
const theme = @import("theme.zig");

const Io = std.Io;
const Text = download.Text;
const Style = theme.Style;
const State = download.State;

/// Seconds of speed history kept, per download and for the whole session.
const history_len = 60;
const log_len = 8;

/// **Every string a frame prints lives here until the next frame.** The
/// screen keeps the slice it was given — `Screen.writeCell` stores the
/// grapheme by reference and `render` copies it — so text formatted into a
/// stack buffer is garbage by the time it is drawn. The first version did
/// that and the Path field showed the bytes of whatever came next.
var frame: std.heap.ArenaAllocator = undefined;

fn txt(comptime format: []const u8, args: anytype) []const u8 {
    return std.fmt.allocPrint(frame.allocator(), format, args) catch "";
}

pub const Item = struct {
    id: i64,
    url: Text,
    name: Text,
    path: Text,
    total: ?u64 = null,
    bytes: u64 = 0,
    segments: u8 = 0,
    state: State = .queued,
    note: Text = .{},
    elapsed_ms: i64 = 0,
    resumed: bool = false,
    seg: [download.max_shown_segments]download.SegView = @splat(.{ .len = 0, .done = 0 }),
    seg_count: u8 = 0,
    // The rate is sampled once a second from what `bytes` did meanwhile.
    rate_bytes: u64 = 0,
    rate: f64 = 0,
    history: [history_len]f32 = @splat(0),
    history_n: usize = 0,
    log: [log_len]LogLine = undefined,
    log_n: usize = 0,

    const LogLine = struct { at_ms: i64, text: Text };

    fn pushLog(it: *Item, at_ms: i64, text: Text) void {
        if (it.log_n == log_len) {
            std.mem.copyForwards(LogLine, it.log[0 .. log_len - 1], it.log[1..]);
            it.log_n -= 1;
        }
        it.log[it.log_n] = .{ .at_ms = at_ms, .text = text };
        it.log_n += 1;
    }

    fn pushHistory(it: *Item, rate: f64) void {
        if (it.history_n == history_len) {
            std.mem.copyForwards(f32, it.history[0 .. history_len - 1], it.history[1..]);
            it.history_n -= 1;
        }
        it.history[it.history_n] = @floatCast(rate);
        it.history_n += 1;
    }

    fn frac(it: *const Item) ?f64 {
        const total = it.total orelse return null;
        if (total == 0) return 1;
        return @as(f64, @floatFromInt(it.bytes)) / @as(f64, @floatFromInt(total));
    }

    fn etaSecs(it: *const Item) ?u64 {
        const total = it.total orelse return null;
        if (it.rate <= 1 or it.bytes >= total) return null;
        return @intFromFloat(@as(f64, @floatFromInt(total - it.bytes)) / it.rate);
    }
};

const Filter = enum {
    all,
    active,
    queued,
    done,
    failed,

    fn label(f: Filter) []const u8 {
        return switch (f) {
            .all => "All",
            .active => "Active",
            .queued => "Queued",
            .done => "Done",
            .failed => "Failed",
        };
    }

    fn admits(f: Filter, s: State) bool {
        return switch (f) {
            .all => true,
            .active => s == .running,
            .queued => s == .queued,
            .done => s == .done,
            .failed => s == .failed or s == .cancelled,
        };
    }

    fn next(f: Filter) Filter {
        return @enumFromInt((@intFromEnum(f) + 1) % @typeInfo(Filter).@"enum".fields.len);
    }
};

pub const Model = struct {
    gpa: std.mem.Allocator,
    items: std.ArrayList(Item) = .empty,
    /// Index into `items` — not into the filtered view.
    selected: usize = 0,
    scroll: usize = 0,
    filter: Filter = .all,
    /// The URL being typed after `a`, or null when nothing is.
    input: ?std.ArrayList(u8) = null,
    /// The search box: typing when `searching`, and a filter when not.
    search: std.ArrayList(u8) = .empty,
    searching: bool = false,
    /// A delete waiting for its answer: keep the file, or not.
    confirm: ?i64 = null,
    /// Something the worker said that is about nobody in particular.
    status: Text = .{},
    toast: Text = .{},
    toast_until: i64 = 0,
    // The whole session's network line.
    net_history: [history_len]f32 = @splat(0),
    net_n: usize = 0,
    net_rate: f64 = 0,
    net_peak: f64 = 0,
    session_bytes: u64 = 0,
    last_sample_ms: i64 = 0,

    pub fn deinit(m: *Model) void {
        m.items.deinit(m.gpa);
        m.search.deinit(m.gpa);
        if (m.input) |*in| in.deinit(m.gpa);
    }

    pub fn find(m: *Model, id: i64) ?*Item {
        for (m.items.items) |*it| if (it.id == id) return it;
        return null;
    }

    fn shown(m: *const Model, it: *const Item) bool {
        if (!m.filter.admits(it.state)) return false;
        if (m.search.items.len == 0) return true;
        return std.ascii.indexOfIgnoreCase(it.name.slice(), m.search.items) != null or
            std.ascii.indexOfIgnoreCase(it.url.slice(), m.search.items) != null;
    }

    /// Indices of the items the filter and the search let through.
    fn view(m: *const Model, buf: []usize) []usize {
        var n: usize = 0;
        for (m.items.items, 0..) |*it, i| {
            if (n == buf.len) break;
            if (m.shown(it)) {
                buf[n] = i;
                n += 1;
            }
        }
        return buf[0..n];
    }

    /// Move the selection within the view. `delta` may be negative.
    fn move(m: *Model, delta: i32) void {
        var buf: [4096]usize = undefined;
        const v = m.view(&buf);
        if (v.len == 0) return;
        var pos: usize = 0;
        for (v, 0..) |i, p| if (i == m.selected) {
            pos = p;
            break;
        };
        const want: i64 = @as(i64, @intCast(pos)) + delta;
        const clamped: usize = @intCast(@max(0, @min(want, @as(i64, @intCast(v.len)) - 1)));
        m.selected = v[clamped];
    }

    /// The selection must be something the view shows, or nothing.
    fn settle(m: *Model) void {
        if (m.items.items.len == 0) {
            m.selected = 0;
            return;
        }
        if (m.selected >= m.items.items.len) m.selected = m.items.items.len - 1;
        if (m.shown(&m.items.items[m.selected])) return;
        var buf: [4096]usize = undefined;
        const v = m.view(&buf);
        if (v.len > 0) m.selected = v[0];
    }

    fn say(m: *Model, now: i64, text: Text) void {
        m.toast = text;
        m.toast_until = now + 2500;
    }

    /// `update`: one event into the model.
    pub fn apply(m: *Model, ev: download.Event, now: i64) void {
        switch (ev) {
            .added => |a| {
                // The worker owns the list; a row it reports twice is one row.
                if (m.find(a.id) != null) return;
                m.items.append(m.gpa, .{
                    .id = a.id,
                    .url = a.url,
                    .name = a.name,
                    .path = a.path,
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
                it.pushLog(now, .from("queued"));
            },
            .started => |s| if (m.find(s.id)) |it| {
                it.name = s.name;
                it.total = s.total;
                it.segments = s.segments;
                it.state = .running;
                it.resumed = s.resumed;
                it.note = .{};
                it.rate_bytes = it.bytes;
                it.pushLog(now, if (s.resumed) .fmt("resumed, {d} segments", .{s.segments}) else .fmt("started, {d} segments", .{s.segments}));
            },
            .progress => |p| if (m.find(p.id)) |it| {
                if (p.bytes > it.bytes) m.session_bytes += p.bytes - it.bytes;
                it.bytes = p.bytes;
                it.seg = p.seg;
                it.seg_count = p.seg_count;
            },
            .note => |n| if (m.find(n.id)) |it| {
                it.note = n.text;
                it.pushLog(now, n.text);
            },
            .done => |d| if (m.find(d.id)) |it| {
                it.bytes = d.bytes;
                it.elapsed_ms = d.elapsed_ms;
                it.state = .done;
                it.rate = 0;
                it.pushLog(now, .fmt("done in {d:.1}s", .{@as(f64, @floatFromInt(d.elapsed_ms)) / 1000.0}));
            },
            .failed => |f| if (m.find(f.id)) |it| {
                it.state = .failed;
                it.note = f.text;
                it.rate = 0;
                it.pushLog(now, .fmt("failed: {s}", .{f.text.slice()}));
            },
            .cancelled => |c| if (m.find(c.id)) |it| {
                it.state = .cancelled;
                it.rate = 0;
                it.pushLog(now, .from("paused"));
            },
            .removed => |r| {
                for (m.items.items, 0..) |*it, i| if (it.id == r.id) {
                    _ = m.items.orderedRemove(i);
                    break;
                };
                m.settle();
            },
            .fatal => |t| m.status = t,
        }
    }

    /// Once a second: every rate, every history.
    fn sample(m: *Model, now: i64) void {
        if (now - m.last_sample_ms < 1000) return;
        const dt = @as(f64, @floatFromInt(now - m.last_sample_ms)) / 1000.0;
        m.last_sample_ms = now;
        var total: f64 = 0;
        for (m.items.items) |*it| {
            if (it.state != .running) continue;
            it.rate = @as(f64, @floatFromInt(it.bytes -| it.rate_bytes)) / dt;
            it.rate_bytes = it.bytes;
            it.pushHistory(it.rate);
            total += it.rate;
        }
        m.net_rate = total;
        m.net_peak = @max(m.net_peak, total);
        if (m.net_n == history_len) {
            std.mem.copyForwards(f32, m.net_history[0 .. history_len - 1], m.net_history[1..]);
            m.net_n -= 1;
        }
        m.net_history[m.net_n] = @floatCast(total);
        m.net_n += 1;
    }
};

// ------------------------------------------------------------------- run

const Event = union(enum) {
    key_press: vaxis.Key,
    winsize: vaxis.Winsize,
    tick,
};

/// The loop. Returns when the person quits.
pub fn run(gpa: std.mem.Allocator, io: Io, env: *std.process.Environ.Map, worker: *download.Worker, urls: []const []const u8) !void {
    var m: Model = .{ .gpa = gpa };
    defer m.deinit();
    m.last_sample_ms = download.nowMs(io);
    frame = .init(gpa);
    defer frame.deinit();

    for (urls) |u| try add(worker, u);

    var tty_buf: [4096]u8 = undefined;
    var tty = try vaxis.Tty.init(io, &tty_buf);
    defer tty.deinit();

    var vx = try vaxis.init(io, gpa, env, .{});
    defer vx.deinit(gpa, tty.writer());

    var loop: vaxis.Loop(Event) = .init(io, &tty, &vx);
    try loop.start();
    defer loop.stop();

    try vx.enterAltScreen(tty.writer());
    try vx.queryTerminal(tty.writer(), .fromSeconds(1));

    var ticking: std.atomic.Value(bool) = .init(true);
    const ticker = try std.Thread.spawn(.{}, tick, .{ &loop, io, &ticking });
    defer {
        ticking.store(false, .release);
        ticker.join();
    }

    while (true) {
        const event = try loop.nextEvent();
        const now = download.nowMs(io);
        switch (event) {
            .key_press => |key| if (!try handleKey(&m, worker, key, now)) {
                        return;
            },
            .winsize => |ws| try vx.resize(gpa, tty.writer(), ws),
            .tick => {
                const events = try worker.take();
                defer gpa.free(events);
                for (events) |ev| m.apply(ev, now);
                m.sample(now);
            },
        }
        draw(&m, vx.window(), now);
        try vx.render(tty.writer());
    }
}

fn tick(loop: *vaxis.Loop(Event), io: Io, ticking: *std.atomic.Value(bool)) void {
    while (ticking.load(.acquire)) {
        Io.sleep(io, Io.Duration.fromMilliseconds(100), .awake) catch return;
        _ = loop.tryPostEvent(.tick) catch return;
    }
}

/// Hand a URL to the worker. The item appears when the worker reports the
/// row it made for it.
fn add(worker: *download.Worker, url: []const u8) !void {
    const trimmed = std.mem.trim(u8, url, " \t\r\n");
    if (trimmed.len == 0) return;
    // The worker frees this.
    const owned = try worker.gpa.dupe(u8, trimmed);
    errdefer worker.gpa.free(owned);
    try worker.send(.{ .add = owned });
}

/// One key into the model. False means quit.
fn handleKey(m: *Model, worker: *download.Worker, key: vaxis.Key, now: i64) !bool {
    if (key.matches('c', .{ .ctrl = true })) return false;

    // A modal takes every key while it is up.
    if (m.input) |*in| {
        if (key.matches(vaxis.Key.enter, .{})) {
            const url = try in.toOwnedSlice(m.gpa);
            defer m.gpa.free(url);
            in.deinit(m.gpa);
            m.input = null;
            try add(worker, url);
        } else if (key.matches(vaxis.Key.escape, .{})) {
            in.deinit(m.gpa);
            m.input = null;
        } else if (key.matches(vaxis.Key.backspace, .{})) {
            _ = in.pop();
        } else if (key.matches('u', .{ .ctrl = true })) {
            in.clearRetainingCapacity();
        } else if (key.text) |t| {
            try in.appendSlice(m.gpa, t);
        }
        return true;
    }
    if (m.confirm) |id| {
        if (key.matches('y', .{})) {
            try worker.send(.{ .delete = .{ .id = id, .file = true } });
            m.say(now, .from("Deleted, file too"));
        } else if (key.matches('n', .{})) {
            try worker.send(.{ .delete = .{ .id = id, .file = false } });
            m.say(now, .from("Deleted, file kept"));
        } else if (!key.matches(vaxis.Key.escape, .{})) return true;
        m.confirm = null;
        return true;
    }
    if (m.searching) {
        if (key.matches(vaxis.Key.enter, .{})) {
            m.searching = false;
        } else if (key.matches(vaxis.Key.escape, .{})) {
            m.searching = false;
            m.search.clearRetainingCapacity();
        } else if (key.matches(vaxis.Key.backspace, .{})) {
            _ = m.search.pop();
        } else if (key.text) |t| {
            try m.search.appendSlice(m.gpa, t);
        }
        m.settle();
        return true;
    }

    if (key.matches('q', .{})) return false;
    if (key.matches('a', .{})) {
        m.input = .empty;
    } else if (key.matches('/', .{})) {
        m.searching = true;
    } else if (key.matches(vaxis.Key.tab, .{})) {
        m.filter = m.filter.next();
        m.settle();
    } else if (key.matches(vaxis.Key.escape, .{})) {
        m.filter = .all;
        m.search.clearRetainingCapacity();
        m.settle();
    } else if (key.matchesAny(&.{ 'j', vaxis.Key.down }, .{})) {
        m.move(1);
    } else if (key.matchesAny(&.{ 'k', vaxis.Key.up }, .{})) {
        m.move(-1);
    } else if (key.matches('g', .{})) {
        m.move(-100_000);
    } else if (key.matches('G', .{ .shift = true }) or key.matches('G', .{})) {
        m.move(100_000);
    } else if (m.items.items.len > 0) {
        const it = &m.items.items[m.selected];
        if (key.matches('p', .{})) {
            if (it.state == .running or it.state == .queued) try worker.send(.{ .cancel = it.id });
        } else if (key.matches('r', .{})) {
            if (it.state == .failed or it.state == .cancelled) try worker.send(.{ .restart = it.id });
        } else if (key.matches('d', .{})) {
            m.confirm = it.id;
        }
    }
    return true;
}

// ------------------------------------------------------------------ draw

fn draw(m: *Model, root: vaxis.Window, now: i64) void {
    _ = frame.reset(.retain_capacity);
    root.clear();
    const w = root.width;
    const h = root.height;
    if (w < 20 or h < 6) return;

    const body = root.child(.{ .height = h - 1 });
    const wide = w >= 100;
    const left_w: u16 = if (wide) @max(48, w * 11 / 20) else w;

    drawList(m, body.child(.{ .width = left_w }));
    if (wide) {
        const right = body.child(.{ .x_off = left_w, .width = w - left_w });
        const net_h: u16 = 6;
        drawNetwork(m, right.child(.{ .height = net_h }));
        drawDetails(m, right.child(.{ .y_off = net_h, .height = right.height - net_h }), now);
    }
    drawHelp(m, root.child(.{ .y_off = h - 1, .height = 1 }), now);

    if (m.input) |in| drawInput(root, in.items);
    if (m.confirm) |id| if (m.find(id)) |it| drawConfirm(root, it);
}

fn pane(win: vaxis.Window, title: []const u8, focus: bool) vaxis.Window {
    const inner = win.child(.{ .border = .{ .where = .all, .style = if (focus) theme.border_focus else theme.border, .glyphs = .single_rounded } });
    _ = win.printSegment(.{ .text = title, .style = theme.title }, .{ .col_offset = 2, .wrap = .none });
    return inner.child(.{ .x_off = 1, .width = inner.width -| 2 });
}

fn drawList(m: *Model, win: vaxis.Window) void {
    const inner = pane(win, " fdm ", true);
    var buf: [4096]usize = undefined;
    const v = m.view(&buf);

    // Tabs, with counts, and the search box after them.
    var col: u16 = 0;
    inline for (@typeInfo(Filter).@"enum".fields) |f| {
        const filter: Filter = @enumFromInt(f.value);
        var n: usize = 0;
        for (m.items.items) |*it| if (filter.admits(it.state)) {
            n += 1;
        };
        const label = txt("{s} {d}", .{ filter.label(), n });
        const r = inner.printSegment(.{ .text = label, .style = if (filter == m.filter) theme.tab_on else theme.tab_off }, .{ .col_offset = col, .wrap = .none });
        col = r.col + 3;
    }
    if (m.searching or m.search.items.len > 0) {
        const s = txt("/ {s}{s}", .{ m.search.items, if (m.searching) "▏" else "" });
        _ = inner.printSegment(.{ .text = s, .style = if (m.searching) theme.text else theme.muted }, .{ .col_offset = col + 1, .wrap = .none });
    }

    const list = inner.child(.{ .y_off = 2, .height = inner.height -| 2 });
    if (v.len == 0) {
        const msg = if (m.items.items.len == 0) "Nothing here yet — press a to add a URL" else "Nothing matches";
        _ = list.printSegment(.{ .text = msg, .style = theme.muted }, .{ .row_offset = 1, .col_offset = 1, .wrap = .none });
        return;
    }

    // Three rows an item; keep the selection on screen.
    const per: usize = 3;
    const cap: usize = @max(1, list.height / per);
    var pos: usize = 0;
    for (v, 0..) |i, p| if (i == m.selected) {
        pos = p;
        break;
    };
    if (pos < m.scroll) m.scroll = pos;
    if (pos >= m.scroll + cap) m.scroll = pos - cap + 1;
    if (m.scroll > v.len -| cap) m.scroll = v.len -| cap;

    var row: u16 = 0;
    for (v[m.scroll..]) |i| {
        if (row + 2 > list.height) break;
        const it = &m.items.items[i];
        const on = i == m.selected;
        const line = list.child(.{ .y_off = row, .height = 2 });
        if (on) line.fill(.{ .style = theme.selected });
        drawItem(line, it, on);
        row += @intCast(per);
    }
    if (m.scroll + cap < v.len) {
        _ = inner.printSegment(.{ .text = "▼", .style = theme.muted }, .{ .row_offset = inner.height - 1, .col_offset = inner.width - 1, .wrap = .none });
    }
}

fn drawItem(win: vaxis.Window, it: *const Item, on: bool) void {
    const base: Style = if (on) theme.selected else theme.text;
    _ = win.printSegment(.{ .text = if (on) "▎" else " ", .style = with(theme.running, base) }, .{ .wrap = .none });

    var status: []const u8 = "";
    var status_style: Style = theme.muted;
    switch (it.state) {
        .running => {
            status = txt("↓ {s}/s", .{human(it.rate)});
            status_style = theme.running;
        },
        .done => {
            status = txt("✓ {s}", .{human(@floatFromInt(it.bytes))});
            status_style = theme.done;
        },
        .failed => {
            status = txt("✗ {s}", .{it.note.slice()});
            status_style = theme.failed;
        },
        .cancelled => {
            status = txt("‖ paused at {s}", .{human(@floatFromInt(it.bytes))});
            status_style = theme.paused;
        },
        .queued => {
            status = "◌ queued";
            status_style = theme.queued;
        },
    }
    const status_w = win.gwidth(status);
    const name_w = win.width -| (status_w + 4);
    _ = win.printSegment(.{ .text = fit(it.name.slice(), name_w), .style = with(if (on) theme.strong else theme.text, base) }, .{ .col_offset = 2, .wrap = .none });
    _ = win.printSegment(.{ .text = status, .style = with(status_style, base) }, .{ .col_offset = win.width -| (status_w + 1), .wrap = .none });

    // The second line: the bar, or what there is to say instead.
    const line2 = win.child(.{ .y_off = 1, .x_off = 2, .height = 1, .width = win.width -| 3 });
    switch (it.state) {
        .cancelled, .running => if (it.total == null and it.state == .cancelled) {
            // Paused before the server ever answered: nothing to draw a bar of.
            _ = line2.printSegment(.{ .text = fit(it.url.slice(), line2.width), .style = with(theme.faint, base) }, .{ .wrap = .none });
        } else {
            var tail: []const u8 = "";
            if (it.total) |total| {
                if (it.state == .running) {
                    if (it.etaSecs()) |eta| {
                        tail = txt("{d:>3.0}%  {s} / {s}  {s}", .{ (it.frac() orelse 0) * 100, human(@floatFromInt(it.bytes)), human(@floatFromInt(total)), clock(eta) });
                    } else {
                        tail = txt("{d:>3.0}%  {s} / {s}", .{ (it.frac() orelse 0) * 100, human(@floatFromInt(it.bytes)), human(@floatFromInt(total)) });
                    }
                } else {
                    tail = txt("{d:>3.0}%  of {s}", .{ (it.frac() orelse 0) * 100, human(@floatFromInt(total)) });
                }
            } else {
                tail = txt("{s}  unknown length", .{human(@floatFromInt(it.bytes))});
            }
            const tail_w = line2.gwidth(tail);
            const bar_w = line2.width -| (tail_w + 2);
            bar(line2, 0, 0, bar_w, it.frac() orelse 0, if (it.state == .running) theme.bar_fill else theme.paused, base);
            _ = line2.printSegment(.{ .text = tail, .style = with(theme.muted, base) }, .{ .col_offset = bar_w + 2, .wrap = .none });
        },
        .done => {
            const secs = @as(f64, @floatFromInt(it.elapsed_ms)) / 1000.0;
            const s = if (it.elapsed_ms > 0)
                txt("{d:.1}s · {s}/s", .{ secs, human(@as(f64, @floatFromInt(it.bytes)) / secs) })
            else
                fit(shortPath(it.path.slice()), line2.width);
            _ = line2.printSegment(.{ .text = s, .style = with(theme.faint, base) }, .{ .wrap = .none });
        },
        .failed, .queued => {
            _ = line2.printSegment(.{ .text = fit(it.url.slice(), line2.width), .style = with(theme.faint, base) }, .{ .wrap = .none });
        },
    }
}

fn drawNetwork(m: *Model, win: vaxis.Window) void {
    const inner = pane(win, " Network ", false);
    const head = txt("▼ {s}/s", .{human(m.net_rate)});
    const meta = txt("peak {s}/s   session {s}", .{ human(m.net_peak), human(@floatFromInt(m.session_bytes)) });
    _ = inner.printSegment(.{ .text = head, .style = theme.title }, .{ .wrap = .none });
    if (inner.gwidth(head) + inner.gwidth(meta) + 3 <= inner.width) {
        _ = inner.printSegment(.{ .text = meta, .style = theme.muted }, .{ .col_offset = inner.width -| inner.gwidth(meta), .wrap = .none });
    }
    sparkline(inner.child(.{ .y_off = 1, .height = 3 }), m.net_history[0..m.net_n], m.net_peak, theme.spark);
}

fn drawDetails(m: *Model, win: vaxis.Window, now: i64) void {
    const inner = pane(win, " Details ", false);
    if (m.items.items.len == 0) return;
    const it = &m.items.items[m.selected];
    var row: u16 = 0;

    _ = inner.printSegment(.{ .text = fit(it.name.slice(), inner.width), .style = theme.strong }, .{ .row_offset = row, .wrap = .none });
    row += 1;
    field(inner, row, "URL", fit(it.url.slice(), inner.width -| 7));
    row += 1;
    field(inner, row, "Path", fit(it.path.slice(), inner.width -| 7));
    row += 1;
    const size = if (it.total) |t|
        txt("{s} / {s}", .{ human(@floatFromInt(it.bytes)), human(@floatFromInt(t)) })
    else
        human(@floatFromInt(it.bytes));
    field(inner, row, "Size", size);
    row += 1;
    const st = switch (it.state) {
        .running => txt("downloading · {s}/s · {d} segments{s}{s}", .{ human(it.rate), if (it.seg_count > 0) it.seg_count else it.segments, if (it.etaSecs() != null) " · " else "", if (it.etaSecs()) |e| clock(e) else "" }),
        .done => "done",
        .failed => "failed",
        .cancelled => "paused",
        .queued => "queued",
    };
    field(inner, row, "State", st);
    row += 2;

    if (it.state == .running and it.history_n > 1) {
        _ = inner.printSegment(.{ .text = "Speed", .style = theme.muted }, .{ .row_offset = row, .wrap = .none });
        row += 1;
        sparkline(inner.child(.{ .y_off = row, .height = 2 }), it.history[0..it.history_n], m.net_peak, theme.spark);
        row += 3;
    }

    if (it.seg_count > 0 and it.total != null and (it.state == .running or it.state == .cancelled)) {
        _ = inner.printSegment(.{ .text = txt("Segments · {d}", .{it.seg_count}), .style = theme.muted }, .{ .row_offset = row, .wrap = .none });
        row += 1;
        const n: u16 = it.seg_count;
        const per_row: u16 = @min(n, 8);
        const cell_w: u16 = @max(4, inner.width / per_row);
        const bar_w = cell_w -| 1;
        var i: u16 = 0;
        while (i < n) : (i += 1) {
            const r = row + i / per_row;
            const c = (i % per_row) * cell_w;
            const sv = it.seg[i];
            const fr = if (sv.len == 0) 1.0 else @as(f64, @floatFromInt(sv.done)) / @as(f64, @floatFromInt(sv.len));
            bar(inner, c, r, bar_w, fr, if (fr >= 1) theme.bar_done else theme.bar_fill, .{});
        }
        row += (n + per_row - 1) / per_row + 1;
    }

    if (it.log_n > 0 and row < inner.height) {
        _ = inner.printSegment(.{ .text = "Log", .style = theme.muted }, .{ .row_offset = row, .wrap = .none });
        row += 1;
        // The newest at the bottom, as many as fit.
        const room: usize = inner.height -| row;
        const first = it.log_n -| room;
        for (it.log[first..it.log_n]) |line| {
            const ago = @divTrunc(@max(now - line.at_ms, 0), 1000);
            const stamp = txt("{s} ago", .{clock(@intCast(ago))});
            _ = inner.printSegment(.{ .text = stamp, .style = theme.faint }, .{ .row_offset = row, .wrap = .none });
            _ = inner.printSegment(.{ .text = fit(line.text.slice(), inner.width -| 11), .style = theme.text }, .{ .row_offset = row, .col_offset = 11, .wrap = .none });
            row += 1;
        }
    }
}

fn drawHelp(m: *Model, win: vaxis.Window, now: i64) void {
    const keys = [_][2][]const u8{
        .{ "a", "add" },      .{ "p", "pause" },  .{ "r", "resume" }, .{ "d", "delete" },
        .{ "tab", "filter" }, .{ "/", "search" }, .{ "↑↓", "move" },  .{ "q", "quit" },
    };
    var col: u16 = 1;
    for (keys) |k| {
        var r = win.printSegment(.{ .text = k[0], .style = theme.key }, .{ .col_offset = col, .wrap = .none });
        r = win.printSegment(.{ .text = k[1], .style = theme.muted }, .{ .col_offset = r.col + 1, .wrap = .none });
        col = r.col + 2;
    }
    const right: []const u8 = if (m.status.len > 0) m.status.slice() else if (now < m.toast_until) m.toast.slice() else "";
    if (right.len > 0) {
        const style: Style = if (m.status.len > 0) theme.failed else theme.done;
        _ = win.printSegment(.{ .text = right, .style = style }, .{ .col_offset = win.width -| (win.gwidth(right) + 1), .wrap = .none });
    }
}

fn drawInput(root: vaxis.Window, text: []const u8) void {
    const w: u16 = @min(root.width -| 4, 90);
    const box = root.child(.{ .x_off = (root.width - w) / 2, .y_off = root.height / 2 - 2, .width = w, .height = 3 });
    box.fill(.{ .style = .{ .bg = theme.bg_alt } });
    const inner = box.child(.{ .border = .{ .where = .all, .style = theme.border_focus, .glyphs = .single_rounded } });
    _ = box.printSegment(.{ .text = " Add URL ", .style = theme.title }, .{ .col_offset = 2, .wrap = .none });
    // Show the tail when it is longer than the box.
    const room: usize = inner.width -| 3;
    const shown = txt("{s}", .{if (text.len > room) text[text.len - room ..] else text});
    _ = inner.print(&.{ .{ .text = " ", .style = .{} }, .{ .text = shown, .style = theme.text }, .{ .text = "▏", .style = theme.running } }, .{ .wrap = .none });
}

fn drawConfirm(root: vaxis.Window, it: *const Item) void {
    const w: u16 = @min(root.width -| 4, 60);
    const box = root.child(.{ .x_off = (root.width - w) / 2, .y_off = root.height / 2 - 3, .width = w, .height = 5 });
    box.fill(.{ .style = .{ .bg = theme.bg_alt } });
    const inner = box.child(.{ .border = .{ .where = .all, .style = theme.failed, .glyphs = .single_rounded } });
    _ = box.printSegment(.{ .text = " Delete ", .style = .{ .fg = theme.err, .bold = true } }, .{ .col_offset = 2, .wrap = .none });
    _ = inner.printSegment(.{ .text = fit(it.name.slice(), inner.width -| 2), .style = theme.strong }, .{ .col_offset = 1, .wrap = .none });
    _ = inner.print(&.{
        .{ .text = " y ", .style = theme.key },   .{ .text = "file too   ", .style = theme.muted },
        .{ .text = "n ", .style = theme.key },    .{ .text = "keep file   ", .style = theme.muted },
        .{ .text = "esc ", .style = theme.key },  .{ .text = "cancel", .style = theme.muted },
    }, .{ .row_offset = 2, .wrap = .none });
}

// --------------------------------------------------------------- pieces

fn field(win: vaxis.Window, row: u16, label: []const u8, value: []const u8) void {
    _ = win.printSegment(.{ .text = label, .style = theme.muted }, .{ .row_offset = row, .wrap = .none });
    _ = win.printSegment(.{ .text = value, .style = theme.text }, .{ .row_offset = row, .col_offset = 7, .wrap = .none });
}

/// A progress bar of `width` cells at (col, row): `━` filled, `╸` for the
/// half cell at the head, `─` for the rest.
fn bar(win: vaxis.Window, col: u16, row: u16, width: u16, frac: f64, fill_style: Style, base: Style) void {
    const f = @min(@max(frac, 0), 1) * @as(f64, @floatFromInt(width));
    const full: u16 = @intFromFloat(@floor(f));
    const half = f - @floor(f) >= 0.5 and full < width;
    var i: u16 = 0;
    while (i < width) : (i += 1) {
        const ch: []const u8 = if (i < full) "━" else if (i == full and half) "╸" else "─";
        const style = if (i < full or (i == full and half)) with(fill_style, base) else with(theme.bar_rest, base);
        win.writeCell(col + i, row, .{ .char = .{ .grapheme = ch, .width = 1 }, .style = style });
    }
}

/// `samples` as columns of eighth-blocks over `win.height` rows, scaled to
/// `peak`. The newest sample is the rightmost column.
fn sparkline(win: vaxis.Window, samples: []const f32, peak: f64, style: Style) void {
    const blocks = [_][]const u8{ " ", "▁", "▂", "▃", "▄", "▅", "▆", "▇", "█" };
    if (win.height == 0 or win.width == 0) return;
    const rows: usize = win.height;
    const levels: usize = rows * 8;
    const cols: usize = @min(samples.len, win.width);
    const first = samples.len - cols;
    for (samples[first..], 0..) |s, i| {
        const v: f64 = if (peak <= 0) 0 else @as(f64, s) / peak;
        const level: usize = @intFromFloat(@round(@min(@max(v, 0), 1) * @as(f64, @floatFromInt(levels))));
        const col: u16 = @intCast(win.width - cols + i);
        var r: usize = 0;
        while (r < rows) : (r += 1) {
            // Row 0 is the top; the bottom row fills first.
            const from_bottom = rows - 1 - r;
            const in_row = @min(8, level -| from_bottom * 8);
            win.writeCell(col, @intCast(r), .{ .char = .{ .grapheme = blocks[in_row], .width = 1 }, .style = style });
        }
    }
}

/// `style` over `base`: the base's background with the style's ink.
fn with(style: Style, base: Style) Style {
    var s = style;
    if (s.bg == .default) s.bg = base.bg;
    return s;
}

/// `s` cut to `width` cells with an ellipsis. Copied into the frame either
/// way, because `s` may be a Text on the stack of the caller.
fn fit(s: []const u8, width: u16) []const u8 {
    const n = std.unicode.utf8CountCodepoints(s) catch s.len;
    if (n <= width) return txt("{s}", .{s});
    if (width < 2) return "";
    // Walk codepoints until width - 1 of them are kept.
    var it = std.unicode.Utf8View.initUnchecked(s).iterator();
    var kept: usize = 0;
    var end: usize = 0;
    while (it.nextCodepointSlice()) |cp| {
        if (kept + 1 > width - 1) break;
        end += cp.len;
        kept += 1;
    }
    return txt("{s}…", .{s[0..end]});
}

fn shortPath(p: []const u8) []const u8 {
    return if (std.mem.lastIndexOfScalar(u8, p, '/')) |i| p[0..i] else p;
}

/// Bytes (or bytes per second) as a short number with a unit.
fn human(v: f64) []const u8 {
    if (v >= 1 << 30) return txt("{d:.2} GB", .{v / (1 << 30)});
    if (v >= 1 << 20) return txt("{d:.1} MB", .{v / (1 << 20)});
    if (v >= 1 << 10) return txt("{d:.0} KB", .{v / (1 << 10)});
    return txt("{d:.0} B", .{v});
}

/// Seconds as `m:ss` or `h:mm:ss`.
fn clock(secs: u64) []const u8 {
    if (secs >= 3600) return txt("{d}:{d:0>2}:{d:0>2}", .{ secs / 3600, (secs / 60) % 60, secs % 60 });
    return txt("{d}:{d:0>2}", .{ secs / 60, secs % 60 });
}

test "fit keeps short text and ellipsises long text" {
    frame = .init(std.testing.allocator);
    defer frame.deinit();
    try std.testing.expectEqualStrings("abc", fit("abc", 5));
    try std.testing.expectEqualStrings("abcd…", fit("abcdefgh", 5));
}
