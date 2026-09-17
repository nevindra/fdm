//! One answer per host, kept for a while, in front of the Io's resolver.
//!
//! `std.http.Client` resolves the name on every connection it opens, and a
//! download here opens sixteen at once and one more on every steal and
//! every reconnect. Against a resolver that takes five seconds — or fails —
//! for a name, that is a probe that fails and a stolen segment that sits at
//! 0 KB/s until the health check reconnects it, which asks again. `curl`
//! asks once. The router on the machine this was found on did exactly that
//! for `mirror.rackspace.com`, a geo-DNS name with a short TTL: 0.5–6 s a
//! lookup and one in four `EAI_AGAIN`, and fdm failed three runs in a row at
//! 5.11 s with nothing on disk while curl took 12 s every time.
//!
//! So the Io the worker hands to `fetch.Client` is `std.Io.Threaded`'s with
//! one vtable slot swapped: `netLookup` answers from a table keyed by host
//! and port, and asks the real resolver only on a miss or once an entry is
//! `ttl_ms` old. Everything else on the vtable is untouched, and `userdata`
//! stays the Threaded's — which is why the table is a global rather than a
//! field: a vtable function has nothing else to find it by, and there is
//! one worker in the process.
//!
//! One answer a download also means one mirror node a download, which is
//! what sixteen segments of the same file want anyway. A failure is not
//! kept; the next connection asks again.

const std = @import("std");
const Io = std.Io;
const HostName = Io.net.HostName;
const IpAddress = Io.net.IpAddress;

/// Longer than a download, shorter than a mirror moving.
pub const ttl_ms: i64 = 10 * std.time.ms_per_min;
const max_entries = 8;
const max_addresses = 16;

const Entry = struct {
    name: [HostName.max_len]u8,
    name_len: usize,
    port: u16,
    addresses: [max_addresses]IpAddress,
    count: usize,
    canon: [HostName.max_len]u8,
    canon_len: usize,
    at_ms: i64,

    fn host(e: *const Entry) HostName {
        return .{ .bytes = e.name[0..e.name_len] };
    }
};

const Lock = struct {
    held: std.atomic.Value(bool) = .init(false),

    fn lock(l: *Lock) void {
        while (l.held.cmpxchgWeak(false, true, .acquire, .monotonic) != null) std.atomic.spinLoopHint();
    }

    fn unlock(l: *Lock) void {
        l.held.store(false, .release);
    }
};

var base: Io = undefined;
var vtable: Io.VTable = undefined;
var lock: Lock = .{};
var entries: [max_entries]Entry = undefined;
var used: usize = 0;
var next_victim: usize = 0;

/// The same Io with a remembering `netLookup`. Call once, on the worker.
pub fn wrap(inner: Io) Io {
    base = inner;
    vtable = inner.vtable.*;
    vtable.netLookup = netLookup;
    return .{ .userdata = inner.userdata, .vtable = &vtable };
}

/// For tests and for a `note`: what the table holds right now.
pub fn count() usize {
    lock.lock();
    defer lock.unlock();
    return used;
}

fn netLookup(
    userdata: ?*anyopaque,
    host: HostName,
    resolved: *Io.Queue(HostName.LookupResult),
    options: HostName.LookupOptions,
) HostName.LookupError!void {
    // `base` carries the same pointer; the slot is kept for the signature.
    _ = userdata;
    const io = base;

    // A caller that wants one family gets the resolver's own answer: the
    // table is keyed on what `connectMany` asks for, which is either.
    if (options.family != null) return base.vtable.netLookup(base.userdata, host, resolved, options);

    defer resolved.close(io);

    var entry: Entry = undefined;
    if (find(host, options.port, nowMs(io), &entry)) return replay(io, &entry, resolved, options);

    // A miss. The real resolver answers into a queue of our own, drained
    // here while it runs, so an answer longer than the buffer cannot block
    // it — the shape `HostName.connectMany` uses for the same reason.
    var buffer: [32]HostName.LookupResult = undefined;
    var queue: Io.Queue(HostName.LookupResult) = .init(&buffer);
    var canon: [HostName.max_len]u8 = undefined;
    var future = io.async(ask, .{ host, &queue, HostName.LookupOptions{
        .port = options.port,
        .canonical_name_buffer = &canon,
        .family = null,
    } });

    entry = .{
        .name = undefined,
        .name_len = host.bytes.len,
        .port = options.port,
        .addresses = undefined,
        .count = 0,
        .canon = undefined,
        .canon_len = 0,
        .at_ms = nowMs(io),
    };
    @memcpy(entry.name[0..host.bytes.len], host.bytes);

    while (queue.getOne(io)) |result| switch (result) {
        .address => |address| if (entry.count < max_addresses) {
            entry.addresses[entry.count] = address;
            entry.count += 1;
        },
        .canonical_name => |name| {
            @memcpy(entry.canon[0..name.bytes.len], name.bytes);
            entry.canon_len = name.bytes.len;
        },
    } else |err| switch (err) {
        error.Canceled => {
            future.cancel(io) catch {};
            return error.Canceled;
        },
        error.Closed => {},
    }
    try future.await(io);

    if (entry.count != 0) put(&entry);
    return replay(io, &entry, resolved, options);
}

fn ask(host: HostName, queue: *Io.Queue(HostName.LookupResult), options: HostName.LookupOptions) HostName.LookupError!void {
    return base.vtable.netLookup(base.userdata, host, queue, options);
}

/// What `Threaded` promises: every address, then exactly one canonical name
/// when the caller gave a buffer for it.
fn replay(io: Io, entry: *const Entry, resolved: *Io.Queue(HostName.LookupResult), options: HostName.LookupOptions) HostName.LookupError!void {
    for (entry.addresses[0..entry.count]) |address| {
        resolved.putOne(io, .{ .address = address }) catch |err| switch (err) {
            error.Closed => unreachable, // `resolved` must not be closed until `netLookup` returns
            error.Canceled => return error.Canceled,
        };
    }
    if (options.canonical_name_buffer) |buf| {
        const canon = if (entry.canon_len != 0) entry.canon[0..entry.canon_len] else entry.host().bytes;
        const dest = buf[0..canon.len];
        @memcpy(dest, canon);
        resolved.putOne(io, .{ .canonical_name = .{ .bytes = dest } }) catch |err| switch (err) {
            error.Closed => unreachable,
            error.Canceled => return error.Canceled,
        };
    }
}

fn find(host: HostName, port: u16, now_ms: i64, out: *Entry) bool {
    lock.lock();
    defer lock.unlock();
    for (entries[0..used]) |*e| {
        if (e.port != port or !e.host().eql(host)) continue;
        if (now_ms - e.at_ms > ttl_ms) return false;
        out.* = e.*;
        return true;
    }
    return false;
}

fn put(entry: *const Entry) void {
    lock.lock();
    defer lock.unlock();
    for (entries[0..used]) |*e| {
        if (e.port == entry.port and e.host().eql(entry.host())) {
            e.* = entry.*;
            return;
        }
    }
    if (used < max_entries) {
        entries[used] = entry.*;
        used += 1;
        return;
    }
    entries[next_victim] = entry.*;
    next_victim = (next_victim + 1) % max_entries;
}

fn nowMs(io: Io) i64 {
    return @intCast(@divTrunc(Io.Timestamp.now(io, .awake).nanoseconds, std.time.ns_per_ms));
}

test "the second lookup of a name is answered from the table" {
    var threaded: Io.Threaded = .init(std.testing.allocator, .{});
    defer threaded.deinit();
    const io = wrap(threaded.io());

    const host: HostName = .{ .bytes = "localhost" };
    const before = count();
    var canon: [HostName.max_len]u8 = undefined;

    for (0..2) |_| {
        var buffer: [32]HostName.LookupResult = undefined;
        var queue: Io.Queue(HostName.LookupResult) = .init(&buffer);
        try host.lookup(io, &queue, .{ .port = 80, .canonical_name_buffer = &canon });

        var addresses: usize = 0;
        var names: usize = 0;
        while (queue.getOne(io)) |r| switch (r) {
            .address => |a| {
                try std.testing.expectEqual(@as(u16, 80), a.getPort());
                addresses += 1;
            },
            .canonical_name => names += 1,
        } else |err| switch (err) {
            error.Closed => {},
            error.Canceled => unreachable,
        }
        try std.testing.expect(addresses >= 1);
        try std.testing.expectEqual(@as(usize, 1), names);
        try std.testing.expectEqual(before + 1, count());
    }

    // Keyed on the port as well as the name.
    var entry: Entry = undefined;
    try std.testing.expect(find(host, 80, nowMs(io), &entry));
    try std.testing.expect(!find(host, 81, nowMs(io), &entry));
}
