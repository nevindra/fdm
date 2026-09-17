//! What survives a restart: one SQLite file, two tables, and the handful of
//! statements the worker makes against them.
//!
//! The Rows are the schema. `sql.migrate.createMissing` builds both tables
//! from the structs below on first open, and `nilo_sql` checks every
//! statement against them while compiling — so a column renamed here and
//! not there is a build error rather than a `no such column` at run time.
//!
//! The queue is a third table in the same file — `nilo_job`'s own Row over
//! this Db — so a download and the job that fetches it commit together.
//!
//! The wire is `.in_fiber`: there is no Engine here, the caller is a task on
//! the worker's `std.Io.Threaded`, and a statement runs on whichever thread
//! asks. The pool is one writer and one reader, which is what SQLite is.

const std = @import("std");
const core = @import("nilo_core");
const job = @import("nilo_job");
const sql = @import("nilo_sql");

pub const Db = sql.Sqlite(.{ .threading = .in_fiber });
pub const Run = core.Run;
pub const JobTable = job.Table(Db);

pub const State = enum { queued, running, done, failed, cancelled };

pub const Download = struct {
    pub const nilo_table = .{ .name = "downloads", .key = .id };

    id: i64,
    url: []const u8,
    /// The file's name, for showing.
    name: []const u8,
    /// Where it is being written, absolute — a resume from another
    /// directory has to find it.
    path: []const u8,
    total: ?i64,
    segments: i32,
    /// What the server said the object was, `etag` or failing that
    /// `last-modified`. A resume is refused when it has changed.
    etag: ?[]const u8,
    state: State,
    /// Why, when `state` is `failed`.
    reason: ?[]const u8,
    created_ms: i64,
};

pub const Segment = struct {
    pub const nilo_table = .{ .name = "segments", .key = .id };

    id: i64,
    download_id: i64,
    idx: i32,
    start: i64,
    /// Exclusive.
    stop: i64,
    /// Bytes that have reached the file, counted from `start`.
    done: i64,
};

/// Open the file — creating it and the directory above it — and make sure
/// both tables exist.
pub fn open(gpa: std.mem.Allocator, io: std.Io, path: []const u8) !Db {
    if (std.fs.path.dirname(path)) |dir| try std.Io.Dir.cwd().createDirPath(io, dir);

    var db: Db = .init(gpa, path, .{ .size = 2 });
    errdefer db.deinit();
    try db.nilo_start(io, .off);

    var run: Run = .initIo(gpa, io);
    defer run.deinit();
    try sql.migrate.createMissing(&db, &run, &.{ Download, Segment, JobTable.Row });
    return db;
}

/// **One process owns this file**, so a job row that is `running` when the
/// process starts belongs to a process that is gone. Without this it would
/// sit until its lease ran out — a day, since a download may take that
/// long — and the download it carries would not resume until then.
pub fn releaseStale(db: *Db, run: *Run) !usize {
    return db.update(JobTable.Row, run, .{
        .set = .{ .state = .queued, .lease_until = @as(i64, 0) },
        .where = .{ .state = .running },
    });
}

pub fn all(db: *Db, run: *Run) ![]Download {
    return db.select(Download, run, .{ .order = .{ .id = .asc } });
}

pub fn segmentsOf(db: *Db, run: *Run, id: i64) ![]Segment {
    return db.select(Segment, run, .{ .where = .{ .download_id = id }, .order = .{ .idx = .asc } });
}

pub fn add(db: *Db, run: *Run, url: []const u8, name: []const u8, path: []const u8, now_ms: i64) !Download {
    return db.insert(Download, run, .{
        .url = url,
        .name = name,
        .path = path,
        .total = null,
        .segments = 0,
        .etag = null,
        .state = .queued,
        .reason = null,
        .created_ms = now_ms,
    });
}

pub fn setState(db: *Db, run: *Run, id: i64, state: State, reason: ?[]const u8) !void {
    _ = try db.update(Download, run, .{
        .set = .{ .state = state, .reason = reason },
        .where = .{ .id = id },
    });
}

/// A fresh plan: what the server said, and the segments to fetch it in.
/// Any segments from an earlier attempt go.
pub fn plan(db: *Db, run: *Run, id: i64, total: ?i64, etag: ?[]const u8, starts: []const i64, stops: []const i64) ![]Segment {
    var tx = try db.begin(run, .{});
    errdefer tx.rollback();
    _ = try tx.delete(Segment, run, .{ .where = .{ .download_id = id } });
    _ = try tx.update(Download, run, .{
        .set = .{ .total = total, .etag = etag, .segments = @as(i32, @intCast(starts.len)), .state = .running },
        .where = .{ .id = id },
    });
    const rows = try run.arena().alloc(Segment, starts.len);
    for (rows, starts, stops, 0..) |*row, start, stop, i| {
        row.* = try tx.insert(Segment, run, .{
            .download_id = id,
            .idx = @as(i32, @intCast(i)),
            .start = start,
            .stop = stop,
            .done = 0,
        });
    }
    try tx.commit();
    return rows;
}

/// The row, its segments, and any job still waiting to fetch it.
pub fn remove(db: *Db, run: *Run, id: i64) !void {
    var key: [32]u8 = undefined;
    const unique = try std.fmt.bufPrint(&key, "dl:{d}", .{id});
    var tx = try db.begin(run, .{});
    errdefer tx.rollback();
    _ = try tx.delete(Segment, run, .{ .where = .{ .download_id = id } });
    _ = try tx.delete(JobTable.Row, run, .{ .where = .{ .unique_key = unique, .state = .queued } });
    _ = try tx.delete(Download, run, .{ .where = .{ .id = id } });
    try tx.commit();
}

pub fn saveDone(db: *Db, run: *Run, segment_id: i64, done: i64) !void {
    _ = try db.update(Segment, run, .{ .set = .{ .done = done }, .where = .{ .id = segment_id } });
}

test "the tables build, and a download round-trips with its segments" {
    var threaded: std.Io.Threaded = .init(std.testing.allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var db = try open(std.testing.allocator, io, "file:fdm_store_test?mode=memory&cache=shared");
    defer db.deinit();
    var run: Run = .initIo(std.testing.allocator, io);
    defer run.deinit();

    const d = try add(&db, &run, "https://x.y/a.bin", "a.bin", "/tmp/a.bin", 1);
    try std.testing.expectEqual(State.queued, d.state);

    const segs = try plan(&db, &run, d.id, 100, "\"abc\"", &.{ 0, 50 }, &.{ 50, 100 });
    try std.testing.expectEqual(@as(usize, 2), segs.len);
    try saveDone(&db, &run, segs[1].id, 7);

    const back = try segmentsOf(&db, &run, d.id);
    try std.testing.expectEqual(@as(i64, 7), back[1].done);
    const rows = try all(&db, &run);
    try std.testing.expectEqual(@as(?i64, 100), rows[0].total);
    try std.testing.expectEqualStrings("\"abc\"", rows[0].etag.?);
    try std.testing.expectEqual(State.running, rows[0].state);

    // The queue table is there too, and a stale running row goes back.
    var table = JobTable.open(&db);
    const id = (try table.push(&run, "fetch", "{}", .{ .run_at = 0 })).?;
    _ = try table.claim(&run, 1, std.math.maxInt(i64));
    try std.testing.expectEqual(@as(usize, 1), try releaseStale(&db, &run));
    try std.testing.expectEqual(id, (try table.claim(&run, 2, std.math.maxInt(i64))).?.id);
}
