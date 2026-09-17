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
    /// `Name: value` lines, newline-separated, sent with every request
    /// for this download. Null when there are none.
    headers: ?[]const u8,
    /// The person chose the file name, so the server's
    /// `Content-Disposition` does not rename it.
    named: bool,
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
    try addMissingColumns(&db, &run);
    return db;
}

/// Columns that came after the first release, put onto a file made before
/// them. `createMissing` creates a table that is not there and leaves one
/// that is, so each of these is one `ALTER TABLE` when `pragma_table_info`
/// does not list it. The types are the ones `createMissing` writes for the
/// same fields, so a file made either way is the same file.
fn addMissingColumns(db: *Db, run: *Run) !void {
    const added = [_]struct { name: []const u8, sql: []const u8 }{
        .{ .name = "headers", .sql = "ALTER TABLE \"downloads\" ADD COLUMN \"headers\" TEXT" },
        .{ .name = "named", .sql = "ALTER TABLE \"downloads\" ADD COLUMN \"named\" INTEGER NOT NULL DEFAULT 0" },
    };
    const Col = struct {
        pub const nilo_table = .projection;
        name: []const u8,
    };
    const have = try db.raw(Col, run, "SELECT name FROM pragma_table_info('downloads')", .{});
    for (added) |col| {
        var found = false;
        for (have) |h| if (std.mem.eql(u8, h.name, col.name)) {
            found = true;
        };
        if (!found) _ = try db.exec(run, col.sql, .{});
    }
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

pub fn add(db: *Db, run: *Run, url: []const u8, name: []const u8, path: []const u8, headers: ?[]const u8, named: bool, now_ms: i64) !Download {
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
        .headers = headers,
        .named = named,
    });
}

/// A row that already has this URL, or failing that this path — whatever
/// its state. Two lookups rather than an `OR`, and the URL wins because it
/// is the one a person recognises.
pub fn duplicate(db: *Db, run: *Run, url: []const u8, path: []const u8) !?Download {
    if (try db.one(Download, run, .{ .where = .{ .url = url } })) |row| return row;
    return db.one(Download, run, .{ .where = .{ .path = path } });
}

/// The server had a better name than the URL did.
pub fn rename(db: *Db, run: *Run, id: i64, name: []const u8, path: []const u8) !void {
    _ = try db.update(Download, run, .{
        .set = .{ .name = name, .path = path },
        .where = .{ .id = id },
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

/// Half of a running segment's remainder becomes a new one: the old row
/// stops at `mid` and the new row runs from there to the old stop. One
/// transaction, so a crash between the two leaves nothing uncovered.
pub fn split(db: *Db, run: *Run, download_id: i64, victim_id: i64, mid: i64, idx: i32, old_stop: i64) !Segment {
    var tx = try db.begin(run, .{});
    errdefer tx.rollback();
    _ = try tx.update(Segment, run, .{ .set = .{ .stop = mid }, .where = .{ .id = victim_id } });
    const made = try tx.insert(Segment, run, .{
        .download_id = download_id,
        .idx = idx,
        .start = mid,
        .stop = old_stop,
        .done = 0,
    });
    try tx.commit();
    return made;
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

    const d = try add(&db, &run, "https://x.y/a.bin", "a.bin", "/tmp/a.bin", "Cookie: k=v", false, 1);
    try std.testing.expectEqual(State.queued, d.state);
    try std.testing.expectEqualStrings("Cookie: k=v", d.headers.?);
    try std.testing.expect(!d.named);

    // The same URL, or the same path, is found; another is not.
    try std.testing.expectEqual(d.id, (try duplicate(&db, &run, "https://x.y/a.bin", "/tmp/b.bin")).?.id);
    try std.testing.expectEqual(d.id, (try duplicate(&db, &run, "https://x.y/b.bin", "/tmp/a.bin")).?.id);
    try std.testing.expect((try duplicate(&db, &run, "https://x.y/b.bin", "/tmp/b.bin")) == null);
    try rename(&db, &run, d.id, "real.bin", "/tmp/real.bin");

    const segs = try plan(&db, &run, d.id, 100, "\"abc\"", &.{ 0, 50 }, &.{ 50, 100 });
    try std.testing.expectEqual(@as(usize, 2), segs.len);
    try saveDone(&db, &run, segs[1].id, 7);

    const back = try segmentsOf(&db, &run, d.id);
    try std.testing.expectEqual(@as(i64, 7), back[1].done);
    const rows = try all(&db, &run);
    try std.testing.expectEqual(@as(?i64, 100), rows[0].total);
    try std.testing.expectEqualStrings("real.bin", rows[0].name);
    try std.testing.expectEqualStrings("\"abc\"", rows[0].etag.?);
    try std.testing.expectEqual(State.running, rows[0].state);

    // The queue table is there too, and a stale running row goes back.
    var table = JobTable.open(&db);
    const id = (try table.push(&run, "fetch", "{}", .{ .run_at = 0 })).?;
    _ = try table.claim(&run, 1, std.math.maxInt(i64));
    try std.testing.expectEqual(@as(usize, 1), try releaseStale(&db, &run));
    try std.testing.expectEqual(id, (try table.claim(&run, 2, std.math.maxInt(i64))).?.id);
}

test "a file from before `headers` and `named` gets both columns on open" {
    var threaded: std.Io.Threaded = .init(std.testing.allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();
    const uri = "file:fdm_store_old?mode=memory&cache=shared";

    // The first release's table, made by hand; the connection keeps the
    // in-memory file alive for the `open` below.
    var old: Db = .init(std.testing.allocator, uri, .{ .size = 1 });
    defer old.deinit();
    try old.nilo_start(io, .off);
    var run: Run = .initIo(std.testing.allocator, io);
    defer run.deinit();
    _ = try old.exec(&run,
        \\CREATE TABLE "downloads" ("id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
        \\  "url" TEXT NOT NULL, "name" TEXT NOT NULL, "path" TEXT NOT NULL, "total" INTEGER,
        \\  "segments" INTEGER NOT NULL, "etag" TEXT, "state" TEXT NOT NULL, "reason" TEXT,
        \\  "created_ms" INTEGER NOT NULL)
    , .{});
    _ = try old.exec(&run,
        \\INSERT INTO "downloads" ("url", "name", "path", "segments", "state", "created_ms")
        \\  VALUES ('https://x.y/old.bin', 'old.bin', '/tmp/old.bin', 0, 'done', 1)
    , .{});

    var db = try open(std.testing.allocator, io, uri);
    defer db.deinit();
    const rows = try all(&db, &run);
    try std.testing.expectEqual(@as(usize, 1), rows.len);
    try std.testing.expect(rows[0].headers == null);
    try std.testing.expect(!rows[0].named);
    // And a second open finds them there and adds nothing.
    var again = try open(std.testing.allocator, io, uri);
    defer again.deinit();
}
