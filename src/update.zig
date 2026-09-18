//! `fdm update`: the newest release on GitHub, into the place this binary
//! runs from.
//!
//! One request for the latest release, one for `sha256sums.txt`, one for
//! the asset built for this OS and CPU — all through `nilo_fetch`, the
//! same client the downloads use. The new binary is written beside the
//! old one as `.new`, its digest checked against the sums file the
//! release published, and then renamed over the old one: on Linux and
//! macOS a running binary keeps its inode and the rename is the whole
//! swap; Windows will not let a running `.exe` be replaced or deleted but
//! will let it be renamed, so the old one moves to `.old` first and is
//! swept on the next update.
//!
//! The version compared against is `-Dversion`, which the release
//! workflow sets from the tag; a build without one is `0.0.0-dev` and
//! always counts as behind.

const std = @import("std");
const builtin = @import("builtin");
const core = @import("nilo_core");
const fetch = @import("nilo_fetch");
const build_options = @import("build_options");

const Io = std.Io;

pub const version = build_options.version;
pub const repo = "nevindra/fdm";
/// `fdm-x86_64-linux`, `fdm-aarch64-macos`, `fdm-x86_64-windows.exe` —
/// what `release.yml` names them.
pub const asset_name = "fdm-" ++ @tagName(builtin.cpu.arch) ++ "-" ++ @tagName(builtin.os.tag) ++
    (if (builtin.os.tag == .windows) ".exe" else "");

const Release = struct {
    tag_name: []const u8,
    assets: []const struct { name: []const u8, browser_download_url: []const u8 },
};

pub fn run(gpa: std.mem.Allocator, io: Io) !void {
    // Two minutes covers a binary over a slow link; the release JSON is
    // a second of that.
    var client: fetch.Client = .init(gpa, .{ .max_in_flight = 4, .timeout_ms = 120_000, .max_body = 128 << 20 });
    defer client.deinit();
    try client.nilo_start(io, .none);
    var scope: core.Run = .initIo(gpa, io);
    defer scope.deinit();

    const headers = [_]std.http.Header{
        .{ .name = "user-agent", .value = "fdm/" ++ version },
        .{ .name = "accept", .value = "application/vnd.github+json" },
    };

    const res = try client.get(&scope, "https://api.github.com/repos/" ++ repo ++ "/releases/latest", .{ .headers = &headers });
    if (res.status == .not_found) {
        std.debug.print("fdm {s}: no release published yet.\n", .{version});
        return;
    }
    if (!res.ok()) {
        std.debug.print("GitHub answered HTTP {d} for the latest release.\n", .{@intFromEnum(res.status)});
        return error.NoRelease;
    }
    const release = try res.json(Release, &scope);
    const latest = std.mem.trimStart(u8, release.tag_name, "v");
    if (std.mem.eql(u8, latest, version)) {
        std.debug.print("fdm {s} is the latest.\n", .{version});
        return;
    }

    var asset_url: ?[]const u8 = null;
    var sums_url: ?[]const u8 = null;
    for (release.assets) |a| {
        if (std.mem.eql(u8, a.name, asset_name)) asset_url = a.browser_download_url;
        if (std.mem.eql(u8, a.name, "sha256sums.txt")) sums_url = a.browser_download_url;
    }
    const url = asset_url orelse {
        std.debug.print("{s} has no build named {s}.\n", .{ release.tag_name, asset_name });
        return error.NoAsset;
    };
    const sums = sums_url orelse {
        std.debug.print("{s} has no sha256sums.txt; not installing an unverified binary.\n", .{release.tag_name});
        return error.NoChecksum;
    };

    std.debug.print("fdm {s} -> {s}, fetching {s}\n", .{ version, latest, asset_name });
    const sums_res = try client.get(&scope, sums, .{ .headers = &headers });
    if (!sums_res.ok()) return error.NoChecksum;
    const expected = digestFor(sums_res.body.view(), asset_name) orelse {
        std.debug.print("sha256sums.txt has no line for {s}.\n", .{asset_name});
        return error.NoChecksum;
    };

    const bin = try client.get(&scope, url, .{ .headers = &headers });
    if (!bin.ok()) {
        std.debug.print("HTTP {d} fetching {s}.\n", .{ @intFromEnum(bin.status), asset_name });
        return error.NoAsset;
    }
    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bin.body.view(), &digest, .{});
    if (!std.mem.eql(u8, &digest, &expected)) {
        std.debug.print("{s} does not match sha256sums.txt; not installed.\n", .{asset_name});
        return error.ChecksumMismatch;
    }

    const arena = scope.arena();
    const exe = try std.process.executablePathAlloc(io, arena);
    const fresh = try std.fmt.allocPrint(arena, "{s}.new", .{exe});
    const stale = try std.fmt.allocPrint(arena, "{s}.old", .{exe});
    const cwd = Io.Dir.cwd();

    {
        const f = try cwd.createFile(io, fresh, .{});
        defer f.close(io);
        try f.writeStreamingAll(io, bin.body.view());
        if (Io.File.Permissions.has_executable_bit) try f.setPermissions(io, .executable_file);
    }
    if (builtin.os.tag == .windows) {
        cwd.deleteFile(io, stale) catch {};
        try Io.Dir.rename(cwd, exe, cwd, stale, io);
    }
    try Io.Dir.rename(cwd, fresh, cwd, exe, io);
    std.debug.print("installed fdm {s} at {s}\n", .{ latest, exe });
}

/// The digest on the `<hex>  <name>` line for `name`, from a file in the
/// shape `sha256sum` writes.
fn digestFor(sums: []const u8, name: []const u8) ?[32]u8 {
    var lines = std.mem.splitScalar(u8, sums, '\n');
    while (lines.next()) |line| {
        var parts = std.mem.tokenizeAny(u8, line, " \t*");
        const hex = parts.next() orelse continue;
        const file = parts.next() orelse continue;
        if (!std.mem.eql(u8, file, name) or hex.len != 64) continue;
        var out: [32]u8 = undefined;
        _ = std.fmt.hexToBytes(&out, hex) catch continue;
        return out;
    }
    return null;
}

test "the digest is read from the line that names this binary" {
    const sums =
        "0000000000000000000000000000000000000000000000000000000000000001  fdm-x86_64-linux\n" ++
        "00000000000000000000000000000000000000000000000000000000000000ff *fdm-x86_64-windows.exe\n";
    try std.testing.expectEqual(@as(u8, 1), digestFor(sums, "fdm-x86_64-linux").?[31]);
    try std.testing.expectEqual(@as(u8, 0xff), digestFor(sums, "fdm-x86_64-windows.exe").?[31]);
    try std.testing.expect(digestFor(sums, "fdm-aarch64-macos") == null);
}
