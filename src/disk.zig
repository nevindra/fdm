//! How much room the filesystem under a directory has, for the one
//! comparison the probe makes before a file is created. `statvfs` where
//! there is one and `GetDiskFreeSpaceExW` on Windows; null when the
//! question cannot be answered, which is not a reason to refuse.

const std = @import("std");
const builtin = @import("builtin");

pub fn free(dir: []const u8) ?u64 {
    switch (builtin.os.tag) {
        .windows => {
            var wide: [std.fs.max_path_bytes + 1]u16 = undefined;
            const n = std.unicode.wtf8ToWtf16Le(wide[0..std.fs.max_path_bytes], dir) catch return null;
            wide[n] = 0;
            var avail: u64 = 0;
            if (GetDiskFreeSpaceExW(wide[0..n :0].ptr, &avail, null, null) == 0) return null;
            return avail;
        },
        else => {
            if (dir.len >= std.fs.max_path_bytes) return null;
            var path: [std.fs.max_path_bytes:0]u8 = undefined;
            @memcpy(path[0..dir.len], dir);
            path[dir.len] = 0;
            var st: c.struct_statvfs = undefined;
            if (c.statvfs(&path, &st) != 0) return null;
            // What an unprivileged process may take, in fragments.
            return @as(u64, @intCast(st.f_bavail)) * @as(u64, @intCast(st.f_frsize));
        },
    }
}

const c = if (builtin.os.tag != .windows) @cImport(@cInclude("sys/statvfs.h")) else struct {};

extern "kernel32" fn GetDiskFreeSpaceExW(
    lpDirectoryName: [*:0]const u16,
    lpFreeBytesAvailableToCaller: ?*u64,
    lpTotalNumberOfBytes: ?*u64,
    lpTotalNumberOfFreeBytes: ?*u64,
) callconv(.winapi) i32;

test "the directory this runs in has room, and nowhere has none" {
    try std.testing.expect(free(".").? > 0);
    try std.testing.expectEqual(@as(?u64, null), free("/no/such/directory/anywhere"));
}
