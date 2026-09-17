//! fdm — fast download manager.
//!
//! ```
//! fdm [url ...] [-n segments] [--stall ms] [--retries n]
//! ```
//!
//! Starts the worker on its own thread, hands it any URLs on the command
//! line, and runs the terminal front until `q`. `a` adds another URL while
//! it runs. The worker is `download.zig`; the front is `tui.zig`; this file
//! only wires them, and the Native SDK window will be wired the same way.

const std = @import("std");
const download = @import("download.zig");
const tui = @import("tui.zig");

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const args = try init.minimal.args.toSlice(init.arena.allocator());

    var settings: download.Settings = .{};
    var urls: std.ArrayList([]const u8) = .empty;
    defer urls.deinit(gpa);

    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        const a = args[i];
        if (std.mem.eql(u8, a, "-n") or std.mem.eql(u8, a, "--stall") or std.mem.eql(u8, a, "--retries")) {
            if (i + 1 >= args.len) return usage();
            const v = args[i + 1];
            i += 1;
            if (std.mem.eql(u8, a, "-n")) {
                settings.segments = std.fmt.parseInt(u8, v, 10) catch return usage();
            } else if (std.mem.eql(u8, a, "--stall")) {
                settings.stall_ms = std.fmt.parseInt(u32, v, 10) catch return usage();
            } else {
                settings.retries = std.fmt.parseInt(u8, v, 10) catch return usage();
            }
        } else if (a.len > 0 and a[0] == '-') {
            return usage();
        } else try urls.append(gpa, a);
    }
    if (settings.segments == 0) return usage();

    const worker = try download.Worker.start(gpa, settings);
    defer worker.stop();

    try tui.run(gpa, worker, urls.items);
}

fn usage() error{Usage} {
    std.debug.print(
        \\usage: fdm [url ...] [-n segments] [--stall ms] [--retries n]
        \\
    , .{});
    return error.Usage;
}

test {
    _ = download;
}
