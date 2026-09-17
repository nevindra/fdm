//! fdm — fast download manager. This is the headless spike: no window, no
//! TUI, one binary that downloads one URL and prints what it learned.

const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Only the Fitting. No `nilo_http` — nothing here serves — and no `.sql`
    // yet, so nothing beyond zio is fetched (nilo's ADR 0075).
    const nilo = b.dependency("nilo", .{ .target = target, .optimize = optimize });

    const exe = b.addExecutable(.{
        .name = "fdm",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "nilo_fetch", .module = nilo.module("nilo_fetch") },
            },
        }),
    });
    b.installArtifact(exe);

    const run = b.addRunArtifact(exe);
    run.step.dependOn(b.getInstallStep());
    if (b.args) |args| run.addArgs(args);
    b.step("run", "Download one URL: zig build run -- <url> [-o file] [-n segments]").dependOn(&run.step);
}
