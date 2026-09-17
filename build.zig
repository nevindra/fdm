//! fdm — fast download manager: a worker on nilo_fetch, and a terminal
//! front over it until the Native SDK window replaces it.

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
    b.step("run", "fdm [url ...]: zig build run -- <url>").dependOn(&run.step);

    const tests = b.addTest(.{ .root_module = exe.root_module });
    b.step("test", "the unit tests").dependOn(&b.addRunArtifact(tests).step);
}
