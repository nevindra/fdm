//! fdm — fast download manager: a worker on nilo_fetch, and a terminal
//! front over it until the Native SDK window replaces it.

const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // The Fitting and the Service, no `nilo_http`: nothing here serves.
    // `.sql = true` is what fetches the SQLite driver (nilo's ADR 0075).
    const nilo = b.dependency("nilo", .{ .target = target, .optimize = optimize, .sql = true });

    // SQLite links libc, and glibc 2.44's `crt1.o` carries an `.sframe`
    // section with relocations Zig 0.16's own ELF linker refuses
    // (`unhandled relocation type R_X86_64_PC64`). `-flld` alone crashes
    // the compiler on the self-hosted backend, so it is LLVM for both, and
    // Debug builds pay a few seconds for it.
    const exe = b.addExecutable(.{
        .name = "fdm",
        .use_llvm = true,
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "nilo_core", .module = nilo.module("nilo_core") },
                .{ .name = "nilo_fetch", .module = nilo.module("nilo_fetch") },
                .{ .name = "nilo_job", .module = nilo.module("nilo_job") },
                .{ .name = "nilo_sql", .module = nilo.module("nilo_sql") },
            },
        }),
    });
    b.installArtifact(exe);

    const run = b.addRunArtifact(exe);
    run.step.dependOn(b.getInstallStep());
    if (b.args) |args| run.addArgs(args);
    b.step("run", "fdm [url ...]: zig build run -- <url>").dependOn(&run.step);

    const tests = b.addTest(.{ .root_module = exe.root_module, .use_llvm = true });
    b.step("test", "the unit tests").dependOn(&b.addRunArtifact(tests).step);
}
