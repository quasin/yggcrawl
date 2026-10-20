const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // The only C surface left in the project: the SQLite library's headers.
    // libc is linked only because libsqlite3 needs the C runtime (a Zig binary
    // that links a libc-dependent .so without linking libc itself crashes on
    // glibc's first malloc). No C function is called from Zig code: sockets,
    // name resolution, timers and file I/O all go through std.
    const tc = b.addTranslateC(.{
        .root_source_file = b.path("src/c.h"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    const c_mod = tc.createModule();

    const mod = b.createModule(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });
    mod.addImport("c", c_mod);
    mod.linkSystemLibrary("sqlite3", .{});

    const exe = b.addExecutable(.{
        .name = "yggcrawl",
        .root_module = mod,
    });
    b.installArtifact(exe);

    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());
    run_cmd.addPassthruArgs();
    const run_step = b.step("run", "Run yggcrawl");
    run_step.dependOn(&run_cmd.step);
}
