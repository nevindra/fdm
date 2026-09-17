# fdm — fast download manager

A download manager for Linux on [nilo](../nilo) and the
[Native SDK](https://native-sdk.dev/). **This is the headless spike**: no
window and no TUI yet, one binary that downloads one URL and reports what
it learned. The window comes after, on top of the same worker.

```
zig build
./zig-out/bin/fdm <url> [-o file] [-n segments] [--stall ms] [--retries n]
```

## The question the spike asks

A Native SDK app is Elm-shaped: `Model`, `Msg`, `update`, and background
work on a thread the app owns, which makes its own `std.Io.Threaded` and
posts results through `fx.openChannel` (see their `examples/channel-monitor`).
nilo's `fetch/` module is tested under exactly that Io with no Engine. So
the question was whether a download manager's worker — segments, resume,
and a bound on a peer that goes quiet — can be built on `nilo_fetch` in
that environment, with nothing from nilo's `http/` and no zio.

## What was found

Against a local server that honours `Range` (`rangesrv.py`, 20 MB of
`/dev/urandom`, checked by sha256):

| Scenario | Result |
|---|---|
| 4 segments, `Range` honoured | complete, hash matches, 4 attempts |
| Server ignores `Range` (answers 200) | probe sees it, falls back to one stream, hash matches |
| One segment stalls mid-body, socket held open, `--stall 3000` | watchdog fires at 3,003 ms, `Future.cancel` returns in **0 ms** with `ReadFailed`, segment resumes from its last written byte, hash matches, 5 attempts |

And once against the internet — `https://ziglang.org/download/0.16.0/zig-x86_64-linux-0.16.0.tar.xz`,
55,478,392 bytes, 8 segments over TLS: complete, sha256 matches the one
published in `download/index.json`, 8 attempts. 0.2 MB/s, which was the
link that afternoon rather than the code (`curl` alone got 32 KB/s from
the same host at the same time; fdm used 1 s of CPU in 278 s of wall).

No leaks under the Debug allocator in any of the four, and the stall
scenario passes the same way in ReleaseSafe.

**Three things this settles, and one bug it found on the way:**

1. **`std.Io.Threaded` can cancel a blocking `recv` from another thread.**
   `Threaded.cancel` sends `SIGIO` with `tgkill`, the syscall returns
   `EINTR`, and the task unwinds with `error.Canceled` inside `ReadFailed`.
   This is the property nilo's own deadline needs the Engine for, and it is
   available here without one — so the watchdog is a byte counter per
   segment read by the supervising task, not a timer inside nilo.
2. **Segments write at their own offset with a positional `File.Writer`**
   (`fw.pos = from`), so N tasks share one `File` and no seek position.
3. **A 200 where a 206 was asked for is refused before a byte lands.**
   The probe (`Range: bytes=0-0`) decides whether to split at all; every
   segment still checks its own answer, because a server can change its
   mind between two requests.

The bug: **progress must be counted from what reached the file, not from
what entered the writer's buffer.** The first version counted bytes as
`reader.stream` consumed them; the writer buffered up to 64 KiB of those,
a cancelled task lost them, and the retry resumed past a hole — 20,000,000
bytes and the wrong hash. Now `done` is `fw.pos - seg.start`, which only
moves when a `pwrite` has returned, and every exit path flushes what it
can before recording it. An unbuffered writer is not the answer:
`std.Io.net`'s stream writes straight into the writer's buffer and asserts
on an empty one.

## What is still open

- **Throughput against a fast peer** has not been measured: every run so
  far was loopback (200 MB/s, disk-bound) or a slow link. The number that
  matters — segments against one stream on a link that is actually the
  bottleneck — is still to take, with `curl` beside it as the control.
- **Stack per segment**: two 64 KiB buffers plus what `std.http.Client`
  holds for a TLS connection (nilo measures 59,151 bytes). A manager with
  32 segments open is paying ~6 MB in buffers alone; sizing these is a
  decision, not a default.
- **Persistence and a queue** are the next layer — `nilo_sql`'s SQLite
  wire with `.threading = .in_fiber` and `nilo_job` both run on the same
  Io, and neither is touched here yet.
- **Resume across runs** needs `ETag`/`If-Range`; this spike resumes only
  within one run.
