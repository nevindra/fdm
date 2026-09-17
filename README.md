# fdm — fast download manager

A download manager for Linux on [nilo](../nilo) and the
[Native SDK](https://native-sdk.dev/). **MVP 3 is a terminal front over a
worker that remembers and queues**; the window comes next, over the same
worker.

```
zig build
./zig-out/bin/fdm [url ...] [-p parallel] [-n segments] [--stall ms] [--retries n] [--db file]
```

`a` adds a URL, `c` cancels the selected one, `r` resumes a failed or
cancelled one, `j`/`k` move, `q` quits. Files land in the current
directory under the URL's last path segment.

**The list survives.** Every download is a row in one SQLite file
(`--db`, default `$XDG_DATA_HOME/fdm/fdm.db`) and each segment's progress
is written there once a second and on every way out. Start `fdm` again
and the same list is there; whatever was running when it stopped — `q`,
a crash, `kill -9` — carries on from the last byte each segment had. Before
it does, the server is asked again: a different `ETag` or length means a
different file, and that starts over rather than stitching two files into
one. `c` marks a row cancelled and keeps its bytes; `r` resumes it.

**The queue is `nilo_job`.** Each download is a `Fetch` row in the same
file, `-p` workers claim them in order (three by default), and a download
that fails as a whole — every segment out of retries, the server gone for
a minute — is tried again with backoff, three times, before it is dead. A
404 is final and is not retried. Quitting cancels the workers, and a
cancelled worker hands its row back to the queue; that is what "resume on
the next start" is. `std.log` goes to `<db>.log`, never the terminal.

## Layout

| File | What |
|---|---|
| `src/download.zig` | the worker: its own thread and `std.Io.Threaded`, one `fetch.Client`, every download a task with its segments under it. Talks to the rest through `Command` in and `Event` out, and knows nothing about a terminal or a window |
| `src/store.zig` | the tables as structs — `downloads`, `segments`, and `nilo_job`'s own `nilo_jobs` — on `nilo_sql`'s SQLite wire with `.threading = .in_fiber`, and the statements the worker makes against them. `createMissing` builds them on first open |
| `src/tui.zig` | the terminal front: `Item` is the model, `Model.apply` is `update`, `draw` is the view. Raw mode and a `poll` on stdin, no curses |
| `src/main.zig` | wiring |

The seam between the two files is the point. A Native SDK app is
Elm-shaped and does its background work on a thread the app owns, which
posts through `fx.openChannel` and receives `Msg`s in `update` (their
`examples/channel-monitor`). `tui.zig` is that same loop with a terminal
on the end, and `download.zig` does not change when the terminal is
swapped for a window.

## The question the spike asked

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

Driven through a pty (`drive_tui*.py` in the session, not checked in):
two URLs at once, a third typed in with `a`, `c` on one mid-flight, a
404 shown as `HTTP 404 Not Found`, a stall on the slowed server shown as
its note and retried, `q` with downloads still running — every exit
clean, no leaks under the Debug allocator, hashes of what finished
matching.

With the queue, `-p 1` and three URLs: they run one at a time; `q` while
the second is at 5% hands its job back (`queued`, attempts 0) and a restart
resumes it and then runs the third; `kill -9` mid-run leaves a `running`
job row that the next start releases and resumes; a stall that exhausts
the segment retries fails the download, which `nilo_job` retries two
seconds later from where it was; a 404 is dead after one attempt; `c` on a
queued row cancels it before it is claimed. Hashes match throughout.

And for the database, on the same server slowed to 1.3 MB/s: `q` at 29%
then a restart resumes and finishes; `kill -9` at 40% resumes from the
counters written a second earlier and finishes; the server's file
replaced between runs (new `ETag`) starts over and finishes with the new
file's hash; `c` at 8 MB then `r` resumes from 8 MB. Each end state
matches sha256.

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
- **A job's `timeout_ms` is only a lease here.** Without an Engine the
  deadline never fires, so it is set to a day and `store.releaseStale`
  resets `running` rows at start — right for one process owning the file,
  wrong the moment two do.
- **Delete** a row, with or without its file.
- **On this machine `-fllvm` is forced** in `build.zig`: glibc 2.44's
  `crt1.o` carries an `.sframe` section Zig 0.16's own ELF linker
  refuses, and `-flld` alone crashes the compiler. Debug builds pay a few
  seconds for it.
