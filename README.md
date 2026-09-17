# fdm — fast download manager

A download manager for Linux on [nilo](../nilo), with a terminal front on
[libvaxis](https://github.com/rockorager/libvaxis). The worker is written
so a [Native SDK](https://native-sdk.dev/) window could sit on it instead;
for now the terminal is the product.

```
zig build
./zig-out/bin/fdm [url ...] [-p parallel] [-n segments] [--stall ms] [--retries n] [--db file]
```

`a` adds a URL, `p` pauses the selected download, `r` resumes a paused or
failed one, `d` deletes it (asking whether the file goes too), `tab`
cycles the filter, `/` searches, `j`/`k` or the arrows move, `q` quits.
Files land in the current directory under the URL's last path segment.

Two panes when the terminal is 100 columns or wider: the list with a tab
per state on the left; the network graph, and the selected download's URL,
path, ETA, one bar per segment and its own log on the right. The layout
takes after [Surge](https://github.com/SurgeDM/Surge)'s dashboard; the
palette is Tokyo Night and lives in `src/theme.zig`.

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
| `src/tui.zig` | the terminal front on libvaxis: `Item` is the model, `Model.apply` is `update`, `draw` is the view. A tick thread posts into vaxis's queue ten times a second; that is when the worker's events are taken |
| `src/dns.zig` | the worker's Io with one vtable slot swapped: a host is resolved once a download rather than once a connection |
| `src/theme.zig` | every colour, in one place |
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

**Two things libvaxis taught, both in comments where they bit.** The
screen keeps the grapheme *slice* it is handed and `render` copies it, so
a string formatted into a stack buffer is garbage by the time it is drawn
— every string a frame prints now lives in an arena reset at the top of
`draw`. And `Loop.stop` wakes its reader thread by asking the terminal
for a device status report (`ESC[5n`); a pty with nobody answering hangs
there forever, which is a property of the test harness rather than the
program, and the harness now answers.

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

## Connections are not equal, and three things follow

A CDN hands each connection to whichever edge it likes, and one of them
is slow. Three answers, all in `Download.supervise`, and the first two
are Surge's (their `OPTIMIZATIONS.md` says why):

- **Sixteen connections a download** (`-n`), where the per-connection
  ceiling most hosts apply stops being the limit and the pipe becomes it.
- **A slow segment is reconnected.** Every second each running segment's
  rate is sampled; every two seconds one that has run three seconds or
  more and is under 0.3× the mean of the others — including the ones that
  finished in the last ten seconds, so the last segment standing still has
  a yardstick — is cancelled and resumes from where it was on a fresh
  connection. Not a retry: it counts against a cap of four, not against
  `--retries`.
- **A finished segment takes half of what the longest running one has
  left**, when that is 2 MB or more, so the download does not end at the
  pace of its slowest connection. **The victim is not cancelled**: its
  `end` is an atomic the task reads before every chunk, so it stops at the
  new boundary and keeps its connection. The boundary goes half a megabyte
  ahead of where the victim is; the split is one transaction in
  `segments`, so a crash between the two rows leaves nothing uncovered.

Against the test server with one connection made 20× slower (`--slow-one
20`, 4 segments, 20 MB): before, the slow segment crawled alone for ~37 s
after the others were done; after, a steal at 3 s and a reconnect at 3 s
finish the whole file in **6.2 s**, sha256 matching, and `kill -9` in the
middle of it resumes from the five-row table.

## Against curl and Surge

[`bench/result.md`](bench/result.md) is the record: `bench/compare.py`
runs curl, fdm with sixteen and with four segments, and Surge, interleaved,
three rounds a host, and checks every file's size. The short version: on a
host that gives one connection the whole pipe (cdn.kernel.org, nodejs.org,
this link's 11 MB/s) fdm is curl to within a second and there is nothing
to win; on a host that caps a connection (speedtest.tele2.net, 0.3 MB/s
each) fdm-16 finishes 100 MB in 12–16 s against curl's 345 s and Surge's
37 s. Surge carries a fixed nine seconds of start-up on this machine, which
the tables show both with and without.

**Three things the benchmark found were fdm's fault, and each is fixed.**
`mirror.rackspace.com` failed three runs in three with nothing on disk: a
router that answers `AAAA` queries for a name without one by not
answering, so a lookup is five seconds or `EAI_AGAIN`, and fdm asked once
per connection — sixteen, then one more per steal and per reconnect —
where curl asks once. `src/dns.zig` answers the second and later from a
table, and the probe, which is the one request nothing else retried,
gets the same three attempts a segment does. `mirrors.kernel.org` took
fdm-16 nearly three times as long as curl: it answers every request with a 301 to its
edge and lets about eight handshakes a second through, and each segment
followed it alone; the probe now carries the URL it ended on and the
segments go there. And a steal that moved a segment's `end` between its
request and the answer made the Content-Length check refuse a correct
answer as `LengthMismatch`; the check now compares against the end that
was asked for.

## What is still open

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
