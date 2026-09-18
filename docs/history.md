# History

What was tried, measured and found wrong on the way to what `README.md`
describes. The README says what fdm is; this file says how it got there,
so the next person starts from the numbers rather than from the top.
Newest at the bottom.

## The spike: can nilo_fetch carry a download manager with no Engine?

A Native SDK app is Elm-shaped: `Model`, `Msg`, `update`, and background
work on a thread the app owns, which makes its own `std.Io.Threaded` and
posts results through `fx.openChannel` (see their `examples/channel-monitor`).
nilo's `fetch/` module is tested under exactly that Io with no Engine. So
the question was whether a download manager's worker — segments, resume,
and a bound on a peer that goes quiet — can be built on `nilo_fetch` in
that environment, with nothing from nilo's `http/` and no zio.

### What it found

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

## Sixteen connections, the reconnect and the steal

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

## The benchmark, and the three bugs it found

The tables are in [`bench/result.md`](../bench/result.md).

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

## Headers, a name from the server, and what `Add` had to become

Five roadmap entries in one pass, each meant to be a column or a flag,
and the thing they had in common was found first: `Command.add` carried
a URL and nothing else, so a header, an `-o` and a "yes, again" had
nowhere to travel. `Add` is now a struct the UI fills and the worker
frees, and the five entries were fields on it plus what each field costs
once it reaches the worker.

**A `User-Agent` copied off a browser went out twice.** `std.http.Client`
writes `host`, `authorization`, `content-type` and `user-agent` for
itself unless overridden, and `nilo_fetch`'s `Begin` had a slot for the
first three. A test server logging `get_all('user-agent')` saw
`['zig/0.16.0 (std.http)', 'Mozilla/5.0']`. The worker routes
`Authorization`, `Host` and `User-Agent` to their slots — the last one
new in nilo for this — and drops `Connection`, `Accept-Encoding` and
`Content-Length`, which are the client's to decide.

**Where the name comes from is decided in order.** `-o file` is the
person's and nothing renames it; `Content-Disposition` is taken once,
before the file exists and only when no segment was ever planned, so a
resume never moves a half-written file; the URL's last segment is what
is left. A server does not get to choose the directory: a name with a
separator in it, or `.`/`..`, is no name. `filename*=UTF-8''a%20b.bin`
wins over `filename="x.bin"` as RFC 6266 says.

**The duplicate is checked against the path the worker decided**, not
the one the person typed, because `-o out/` and `--dir out` produce the
same file from different words. The lookup is two `one` queries rather
than an `OR`, and the URL is tried first because it is the one a person
recognises in the prompt. Two URLs the server would name the same file
are not caught — the name is not known until the probe.

**`createMissing` creates a table that is not there and leaves one that
is**, so a column added to a Row after the first release does nothing to
a file made before it. `store.open` now reads `pragma_table_info` and
runs one `ALTER TABLE` per missing column, with the type `createMissing`
would have written; a test makes the first release's table by hand and
opens it.

**`--batch` split on newlines before it parsed**, so a pasted `curl`
line with `\` continuations became five URLs, three of them
`-H 'X-Token: letmein' \`. The reader now joins a line ending in `\`
onto the next before `curl.parse` sees it.

Driven against a local `ThreadingHTTPServer` that honours `Range`,
answers 403 without `X-Token`, sends `Content-Disposition` and
`Last-Modified: Tue, 15 Nov 1994`: the 403 without the flag and the
download with it; `--dir` made on the way; `server name.bin` chosen over
`download`; `-o mine.bin` kept over it; the mtime on disk 1994-11-15
12:45:26 UTC; the duplicate refused with exit 1 and taken with
`--force`; a `--batch` of a URL and a curl line with `--json` lines; the
same in the TUI through tmux, the prompt answered `y`, a curl line pasted
after `a`. Every hash matches.

## The second batch, and the two bugs the disk test found

`--sha256`, `refresh`, the disk check, `--auto-resume` and `--retry-wait`
were each a field on `Add` or a number in `Settings`, as the roadmap
said. What was not in the roadmap was what a 2 MB tmpfs and a server
that sends no `Content-Length` turned up.

**An unknown length crashed the plan.** One segment to
`maxInt(u64)` is how a task says "no end", and the plan wrote that into
a signed column: `@intCast` panicked on the first 200 without a length,
which is every `nolen` server and had never been tried. The row now
holds `maxInt(i64)` and `Segment.create` maps it back.

**A full disk arrived as `WriteFailed`.** `Reader.stream` into a
`File.Writer` reports the writer's failure as `error.WriteFailed` and
keeps the real one in `fw.err`; the segment now returns that, so the
supervisor sees `NoSpaceLeft` and stops at once — one attempt, 2 MB
written down, no steal — where before it was four `WriteFailed`s and a
job retry. `NoSpaceLeft` and `ChecksumMismatch` join `BadStatus` in
`Fetch.final`.

**`zig build test` analyses almost none of the program.** The test
runner never calls `main`, and `test { _ = tui; }` pulls in the module's
tests and nothing else, so a `switch` on `Event` missing two arms passed
fifteen tests. `zig build` is the compile check; the tests are the
tests.

Against the local server: a right `--sha256` verified, a wrong one
failed once with the file kept and no job retry; a `--batch` line
`url sha256=<hex>`; a 5 MB file into the 2 MB tmpfs refused at the probe
with `needs 4 MB, 2 MB free`; the same file without a length failed
mid-write as `NoSpaceLeft` in one attempt; a 403 row refreshed with a
curl line carrying the header and finished; a paused row given a new
URL with `u` resumed from 684 KB on the new link; `--auto-resume`
queued a paused row where a plain start left it paused.

## The link that has room, and the frontier

The roadmap's tail entry said "stealing earlier and smaller near the
end, worth perhaps a second", and the grace-period entry said "does not
land without a number". Neither number could be taken on this machine:
a cloud VM whose NIC hands out a burst allowance and then polices at
100 Mbit, so whichever tool ran first after a pause got 150 MB in a
second and the next got 12 MB/s, and three rounds of the same tool on
`cdn.kernel.org` came out 3.3 s, 13.1 s, 12.9 s. `bench/local/` is the
answer: nginx on the loopback as three kinds of host and `tc` as four
kinds of link, so a run comes out the same twice and the thing that
changed between two runs is the code.

**On a shared link, reconnecting a slow segment cost five seconds in
twenty.** 100 Mbit and 40 ms, 200 MB: curl 17.3 s, fdm-16 22.5 s, and
`--slow 0` 17.5 s. Sixteen connections through one FIFO divide it
unevenly — a plain probe saw eight segments at 0–80 KB/s against a mean
of 858 — and the one at 80 KB/s is slow because the others are fast:
cancelling it drops what was in flight for it and the replacement
starts in slow start, and the total goes down. The same is true of a
steal. Both now wait for the download's total to run under 0.75× its
usual second for two seconds, or under half for one — and *usual* is the
median of the last ten, because the burst bucket makes the best second
ten times the rest and everything after it looked like room. And if the
total is lower two seconds after an action than before it, the link
was the limit after all, and both hold for ten seconds. 16.5 s.

**On a host whose connections differ, a split by sixteen decided once
is the wrong shape.** nginx with `limit_rate` by source port — 40% of
connections at 300 KB/s, 30% at 1 MB/s, 30% at 3 MB/s, which is what
`mirrors.kernel.org` looks like from one client — 60 MB: fdm 12.4 s,
aria2 7.2 s. The fast connections finished their 3.75 MB and the slow
ones were still holding theirs, and the steal, which needed two
megabytes left and a megabyte for each half, never fired on a segment
that size. aria2 hands out one-megabyte pieces as connections come
free, which is why it does not have the problem. fdm now plans a quarter
of the file and hands the rest out from a frontier as connections
finish, each chunk sized to eight seconds at the rate that connection
just showed and to a thirty-second of what is left, so the last chunks
are small and everyone ends together; the steal — judged in seconds
now, and splitting by the two rates — is for when the frontier is gone.
5.4 s, which is the link. A connection is reused across chunks: nginx's
log shows 23 requests on 16 connections, so the cost of a chunk is one
round trip idle, 2.5% of eight seconds at 200 ms.

**Half a second of the start was the queue's poll.** 100 KB: curl
0.1 s, aria2 0.2 s, fdm 0.7 s. `nilo_job` polled every 500 ms and the
command loop every 100; 100 and 50 now, and 0.2 s. The idle TUI costs
0.4% of a core for it.

And `zig build` for the debug binary takes 90 s here against 3 s for the
tests, which is why every variant was a flag rather than a rebuild:
`--slow`, `--slow-checks`, `--slow-per-check`, `--steal-min`, and
`compare.py --variant name=flags` runs one more fdm beside the default.


## What nilo took back

Six workarounds above leaned on nilo where it stood: `poll_ms` cut to
100 because a `push` woke nobody, `timeout_ms = 0` because the deadline
needed an Engine to fire, the URL a redirect ended at read off
`ex.req.uri` because nothing else said it, `max_drain` lowered on the
whole client so the probe could drop a 200 it had asked one byte of,
`Authorization`/`Host`/`User-Agent` routed by hand into `Begin`'s slots,
and `store.open` reading `pragma_table_info` to `ALTER TABLE` by itself.
That went to nilo as feedback and came back as nilo `7dfa14a` (ADR 0229
to 0235). Each one is gone:

- **A `push` wakes a worker**, so `poll_ms` is the default again and only
  finds a retry that came due. Add-to-first-byte is the command loop's
  50 ms and nothing else; the idle TUI asks SQLite once a second per
  worker rather than ten times.
- **The probe has a 30 s deadline**, and it fires: a black hole that used
  to hang until the kernel gave up on the connect is `TimedOut` at 30.1 s.
  The segment calls stay unbounded on purpose. nilo's deadline is on the
  whole call and a segment's call is the transfer, which may take hours,
  so the stall watchdog in `supervise` is still the bound that matters
  for them.
- **`head.location(&buf)`** is where the redirect ended, so the probe no
  longer reaches into std's request. `mirrors.kernel.org` still goes to
  its edge once: 38.6 MB in 5.3 s on 8 segments.
- **`ex.discard()`** on a 200 to the range probe drops the connection with
  the body, and `max_drain` is back to nilo's default for every other
  call. `httpbin.org/bytes`, which ignores `Range`, comes down as one
  segment as before.
- **A header std has a slot for goes out once, the caller's copy**, so
  `Headers` no longer sets `Authorization`, `Host` and `User-Agent` apart:
  a pasted `curl` line goes into `headers` as it is. `Connection`,
  `Accept-Encoding` and `Content-Length` are still dropped, for the same
  reason as before.
- **`sql.migrate.addMissingColumns`** replaces the hand-written
  `pragma_table_info` loop: one `ALTER TABLE` per field a shipped table
  lacks, with the type and default the Row says. `named` had to say
  `.default = .{ .named = false }` in the marker for it, which is a truer
  record than a `DEFAULT 0` in a string.

One thing it found. `addMissingColumns` reads the live columns through the
pool while its own transaction holds another connection, and a
`mode=memory&cache=shared` database answers the second connection with
`SQLITE_LOCKED` where a file lets it through. The store's migration test
ran on shared cache and now runs on a temp file, which is what a user's
database is anyway; the finding is in nilo's `docs/input_from_fdm.md`.

And one number that was not measured before. `transfer_buffer` on a
segment was 64 KiB, sixteen of them a download, on the guide's word that
bigger is fewer trips. `std.http.bodyReader` streams a `content-length`
body from the connection's own buffer to the writer without touching it,
and `/proc/<pid>/io` agrees: 38.8 MB on one segment is 15,000 to 20,000
reads with 64 KiB and the same with 0. What sets the read size is std's
`read_buffer_size`, 8 KiB, which nilo does not expose yet.


## What nilo took back, the second time

The measurement above and what stayed behind went to nilo as the second
round, and came back as nilo `a3a201e` (ADR 0237 to 0240). What moved:

- **The stall watchdog is nilo's `stall_ms`.** The client is started with
  `.stall_ms = settings.stall_ms` and `.timeout_ms = 0`, the segment loop
  reads its chunks through `ex.stream` so the read is inside that clock,
  and a peer that goes quiet ends the call as `error.Stalled`, which takes
  the restart path every other failure takes. `last_seen`,
  `last_moved_ms`, the 100 ms comparison in `supervise` and the
  `future.cancel` inside it are gone; the probe says `.stall_ms = 0` and
  keeps its 30 s deadline as its one bound. Against a server that sends
  half the body and holds the socket, `--stall 2000` restarts the segment
  at 2,054 ms and the hash matches, on a server that slices and on one
  that does not.
- **`redirects = .{ .follow = &buf }`** in place of `redirect_buffer`, on
  the probe and the segment: the two calls that follow now say so.
- **The 64 KiB `transfer_buffer` per segment is gone**, and the 4 KiB on
  the probe with it. Nothing crossed either. `read_buffer_size` is the
  number that was supposed to decide a read's size, so it is `--read-buffer`
  and it was measured: sixteen segments on `ls-lR.gz`, `syscr` from
  `/proc/<pid>/io`, two runs each:

  | `--read-buffer` | reads |
  |---|---|
  | 8 KiB (std's default) | 17,778 and 17,895 |
  | 64 KiB | 17,816 and 17,065 |
  | 256 KiB | 16,428 and 16,746 |

  About 2.2 KB a read whichever, which is what std's TLS reader takes at
  a time (one record's header, then its body) rather than what the
  connection's buffer could hold. The default stays at std's 8 KiB, and
  the flag stays for the day a plain-HTTP server or a different TLS
  reader makes it worth asking again.

Two things it did not take.

- **`head.keep(c)` does not fit here.** The probe's head is read before
  any body, and what outlives the call outlives every `scope.reset()` in
  `runInner` too, which an arena copy does not. `Text` is the right shape
  for that, and it is what every event carries anyway.
- **`Exchange.stream` said zero was the end of the body, and over TLS it
  was not.** The first run against `mirrors.kernel.org` ended every
  segment `ShortBody` inside 300 KB: std's TLS reader answers zero for a
  record with no application data, and for one it decrypted into its own
  buffer. The old loop on `ex.reader.stream` had been ignoring the return
  value, which is why nobody had seen it. Fixed in nilo `5940524`, whose
  `stream` reads on until a byte moves or the stream ends, and that is
  the commit `build.zig.zon` pins.

And one thing found on the way, in fdm rather than nilo. A single
segment, the whole file, was sent with no `Range` header at all, so a
retry after a stall or a dropped connection asked for the file from the
top and wrote it at the offset it had reached. Every segment on a server
that slices now asks with a `Range`, sixteen or one, so the retry resumes
where it was; on a server that does not slice the retry starts at zero,
which is what the server is going to send.
