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

