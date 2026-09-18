# fdm

A download manager for Linux, in the terminal. It splits a file into
segments, fetches them over up to sixteen connections, reconnects the
slow ones, and remembers everything in one SQLite file — so `q`, a
crash or `kill -9` costs nothing, and the next start carries on from the
last byte each segment had.

Built on [nilo](https://github.com/nevindra/nilo) (`nilo_fetch` for the
wire, `nilo_sql` for the file, `nilo_job` for the queue) with
[libvaxis](https://github.com/rockorager/libvaxis) for the screen.

## Install

Linux, macOS and Windows, x86_64 and arm64. Each
[release](https://github.com/nevindra/fdm/releases) carries one binary per
platform and a `sha256sums.txt`; take the one for your machine, make it
executable, put it on your `PATH`. From then on:

```
fdm update
```

fetches the newest release, checks it against the sums file, and swaps
it in place. `fdm --version` says which one you have.

To build it yourself, with Zig 0.16 — dependencies are fetched by commit
from `build.zig.zon`, nothing has to sit beside the checkout:

```
zig build -Doptimize=ReleaseSafe
```

Linux is where it is used; macOS and Windows build and pass the tests in
CI on every push, and the worker's one platform-specific need — cutting a
`recv` that has gone quiet from another thread — is something
`std.Io.Threaded` does on all three. What has not happened yet is a
person watching a download on the other two.

## Use

```
fdm [url ...] [-o path] [-H header] [--sha256 hex] [--dir path] [--batch file] [--force]
    [-n segments] [-p parallel] [--stall ms] [--retries n] [--retry-wait ms] [--auto-resume]
    [--db file] [--headless] [--json]
fdm refresh <id> <url> [-H header] [--headless] [--json]
fdm ls [--json]
```

A `url` may also be the line a browser's "Copy as cURL" writes, quoted as
one argument: the URL, every `-H`, `-b`, `-A`, `-e`, `-u` and `-o` on it
are read, and the rest of curl's options are skipped.

| flag | default | what |
|---|---|---|
| `-o` | | where the n-th URL goes: a file, or a directory when it ends in `/` or is one |
| `-H` | | a `Name: value` sent with every request for these URLs; repeatable. A name `std.http` has a slot for (`Authorization`, `Host`, `User-Agent`) goes out once, yours |
| `--sha256` | | what the n-th URL's file must hash to; a mismatch fails it and keeps the file |
| `--dir` | the cwd | where a URL without `-o` lands |
| `--batch` | | a file with one URL, or one `curl` line, per line; `#` comments, `\` continues a line, `url sha256=<hex>` |
| `--force` | | add a URL that is already in the list, or whose file is |
| `-n` | 16 | connections per download |
| `-p` | 3 | downloads running at once; the rest queue |
| `--stall` | 10000 | ms a segment may go without a byte before it is reconnected |
| `--read-buffer` | 8192 | bytes each connection reads into at a time; measured at nothing over TLS, see `docs/history.md` |
| `--retries` | 3 | attempts per segment, and per probe, before the download fails |
| `--retry-wait` | 0 | ms a failed segment waits before its next attempt |
| `--slow` | 0.3 | a segment under this fraction of the others' mean is reconnected; 0 turns it off |
| `--slow-checks` | 1 | health checks (two seconds apart) a segment must be slow for first |
| `--slow-per-check` | 255 | reconnects allowed in one check |
| `--steal-min` | 2 | seconds a segment must still need before a finished one takes part of it |
| `--auto-resume` | | at start, queue the paused downloads too |
| `--db` | `$XDG_DATA_HOME/fdm/fdm.db` | the list, the segments and the queue |
| `--headless` | | no screen; one line per event, exit when the URLs given are done — non-zero if one failed or was a duplicate |
| `--json` | | each `--headless` line, and `fdm ls`, as JSON |

`fdm refresh <id> <url>` gives a paused or failed download a new link — a
signed URL that expired, a mirror that went away — and queues it; the
file and its segments stay, and if the new server says it is the same
object the download carries on from where it was. A pasted `curl` line
brings its headers with it. `fdm ls` prints the list without starting
anything; `fdm update` and `fdm --version` are the other two.

In the terminal:

| key | |
|---|---|
| `a` | add a URL, or paste a `curl` line |
| `p` | pause the selected download |
| `r` | resume a paused or failed one |
| `u` | give it a new URL first |
| `d` | delete it, asking whether the file goes too |
| `tab` | cycle the filter: all, running, queued, done, failed |
| `/` | search |
| `j` `k` `↑` `↓` `g` `G` | move |
| `q` | quit — running downloads resume on the next start |

A file lands in the current directory, or `--dir`, under the URL's last
path segment — unless the server's `Content-Disposition` names it, which
wins over a name like `download` or a hash, or `-o` does, which wins over
both. The file's modification time is the server's `Last-Modified`, so an
archive sorts where it was published. A URL already in the list, or a
path already taken, is refused with a prompt (`--force` in a script). A
file that will not fit is refused before a byte lands, and a disk that
fills mid-file fails the download at once rather than three retries
later, with its progress written for when there is room.
Two panes at 100 columns or wider: the list on the left; the network
graph, the selected download's URL, path, ETA, one bar per segment and
its log on the right. `std.log` goes to `<db>.log`, never the terminal.

## How it works

**Segments.** A probe asks for one byte with a `Range`. A 206 says the
server can slice and how big the whole is, and the file is split into
`-n` segments, each a task writing at its own offset through a
positional `File.Writer`. A 200 means one stream. The probe also follows
any redirect once and hands the segments the URL it ended on, and the
host is resolved once per download rather than once per connection.

**Connections are not equal**, so three things follow: sixteen of them,
so a per-connection cap stops being the limit; a segment running under 0.3×
the mean of the others is reconnected; and a finished segment takes half
of what the longest running one has left, without cancelling it — the
victim's end is an atomic it reads before every chunk.

**Resume.** Each segment's progress is written to the database once a
second and on every way out, counted from bytes that reached the disk,
never from a writer's buffer. Before resuming, the server is asked again;
a different `ETag` or length starts over rather than stitching two files
into one.

**The queue is `nilo_job`.** Each download is a row in the same file, `-p`
workers claim them in order, and a download that fails as a whole is
tried again with backoff, three times. A 404 is final. Quitting cancels
the workers, and a cancelled worker hands its row back to the queue.

## Speed

Against curl, aria2 and Surge, three interleaved rounds a host,
ReleaseSafe — [`bench/result.md`](bench/result.md) has the tables and
how they were taken, on real hosts and on a loopback shaped into four
kinds of link (`bench/local/`). Where one connection gets the whole
link, fdm is curl less a second: 16.6 s to curl's 17.4 and aria2's 17.8
for 200 MB at 100 Mbit. Where a host caps each connection
(speedtest.tele2.net), fdm-16 does 100 MB in 19 s to aria2's 21 and
Surge's 31. Where connections differ, as a CDN's edges do, 60 MB takes
fdm 5.2 s, aria2 8.0 s and Surge 15.1 s, because fdm hands the file out
as connections finish rather than splitting it once.

## Layout

| file | |
|---|---|
| `src/download.zig` | the worker: its own thread and `std.Io.Threaded`, one `fetch.Client`, every download a task with its segments under it. `Command` in, `Event` out; knows nothing about a terminal |
| `src/store.zig` | the tables as structs — `downloads`, `segments`, and `nilo_job`'s own — and the statements the worker makes against them |
| `src/dns.zig` | the worker's Io with one vtable slot swapped: a host is resolved once a download |
| `src/tui.zig` | the terminal front: `Item` is the model, `Model.apply` is `update`, `draw` is the view |
| `src/curl.zig` | a pasted `curl` line, or a URL, into one `Add` |
| `src/disk.zig` | how much room a directory's filesystem has, on each platform |
| `src/theme.zig` | every colour, in one place |
| `src/update.zig` | `fdm update`: the latest release, checked against its `sha256sums.txt`, renamed over this binary |
| `src/main.zig` | wiring, the flags, `--headless`, `fdm ls`, and where the database lives on each platform |
| `.github/workflows` | `ci.yml` tests on all three platforms and cross-builds every target; `release.yml` turns a `v*` tag into a release |
| `bench/compare.py` | curl, aria2, fdm and Surge against the same URLs, in a pty, interleaved; `--variant` for one more fdm with other flags |
| `bench/local/` | nginx as three kinds of host and `tc` as four kinds of link, so a tail can be measured twice and come out the same |
| `docs/history.md` | what was tried, measured, and found wrong on the way here |
| `docs/roadmap.md` | what is coming, what is being weighed and what is refused, much of it read off Surge and aria2 |

The seam between the worker and the screen is the point: `tui.zig` is an
Elm-shaped loop with a terminal on the end, and a window could sit there
instead without `download.zig` changing.

## Open

- **A job's `timeout_ms` is only a lease.** Without an Engine the deadline
  never fires, so it is a day and `store.releaseStale` resets `running`
  rows at start — right for one process owning the file, wrong for two.
- **Stack per segment**: a 64 KiB write buffer plus what `std.http.Client`
  holds for TLS. Thirty-two open segments is ~4 MB in buffers alone.
- **`-fllvm` is forced** in `build.zig`: glibc 2.44's `crt1.o` carries an
  `.sframe` section Zig 0.16's own linker refuses, and `-flld` alone
  crashes the compiler. Debug builds pay a few seconds for it.
