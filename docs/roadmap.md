# Roadmap

What is coming, what is being weighed, and what is refused. Nothing here
is built; the moment something ships its entry leaves this file, and what
was measured on the way goes to `history.md`. Newest thinking at the top
of each section, entries ranked by how much they buy against what they
cost fdm's shape: one process, one SQLite file, a worker that knows
nothing about a terminal.

Most of these were read off two other download managers and put against
what fdm already has. [Surge](https://github.com/SurgeDM/Surge) (Go,
Bubble Tea) is the one `bench/compare.py` races against; its
`docs/SETTINGS.md` and `docs/OPTIMIZATIONS.md` are the source for the
entries marked *Surge*. [aria2](https://aria2.github.io/manual/en/html/aria2c.html)
is twenty years of options for the same job, and the entries marked
*aria2* are the ones that survived that long. Where both have the same
thing it is a strong signal the thing is wanted. What fdm already has
(sixteen connections, the 0.3× mean reconnect, the steal, resume on
`ETag`, the `nilo_job` queue, `--headless`, `fdm update`, headers and a
pasted `curl` line, `-o` and `--dir` and the server's name, the refused
duplicate, `--batch` and `--json` and `fdm ls`, the remote mtime,
`--sha256`, `refresh`, the disk check and `ENOSPC` as final,
`--auto-resume`, `--retry-wait`) is not listed.

## Next: cheap, and the shape of the program does not move

Each of these lands in `store.zig` and `download.zig`, or in `main.zig`'s
flag parsing, and nothing else has to change. Two batches of them did —
headers, `-o`, the duplicate, `--batch`/`--json`, the mtime; then
`--sha256`, `refresh`, the disk check, `--auto-resume`, `--retry-wait` —
and what they cost is in `history.md`; `Add` now carries what the person
asked for, so the next per-download option is a field on it.

**A rate limit, per download and global.** *Surge `surge limit <id>
<speed>`, `--global`; aria2 `--max-download-limit`,
`--max-overall-download-limit`.* A token bucket read before every chunk
in the segment loop; the global one is a second bucket shared through
the worker. `--limit 2M` on the command line and `l` in the TUI. Costs:
one atomic and one clock read per chunk, and nothing when the limit is
zero.


## Weighed: worth building, and each needs a design first

**Mirrors: one file, several URLs, the segments spread across them.**
*Surge multi-source, aria2 metalink and `--uri-selector`.* fdm's
segments already do not care which URL fed them, which is the hard
half. The design questions are the probe (every mirror must agree on
`ETag` and length, or the disagreeing one is dropped), which mirror a
reconnecting segment goes to (aria2's `feedback` selector, by measured
speed, is the one that earns its keep), and how a mirror that 404s is
retired without failing the download. Costs: a `mirrors` table, and a
per-segment URL, which is a pointer.

**Sequential mode, for a file that is watched while it arrives.**
*Surge `sequential_download`, aria2 `--stream-piece-selector=inorder`.*
Since the frontier, the file is already handed out from the front as
connections finish; what sequential adds is a bound on how far ahead of
the lowest unfinished byte the frontier may go, and a steal that never
takes the head. Costs: one number, and a rule in the steal.

**Pre-warmed connections, so a reconnect does not pay the handshake.**
*Surge `dial_hedge_count` 4.* A pool of open connections to the host,
kept ahead of demand, so a reconnected or stolen segment starts sending
bytes in one round trip. The host is already resolved once a download,
so the saving is the TCP and TLS handshake only; on a host where that is
30 ms it will not show, on one where it is 300 ms it will. Measure
before building. Costs: idle sockets, and the TLS state each holds.

**Adaptive connection count.** *Surge `adaptive_concurrency_interval`,
off by default.* Raise `-n` while throughput rises with it, lower it
when it does not. Surge ships this disabled, which is a signal they do
not trust it either; sixteen fixed has beaten it in every race so far.
Waiting on a host where sixteen is wrong in either direction.

**Post-download hooks.** *aria2 `--on-download-complete`,
`--on-download-error`.* Run a command with the path and the outcome:
unpack, notify, move. In `--headless` this is `xargs` on the event
line already; in the TUI it is not. Costs: a `spawn` off the worker
thread, after the file is closed.

**Hold the machine awake while something is downloading.** *Surge
`internal/power`.* On Linux this is `systemd-inhibit` or the logind
D-Bus call; one file per platform, like `main.zig`'s database path.
Costs: one process or one D-Bus connection held open.

**A resume that gives up.** *aria2 `--max-resume-failure-tries`,
`--always-resume=false`.* A server that answered 206 at the probe and
200 on resume is today a fresh start every time, forever. What "giving
up" should mean is not obvious — the fresh start is already the one
stream the server allows — so this waits for a host that does it.
`--retry-wait` shipped on its own.

## Refused, or not yet: they change what fdm is

**A daemon with an HTTP API, and a TUI that connects to it.** *Surge
`surge server`, `surge connect`, `surge add`, token auth, `surge
service install`.* One engine in the background, any number of shells
and tabs feeding it, a `systemd` unit that starts it at boot. This is
the entry that would resolve the first item under "Open" in the README:
the day-long job lease is wrong for two processes on one file, and a
daemon means there is one. It is also the thing a browser extension
needs, and a phone. The cost is the shape: `nilo_http` enters the
binary, `download.zig` takes its `Command`s from a socket as well as a
channel, and there is a token to keep. The seam between the worker and
the screen was drawn so this could happen without `download.zig`
changing; whether it should is a decision, not a gap.
Waiting on: a second client that wants it.

**A browser extension.** *Surge, port 1700.* Intercept the browser's
download and hand it over with its cookies. Needs the daemon; the
headers it would hand over are taken since `Add.headers`. The extension
itself is a hundred lines of JavaScript that is not fdm's to write until
the daemon is there.
Waiting on: the daemon.

**Clipboard monitor.** *Surge `clipboard_monitor`, uGet.* Watch the
clipboard and offer every URL. Needs `wl-paste` or `xclip` and a poll,
and it is the kind of thing that is on by default and turned off in the
first hour. Refused as a default; maybe as `--watch-clipboard`.

**Categories: a subfolder per file type.** *Surge `category_enabled`,
uGet, IDM.* A regex table from extension to directory. `-o` and a
default directory cover the case that matters; this is a policy the
shell can express in one `mv`. Refused.

**Themes, a keymap file, a bug-report wizard.** *Surge.* `theme.zig` is
one place already, so a theme file is cheap; a keymap file is cheap. Not
capability. After everything above.

**Scheduled downloads, and a bandwidth schedule by hour.** *IDM, XDM.*
Start at 02:00, or limit to 1 MB/s between nine and five. `nilo_job`
has `run_at`, so the first is a column already there; the second is the
rate limit plus a clock. Not asked for yet.

**BitTorrent, FTP, SFTP, Metalink files, video-site grabbing.** *aria2,
XDM.* Other jobs, or other programs. Refused; fdm is HTTP.
