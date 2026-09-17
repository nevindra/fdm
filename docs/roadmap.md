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
`ETag`, the `nilo_job` queue, `--headless`, `fdm update`) is not listed.

## Next: cheap, and the shape of the program does not move

Each of these lands in `store.zig` and `download.zig`, or in `main.zig`'s
flag parsing, and nothing else has to change.

**Headers per download, and a paste of "Copy as cURL".** *Surge, aria2
`--header`, `--referer`, `--user-agent`.* fdm sends `range` and nothing
else (`download.zig`, the segment request), so anything behind a cookie,
a signed `Referer` or a bearer token cannot be fetched at all: Google
Drive, most file hosts, any CDN with a session. A `headers` column on
`downloads`, `--header` on the command line, and a parser for the `curl`
line a browser's "Copy as cURL" produces, which is `-H` and `-b` and the
URL. This is the one entry that opens a door to others (the browser
extension below needs it), and it is the most common reason a link fails
today. Costs: one column, one small parser, nothing per byte.

**`-o` and a default download directory.** *Surge
`default_download_dir`, aria2 `--dir`/`--out`.* Everything lands in the
cwd. A per-URL `-o path`, a default in config, and the name from
`Content-Disposition` when the server sends one (aria2, curl `-J`)
rather than the URL's last path segment, which on a redirecting host is
often `download` or a hash. Costs: a `path` decided at probe time rather
than at add time, which the store already holds.

**A rate limit, per download and global.** *Surge `surge limit <id>
<speed>`, `--global`; aria2 `--max-download-limit`,
`--max-overall-download-limit`.* A token bucket read before every chunk
in the segment loop; the global one is a second bucket shared through
the worker. `--limit 2M` on the command line and `l` in the TUI. Costs:
one atomic and one clock read per chunk, and nothing when the limit is
zero.

**`refresh <id> <url>`: a new URL for a paused or failed download.**
*Surge.* Signed URLs expire and mirrors go away, and today the answer is
delete and start over. Resume already asks the server before continuing
and compares `ETag` and length; the same check against a new URL is what
makes this safe, and if it disagrees, the download starts over on the
new link rather than stitching two files. Costs: one `Command`, one
`UPDATE`.

**Disk space checked at probe, and `ENOSPC` handled as final.** *Surge
`orchestrator/disk_precheck`, `scheduler/enospc_policy`.* The probe
knows the length; `statvfs` on the target directory knows the space.
Refusing at add time is one comparison. And a segment that hits
`ENOSPC` mid-file is today retried three times against the same full
disk, then a steal hands it more work; Surge's rule is the right one:
fail at once, no retry, no steal, no mirror, write the state so resume
carries on once there is room. Costs: nothing per byte.

**A duplicate is refused before it is queued.** *Surge
`warn_on_duplicate`.* One query on `downloads` for the same URL or the
same target path, and a prompt in the TUI, a non-zero exit in
`--headless`. Costs: one indexed lookup at add.

**`--batch file`, and `ls --json`.** *Surge `--batch`, aria2
`--input-file`.* One URL a line, `#` comments, for scripts. `--headless`
already prints one line per event; a `--json` form of that line and of
the list is a format, not a feature. Costs: none per byte.

**A checksum, verified at the end.** *aria2 `--checksum
sha-256=…`.* Release pages ship a sums file next to the binary and
nobody checks it by hand. `--sha256 <hex>` on add, hashed while the
segments are written or in one pass at the end, and a mismatch is a
failure with the file kept for inspection. `fdm update` already does
exactly this for itself (`update.zig`); the same code, opened to any
download. Costs: one hash pass over the file, which is disk-bound and
after the network is done.

**The remote modification time, kept.** *aria2 `--remote-time`, wget
`-N`.* `Last-Modified` is already read for the resume check; set it as
the file's mtime on completion, so a downloaded archive sorts where it
was published rather than when it arrived. Costs: one `utimes`.

**Auto-resume on start, opt-in.** *Surge `auto_resume`.* Running
downloads already resume on the next start; paused ones stay paused,
which is right, but a config flag that resumes those too is what a
person who quit to reboot wants. Costs: one query at start.

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
The split today is `size / n` decided once at the probe. In-order
delivery means a sliding window of segments advancing from the front,
which is a second segment strategy rather than a flag on the first, and
the steal has to know about it (stealing from the tail of a window is
fine; stealing from its head is not). Costs: a second `Strategy`, the
same per-segment memory.

**A grace period and a smoothed mean before a segment is called slow.**
*Surge `slow_worker_grace_period` 5s, `speed_ema_alpha` 0.3; aria2
`--lowest-speed-limit`.* The 0.3× mean check uses a raw mean and no
grace, so a segment whose TLS handshake is still in flight can be
judged against segments already streaming and reconnected for nothing.
An EMA and five seconds before a fresh connection is judged should cut
the false reconnects; whether it does is a number `bench/compare.py`
can take, and this entry does not land without it. Costs: nothing.

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

**Retry with a wait, and a resume that gives up.** *aria2
`--retry-wait`, `--max-resume-failure-tries`, `--always-resume=false`.*
The queue already backs off a whole download; a segment's three tries
are back to back. And a server that answered 206 at the probe and 200
on resume is today a fresh start every time, forever. Both are numbers
in `Options` rather than designs.

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
download and hand it over with its cookies. Needs the daemon and needs
the headers entry; the extension itself is a hundred lines of
JavaScript that is not fdm's to write until both are there.
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
