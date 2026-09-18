<h1 align="center">fdm</h1>

<p align="center">
  A fast download manager for the terminal.<br>
  Sixteen connections per file, resume from anywhere, one SQLite file that remembers it all.
</p>

<p align="center">
  <a href="https://github.com/nevindra/fdm/releases"><img alt="release" src="https://img.shields.io/github/v/release/nevindra/fdm?include_prereleases&sort=semver"></a>
  <a href="https://github.com/nevindra/fdm/actions/workflows/ci.yml"><img alt="ci" src="https://github.com/nevindra/fdm/actions/workflows/ci.yml/badge.svg"></a>
  <img alt="platforms" src="https://img.shields.io/badge/linux%20%7C%20macos%20%7C%20windows-x86__64%20%7C%20arm64-blue">
  <img alt="zig" src="https://img.shields.io/badge/zig-0.16-f7a41d">
  <a href="LICENSE"><img alt="license" src="https://img.shields.io/badge/license-MIT-green"></a>
</p>

## Why fdm

- **Fast where it matters.** A file is split across up to sixteen connections, the work is handed out as connections finish rather than divided once, a slow connection is replaced, and a finished one takes over part of the slowest. On a CDN whose edges differ, that is 60 MB in 5.2 s to aria2's 8.0 s and Surge's 15.1 s. On a plain link, it is curl minus a second.
- **Nothing is lost.** Every segment's progress lands in one SQLite file, once a second and on every way out. Quit, crash, `kill -9`, reboot: the next start carries on from the last byte each segment had, after checking the server still has the same file.
- **Paste what the browser gave you.** A URL, or the whole line from "Copy as cURL" with its cookies and headers. `-o`, `--sha256`, `--batch` for a list of them.
- **A terminal UI that stays out of the way.** The list on the left, the selected download's segments, graph and log on the right. Add, pause, resume, give a dead link a new URL, delete. Or `--headless` and `--json` for scripts and cron.
- **One binary, no runtime.** Zig, static, a few MB. `fdm update` fetches the next release and checks it against the published checksums.

## Install

Linux and macOS, x86_64 and arm64:

```sh
curl -fsSL https://raw.githubusercontent.com/nevindra/fdm/master/install.sh | sh
```

That puts the binary for your machine at `~/.local/bin/fdm`, checked against the release's `sha256sums.txt`. `FDM_INSTALL_DIR` picks another directory; `FDM_VERSION=v0.1.0-rc1` picks a pre-release.

**Windows:** download `fdm-x86_64-windows.exe` from the [releases page](https://github.com/nevindra/fdm/releases), rename it `fdm.exe`, put it on your `PATH`.

**From source**, with Zig 0.16 (dependencies are fetched by commit, nothing has to sit beside the checkout):

```sh
zig build -Doptimize=ReleaseSafe
```

Later, `fdm update` swaps in the newest release; `fdm --version` says which one you have.

## Quick start

```sh
fdm https://example.com/big.iso                 # open the TUI, start downloading
fdm https://example.com/big.iso -o ~/isos/      # into a directory
fdm https://example.com/big.iso --sha256 3a7f…  # verify when done
fdm 'curl https://example.com/file -H "Cookie: session=…"'   # a pasted curl line, quoted
fdm --batch urls.txt                            # one URL or curl line per line
fdm --headless --json https://example.com/a https://example.com/b   # for scripts
fdm ls                                          # what is in the list
fdm refresh 3 https://example.com/new-signed-url   # a download whose link expired
```

A file lands in the current directory (or `--dir`) under the URL's last path segment, unless the server names it with `Content-Disposition`, or you do with `-o`. The file's modification time is the server's `Last-Modified`, so an archive sorts where it was published. A URL already in the list, or a path already taken, is refused unless you say `--force`. A file that will not fit is refused before a byte lands.

## In the terminal

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
| `q` | quit; running downloads resume on the next start |

At 100 columns or wider there are two panes: the list, and the selected download's URL, path, ETA, one bar per segment and its log. Logs go to `<db>.log`, never the screen.

## Flags

```
fdm [url ...] [-o path] [-H header] [--sha256 hex] [--dir path] [--batch file] [--force]
    [-n segments] [-p parallel] [--stall ms] [--retries n] [--retry-wait ms] [--auto-resume]
    [--db file] [--headless] [--json]
fdm refresh <id> <url> [-H header] [--headless] [--json]
fdm ls [--json]
fdm update | fdm --version
```

| flag | default | what |
|---|---|---|
| `-o` | | where the n-th URL goes: a file, or a directory when it ends in `/` or is one |
| `-H` | | a `Name: value` sent with every request for these URLs; repeatable |
| `--sha256` | | what the n-th URL's file must hash to; a mismatch fails it and keeps the file |
| `--dir` | the cwd | where a URL without `-o` lands |
| `--batch` | | a file with one URL, or one `curl` line, per line; `#` comments, `\` continues a line, `url sha256=<hex>` |
| `--force` | | add a URL that is already in the list, or whose file is |
| `-n` | 16 | connections per download |
| `-p` | 3 | downloads running at once; the rest queue |
| `--stall` | 10000 | ms a segment may go without a byte before it is reconnected |
| `--retries` | 3 | attempts per segment, and per probe, before the download fails |
| `--retry-wait` | 0 | ms a failed segment waits before its next attempt |
| `--auto-resume` | | at start, queue the paused downloads too |
| `--db` | `$XDG_DATA_HOME/fdm/fdm.db` | the list, the segments and the queue |
| `--headless` | | no screen; one line per event, exit when the URLs given are done, non-zero if one failed |
| `--json` | | each `--headless` line, and `fdm ls`, as JSON |

A `url` may be the line a browser's "Copy as cURL" writes, quoted as one argument: the URL and every `-H`, `-b`, `-A`, `-e`, `-u` and `-o` on it are read, the rest of curl's options are skipped. `fdm refresh <id> <url>` gives a paused or failed download a new link and queues it; if the new server says it is the same file, the download carries on from where it was.

<details>
<summary>Tuning flags, for the benchmark and the curious</summary>

| flag | default | what |
|---|---|---|
| `--slow` | 0.3 | a segment under this fraction of the others' mean rate is reconnected; 0 turns it off |
| `--slow-checks` | 1 | health checks (two seconds apart) a segment must be slow for first |
| `--slow-per-check` | 255 | reconnects allowed in one check |
| `--steal-min` | 2 | seconds a segment must still need before a finished one takes part of it |
| `--read-buffer` | 8192 | bytes each connection reads into at a time; measured at nothing over TLS |

The defaults are what [`bench/result.md`](bench/result.md) settled on; `bench/compare.py --variant` runs one more fdm with other flags beside the default.
</details>

## How it works

**Segments.** A probe asks the server for one byte with a `Range`. A 206 says it can slice and how big the file is, and the file is split into `-n` segments, each a task writing at its own offset. A 200 means one stream. The probe follows any redirect once and hands the segments the URL it ended on.

**Connections are not equal**, so three things follow: sixteen of them, so a per-connection cap stops being the limit; a segment running under 0.3× the mean of the others is reconnected; and a finished segment takes half of what the longest running one has left, without cancelling it.

**Resume.** Progress is counted from bytes that reached the disk, never a writer's buffer. Before resuming, the server is asked again; a different `ETag` or length starts over rather than stitching two files into one.

**The queue.** Each download is a row; `-p` workers claim them in order; a download that fails as a whole is retried with backoff, and a 404 is final. Quitting hands running rows back to the queue.

## Benchmarks

curl, aria2, fdm and Surge, same URLs, interleaved, three rounds, ReleaseSafe. The full tables and how they were taken are in [`bench/result.md`](bench/result.md).

| link | size | fdm | aria2 | curl | Surge |
|---|---|---|---|---|---|
| one link, 100 Mbit, 40 ms | 200 MB | **16.6 s** | 17.8 s | 17.4 s | 19.1 s |
| a host that caps each connection | 100 MB | **19 s** | 21 s | | 31 s |
| a CDN whose edges differ | 60 MB | **5.2 s** | 8.0 s | 57.2 s | 15.1 s |

## Development

```sh
zig build                 # debug build in zig-out/bin/fdm
zig build test            # unit tests
zig build e2e             # the binary against a local server that stalls, cuts and redirects
```

CI runs all three on Linux, macOS and Windows on every tag, and a `v*` tag becomes a release with one binary per platform (`-rcN` tags become pre-releases). The worker is `src/download.zig`, the tables are `src/store.zig`, the screen is `src/tui.zig`, and `src/main.zig` wires them; the seam between worker and screen is the point, so a window could sit where the terminal does without the worker changing. What was tried, measured and found wrong on the way is in [`docs/history.md`](docs/history.md); what is coming, and what is refused, in [`docs/roadmap.md`](docs/roadmap.md).

Linux is where it is used. macOS and Windows build, pass the tests and run the end-to-end scenarios in CI; what has not happened yet is a person watching a download in the TUI on the other two.

## Known limits

- **One process per database.** A job's lease is a day and `running` rows are released at start, which is right for one fdm owning the file and wrong for two on the same `--db`.
- **Memory per segment** is a 64 KiB write buffer plus what `std.http.Client` holds for TLS: thirty-two open segments is about 4 MB in buffers.
- **The Linux binaries are 16 MB** to macOS's 4 MB, most likely debug info the ELF keeps under ReleaseSafe. Same speed, more disk.

## Credits

Built on [nilo](https://github.com/nevindra/nilo) for the wire, the file and the queue, and [libvaxis](https://github.com/rockorager/libvaxis) for the screen. MIT licensed.
