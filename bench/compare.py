"""fdm against curl and Surge, on real hosts, interleaved.

    python3 bench/compare.py --rounds 3 --out /tmp/bench https://host/file ...

Every tool downloads the whole file into a fresh directory; the run is
timed from spawn to exit, from outside, in a pty (Surge and fdm's TUI
both need one; fdm runs `--headless` here). The file's size is checked
against the first run's, and the file is deleted between runs. Tools
rotate order each round, because a host's per-connection rate drifts by
the minute and the fair comparison is neighbours in time.

Prints one markdown table per URL: seconds and MB/s per tool, best and
median over the rounds, plus the spread — a margin inside the spread is
"unchanged", not a win. `--fixed` subtracts a per-tool constant measured
on a tiny file first (Surge spends 9.0 s per run before it exits, whatever
the size), and the table then carries both the raw and the net figure.
"""
import argparse, os, pty, select, shutil, statistics, subprocess, sys, time, fcntl, termios, struct, urllib.parse

ap = argparse.ArgumentParser()
ap.add_argument("urls", nargs="+")
ap.add_argument("--rounds", type=int, default=3)
ap.add_argument("--out", default="/tmp/fdm-bench")
ap.add_argument("--fdm", default=os.path.join(os.path.dirname(__file__), "..", "zig-out", "bin", "fdm"))
ap.add_argument("--surge", default=shutil.which("Surge") or shutil.which("surge"))
ap.add_argument("--skip", default="", help="comma-separated tool names to leave out, e.g. curl")
ap.add_argument("--fixed", default="", help="per-tool seconds to subtract, measured on a tiny file: surge=9.0,fdm-16=0.2")
args = ap.parse_args()
fixed = {k: float(v) for k, v in (kv.split("=") for kv in args.fixed.split(",") if kv)}

# A Debug fdm spends 0.6 s of CPU on TLS that ReleaseSafe spends 0.08 s on,
# and one evening's tables were taken with it: say which binary this is.
fdm_size = os.path.getsize(args.fdm)
print(f"fdm: {args.fdm} ({fdm_size/1e6:.1f} MB){'  ** looks like a Debug build; zig build -Doptimize=ReleaseSafe **' if fdm_size > 18e6 else ''}", flush=True)

def run_pty(argv, cwd):
    """Wall seconds, exit code, and the last 2 KB of output of argv run in a pty."""
    tail = b""
    t0 = time.time()
    pid, fd = pty.fork()
    if pid == 0:
        os.chdir(cwd)
        os.environ["TERM"] = "xterm-256color"
        os.execvp(argv[0], argv)
    fcntl.ioctl(fd, termios.TIOCSWINSZ, struct.pack("HHHH", 40, 160, 0, 0))
    while True:
        r, _, _ = select.select([fd], [], [], 0.2)
        if r:
            try:
                data = os.read(fd, 65536)
            except OSError:
                break
            tail = (tail + data)[-2048:]
            if b"\x1b[6n" in data: os.write(fd, b"\x1b[1;1R")
            if b"\x1b[5n" in data: os.write(fd, b"\x1b[0n")
        else:
            p, st = os.waitpid(pid, os.WNOHANG)
            if p: break
    try:
        _, st = os.waitpid(pid, 0)
    except ChildProcessError:
        st = 0
    return time.time() - t0, os.waitstatus_to_exitcode(st), tail

def tools(url, outdir):
    name = os.path.basename(urllib.parse.urlparse(url).path)
    t = {
        "curl":   (["curl", "-sSL", "-o", name, url], name),
        "fdm-16": ([args.fdm, "--headless", "--db", os.path.join(outdir, "fdm.db"), "-n", "16", url], name),
        "fdm-4":  ([args.fdm, "--headless", "--db", os.path.join(outdir, "fdm.db"), "-n", "4", url], name),
    }
    if args.surge:
        t["surge"] = ([args.surge, url, "--exit-when-done", "--no-server", "--no-resume", "--insecure-http", "-o", outdir], name)
    for s in args.skip.split(","):
        t.pop(s, None)
    return t

def clean(outdir):
    shutil.rmtree(outdir, ignore_errors=True)
    os.makedirs(outdir)

results = {}
for url in args.urls:
    outdir = os.path.join(args.out, "run")
    names = list(tools(url, outdir).keys())
    res = {n: [] for n in names}
    expected = None
    for r in range(args.rounds):
        order = names[r % len(names):] + names[:r % len(names)]
        for n in order:
            clean(outdir)
            argv, fname = tools(url, outdir)[n]
            secs, code, tail = run_pty(argv, outdir)
            path = os.path.join(outdir, fname)
            size = os.path.getsize(path) if os.path.exists(path) else 0
            ok = code == 0 and size > 0 and (expected is None or size == expected)
            if ok and expected is None: expected = size
            res[n].append((secs, size, ok))
            print(f"  {url.split('/')[2]:28} round {r+1} {n:8} {secs:7.2f}s  {size/1e6/secs:6.2f} MB/s  {'ok' if ok else 'FAILED code=%d size=%d' % (code, size)}", flush=True)
            if not ok:
                for line in tail.decode(errors="replace").splitlines()[-6:]:
                    print("      | " + line.strip(), flush=True)
    results[url] = (res, expected)
    clean(outdir)

print()
for url, (res, expected) in results.items():
    print(f"### {url}")
    print(f"{expected or 0:,} bytes, {args.rounds} rounds, interleaved\n")
    print("| tool | best | median | worst | MB/s (median) | fixed | net median | net MB/s |")
    print("|---|---|---|---|---|---|---|---|")
    for n, runs in res.items():
        good = [s for s, _, ok in runs if ok]
        if not good:
            print(f"| {n} | failed | | | | | | |")
            continue
        med = statistics.median(good)
        fx = fixed.get(n, 0.0)
        net = max(med - fx, 0.01)
        print(f"| {n} | {min(good):.1f}s | {med:.1f}s | {max(good):.1f}s | {expected/1e6/med:.1f} | {fx:.1f}s | {net:.1f}s | {expected/1e6/net:.1f} |")
    print()
