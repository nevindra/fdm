"""fdm against curl, aria2 and Surge, on real hosts, interleaved.

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
import argparse, os, pty, resource, select, shutil, statistics, subprocess, sys, time, fcntl, termios, struct, urllib.parse

ap = argparse.ArgumentParser()
ap.add_argument("urls", nargs="+")
ap.add_argument("--rounds", type=int, default=3)
ap.add_argument("--out", default="/tmp/fdm-bench")
ap.add_argument("--fdm", default=os.path.join(os.path.dirname(__file__), "..", "zig-out", "bin", "fdm"))
ap.add_argument("--surge", default=shutil.which("Surge") or shutil.which("surge"))
ap.add_argument("--aria2", default=shutil.which("aria2c"))
ap.add_argument("--only", default="", help="comma-separated tool names to run, and nothing else")
ap.add_argument("--variant", action="append", default=[], help="name=args: one more fdm, run with these flags, as tool fdm:name — for trying a setting against the default")
ap.add_argument("--rest", type=float, default=0, help="seconds to sleep before every timed run — on a cloud VM with a burst bucket, what refills it")
ap.add_argument("--drain", default="", help="a URL to pull ~150 MB from before every timed run — what empties that bucket, so every tool sees the sustained rate")
ap.add_argument("--skip", default="", help="comma-separated tool names to leave out, e.g. curl")
ap.add_argument("--fixed", default="", help="per-tool seconds to subtract, measured on a tiny file: surge=9.0,fdm-16=0.2")
args = ap.parse_args()
fixed = {k: float(v) for k, v in (kv.split("=") for kv in args.fixed.split(",") if kv)}

# A Debug fdm spends 0.6 s of CPU on TLS that ReleaseSafe spends 0.08 s on,
# and one evening's tables were taken with it: say which binary this is.
fdm_size = os.path.getsize(args.fdm)
print(f"fdm: {args.fdm} ({fdm_size/1e6:.1f} MB){'  ** looks like a Debug build; zig build -Doptimize=ReleaseSafe **' if fdm_size > 18e6 else ''}", flush=True)

def run_pty(argv, cwd):
    """Wall seconds, CPU seconds, exit code, and the last 2 KB of output of argv run in a pty."""
    tail = b""
    ru0 = resource.getrusage(resource.RUSAGE_CHILDREN)
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
    wall = time.time() - t0
    ru1 = resource.getrusage(resource.RUSAGE_CHILDREN)
    cpu = (ru1.ru_utime - ru0.ru_utime) + (ru1.ru_stime - ru0.ru_stime)
    return wall, cpu, os.waitstatus_to_exitcode(st), tail

def tools(url, outdir):
    name = os.path.basename(urllib.parse.urlparse(url).path)
    t = {
        "curl":   (["curl", "-sSL", "-o", name, url], name),
        "fdm-16": ([args.fdm, "--headless", "--db", os.path.join(outdir, "fdm.db"), "-n", "16", url], name),
        "fdm-4":  ([args.fdm, "--headless", "--db", os.path.join(outdir, "fdm.db"), "-n", "4", url], name),
    }
    for v in args.variant:
        vname, _, vargs = v.partition("=")
        t["fdm:" + vname] = ([args.fdm, "--headless", "--db", os.path.join(outdir, "fdm.db")] + vargs.split() + [url], name)
    if args.aria2:
        t["aria2-16"] = ([args.aria2, "-q", "--console-log-level=error", "-x", "16", "-s", "16", "-k", "1M", "--file-allocation=none", "--allow-overwrite=true", "-d", outdir, "-o", name, url], name)
    if args.surge:
        t["surge"] = ([args.surge, url, "--exit-when-done", "--no-server", "--no-resume", "--insecure-http", "-o", outdir], name)
    for s in args.skip.split(","):
        t.pop(s, None)
    if args.only:
        t = {k: v for k, v in t.items() if k in args.only.split(",") or k.startswith("fdm:")}
    return t

def drain(url):
    """Pull 160 MB over sixteen connections, again while that still ran
    faster than the sustained rate: one curl at that rate barely outruns
    the bucket's refill and drains nothing, and a full bucket holds more
    than one batch."""
    while True:
        t0 = time.time()
        procs = [subprocess.Popen(["curl", "-sS", "-r", f"{i*9000000}-{i*9000000+9999999}", "-o", "/dev/null", url]) for i in range(16)]
        for p in procs: p.wait()
        rate = 160 / (time.time() - t0)
        if rate < 16: return

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
            if args.drain:
                drain(args.drain)
            if args.rest:
                time.sleep(args.rest)
            argv, fname = tools(url, outdir)[n]
            secs, cpu, code, tail = run_pty(argv, outdir)
            path = os.path.join(outdir, fname)
            size = os.path.getsize(path) if os.path.exists(path) else 0
            ok = code == 0 and size > 0 and (expected is None or size == expected)
            if ok and expected is None: expected = size
            res[n].append((secs, cpu, size, ok))
            print(f"  {url.split('/')[2]:28} round {r+1} {n:8} {secs:7.2f}s  {size/1e6/secs:6.2f} MB/s  cpu {cpu:5.2f}s  {'ok' if ok else 'FAILED code=%d size=%d' % (code, size)}", flush=True)
            if not ok:
                for line in tail.decode(errors="replace").splitlines()[-6:]:
                    print("      | " + line.strip(), flush=True)
    results[url] = (res, expected)
    clean(outdir)

print()
for url, (res, expected) in results.items():
    print(f"### {url}")
    print(f"{expected or 0:,} bytes, {args.rounds} rounds, interleaved\n")
    print("| tool | best | median | worst | MB/s (median) | cpu (median) | fixed | net median | net MB/s |")
    print("|---|---|---|---|---|---|---|---|---|")
    for n, runs in res.items():
        good = [(s, c) for s, c, _, ok in runs if ok]
        if not good:
            print(f"| {n} | failed | | | | | | | |")
            continue
        med = statistics.median(s for s, _ in good)
        cpu = statistics.median(c for _, c in good)
        fx = fixed.get(n, 0.0)
        net = max(med - fx, 0.01)
        print(f"| {n} | {min(s for s, _ in good):.1f}s | {med:.1f}s | {max(s for s, _ in good):.1f}s | {expected/1e6/med:.1f} | {cpu:.2f}s | {fx:.1f}s | {net:.1f}s | {expected/1e6/net:.1f} |")
    print()
