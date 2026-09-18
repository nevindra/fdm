"""fdm end to end, against a server in this process that misbehaves on cue.

    zig build e2e                 # builds, then runs this against zig-out/bin/fdm
    python3 test/e2e.py --fdm path/to/fdm [--keep] [-k stall]

Every scenario starts the binary `--headless` on a fresh database and a
fresh directory, against 127.0.0.1, and checks the exit code, the lines
it printed, the file's sha256 and what the server was asked. The server
is one deterministic file behind paths that say how to serve it:

    /file.bin            Range honoured, ETag and Last-Modified sent
    /noranges/file.bin   200 to everything, the whole file every time
    /stall/file.bin      the first body request sends half and holds the socket
    /cut/file.bin        the first body request sends half and closes
    /redirect/file.bin   301 to /file.bin
    /named/file.bin      Content-Disposition names it server-name.bin

The prefixes compose (`/stall/noranges/x.bin`), a "body request" is one
that is not the probe's `bytes=0-0`, and the first-only rule is per path,
so the retry is served whole. These are the runs `docs/history.md` did by
hand with `rangesrv.py`, kept where a change to `Segment.run` meets them.
"""
import argparse, hashlib, http.server, os, random, re, shutil, subprocess, sys, tempfile, threading, time
from email.utils import formatdate

ap = argparse.ArgumentParser()
ap.add_argument("--fdm", default=os.path.join(os.path.dirname(__file__), "..", "zig-out", "bin", "fdm"))
ap.add_argument("--keep", action="store_true", help="leave the scratch directory behind")
ap.add_argument("-k", default="", help="run only the scenarios whose name contains this")
ap.add_argument("--size", type=int, default=12 << 20, help="bytes in the file; 12 MiB is twelve segments at the default -n")
args = ap.parse_args()

DATA = random.Random(7).randbytes(args.size)
SHA = hashlib.sha256(DATA).hexdigest()
ETAG = '"e2e-%s"' % SHA[:12]
STALL_HOLD_S = 20

# What the server was asked, in order: (path, range or None). The tests read
# it; the lock is for the sixteen handler threads writing it.
asked = []
first_body_seen = set()
state_lock = threading.Lock()


class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def do_GET(self):
        parts = [p for p in self.path.split("/") if p]
        modes = set()
        while parts and parts[0] in ("noranges", "stall", "cut", "redirect", "named"):
            modes.add(parts.pop(0))
        name = parts[-1] if parts else "file.bin"
        rng = self.headers.get("Range")
        with state_lock:
            asked.append((self.path, rng))

        if "redirect" in modes:
            self.send_response(301)
            self.send_header("Location", "/" + name)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return

        start, end = 0, len(DATA) - 1
        ranged = rng is not None and "noranges" not in modes
        if ranged:
            m = re.fullmatch(r"bytes=(\d+)-(\d*)", rng)
            start = int(m.group(1))
            if m.group(2):
                end = min(int(m.group(2)), len(DATA) - 1)
        body = DATA[start:end + 1]
        probe = rng == "bytes=0-0"

        self.send_response(206 if ranged else 200)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Accept-Ranges", "bytes" if "noranges" not in modes else "none")
        self.send_header("ETag", ETAG)
        self.send_header("Last-Modified", formatdate(1_600_000_000, usegmt=True))
        if ranged:
            self.send_header("Content-Range", "bytes %d-%d/%d" % (start, end, len(DATA)))
        if "named" in modes:
            self.send_header("Content-Disposition", 'attachment; filename="server-name.bin"')
        self.end_headers()

        misbehave = None
        if not probe and ("stall" in modes or "cut" in modes):
            with state_lock:
                if self.path not in first_body_seen:
                    first_body_seen.add(self.path)
                    misbehave = "stall" if "stall" in modes else "cut"
        # A reset from fdm's side is not a server error here: a probe's
        # 200 is dropped with its connection, and a stolen segment's tail
        # is not read.
        try:
            if misbehave is None:
                self.wfile.write(body)
                return
            self.close_connection = True
            self.wfile.write(body[: len(body) // 2])
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            return
        if misbehave == "stall":
            time.sleep(STALL_HOLD_S)


class Server(http.server.ThreadingHTTPServer):
    daemon_threads = True

    def handle_error(self, request, client_address):
        if not isinstance(sys.exc_info()[1], (BrokenPipeError, ConnectionResetError)):
            super().handle_error(request, client_address)


server = Server(("127.0.0.1", 0), Handler)
PORT = server.server_address[1]
threading.Thread(target=server.serve_forever, daemon=True).start()
BASE = "http://127.0.0.1:%d" % PORT

root = tempfile.mkdtemp(prefix="fdm-e2e-")
fdm = os.path.abspath(args.fdm)
failures = []


def url(path):
    return BASE + path


def run(name, path, *flags, expect_ok=True, refresh=None, db=None):
    """One fdm process. Returns (exit code, stderr text, file path)."""
    d = os.path.join(root, name)
    os.makedirs(d, exist_ok=True)
    db = db or os.path.join(d, "fdm.db")
    cmd = [fdm]
    if refresh is not None:
        cmd += ["refresh", str(refresh)]
    cmd += [url(path), "--headless", "--db", db, "--dir", d, *flags]
    with state_lock:
        asked.clear()
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    if (p.returncode == 0) != expect_ok:
        raise AssertionError("exit %d, expected %s\n%s" % (p.returncode, "0" if expect_ok else "non-zero", p.stderr))
    return p.returncode, p.stderr, os.path.join(d, os.path.basename(path))


def sha(path):
    with open(path, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()


def check(cond, what):
    if not cond:
        raise AssertionError(what)


def body_ranges():
    """Range starts of every non-probe ranged request the server saw."""
    out = []
    for _, rng in asked:
        if rng is None or rng == "bytes=0-0":
            continue
        out.append(int(rng.split("=")[1].split("-")[0]))
    return out


# ------------------------------------------------------------- scenarios

def ranged():
    _, err, f = run("ranged", "/file.bin")
    check(sha(f) == SHA, "hash")
    check(", 12 segments" in err, "twelve segments planned:\n" + err)
    check(len(body_ranges()) >= 12, "the server saw at least twelve ranges")


def noranges():
    _, err, f = run("noranges", "/noranges/file.bin")
    check(sha(f) == SHA, "hash")
    check(", 1 segments" in err, "one stream when Range is ignored:\n" + err)


def stall_one_segment():
    _, err, f = run("stall-one", "/stall/a.bin", "-n", "1", "--stall", "1500", "--retries", "2")
    check(sha(f) == SHA, "hash")
    check("no bytes for 1500ms, retrying" in err, "the stall was noticed:\n" + err)
    half = len(DATA) // 2
    check(any(abs(s - half) < 70000 for s in body_ranges()), "the retry asked from about half, not from the top: %r" % body_ranges())


def stall_unsliceable():
    # The retry starts at zero because the server will send from zero.
    _, err, f = run("stall-unsliceable", "/stall/noranges/b.bin", "-n", "1", "--stall", "1500", "--retries", "2")
    check(sha(f) == SHA, "hash")
    check("no bytes for 1500ms, retrying" in err, "the stall was noticed:\n" + err)


def stall_sixteen():
    _, err, f = run("stall-sixteen", "/stall/c.bin", "--stall", "1500")
    check(sha(f) == SHA, "hash")
    check("no bytes for 1500ms, retrying" in err, "one segment stalled and came back:\n" + err)


def resume_across_runs():
    # Run one: the connection is cut halfway and there are no retries.
    _, err, f = run("resume", "/cut/d.bin", "-n", "1", "--retries", "0", expect_ok=False)
    check("failed" in err, "the first run failed:\n" + err)
    db = os.path.join(root, "resume", "fdm.db")
    # Run two: `refresh` queues the failed row again; same server, same
    # ETag, so it carries on from what the file has.
    _, err, f = run("resume", "/cut/d.bin", "-n", "1", refresh=1, db=db)
    check(sha(f) == SHA, "hash")
    check("resumed" in err, "the second run resumed rather than restarted:\n" + err)
    check(all(s > 0 for s in body_ranges()), "nothing was asked from the top again: %r" % body_ranges())


def redirect():
    _, err, f = run("redirect", "/redirect/e.bin")
    check(sha(f) == SHA, "hash")
    check("redirected to %s/e.bin" % BASE in err, "the probe reported where it ended:\n" + err)
    check(all(p == "/e.bin" for p, r in asked if r != "bytes=0-0" and not p.startswith("/redirect")), "segments went to where the probe ended")
    check(sum(1 for p, _ in asked if p.startswith("/redirect")) == 1, "only the probe walked the redirect: %r" % asked)


def sha256_checked():
    _, err, f = run("sha-ok", "/file.bin", "--sha256", SHA)
    check("sha256 verified" in err, "a right hash is verified:\n" + err)
    _, err, f = run("sha-bad", "/file.bin", "--sha256", "0" * 64, expect_ok=False)
    check("sha256 mismatch" in err, "a wrong hash fails the download:\n" + err)
    check(os.path.exists(f), "the file is kept for the person to look at")


def named_by_server():
    _, err, f = run("named", "/named/file.bin")
    got = os.path.join(os.path.dirname(f), "server-name.bin")
    check("named server-name.bin by the server" in err, "the server's name was taken:\n" + err)
    check(os.path.exists(got) and sha(got) == SHA, "the file landed under the server's name")


scenarios = [
    ranged, noranges, stall_one_segment, stall_unsliceable, stall_sixteen,
    resume_across_runs, redirect, sha256_checked, named_by_server,
]

print("fdm: %s, server: %s, %d MiB" % (fdm, BASE, args.size >> 20), flush=True)
for fn in scenarios:
    if args.k and args.k not in fn.__name__:
        continue
    with state_lock:
        first_body_seen.clear()
    t0 = time.monotonic()
    try:
        fn()
        print("  ok    %-22s %5.1fs" % (fn.__name__, time.monotonic() - t0), flush=True)
    except Exception as e:  # noqa: BLE001 — one line per scenario, the rest keep running
        failures.append(fn.__name__)
        print("  FAIL  %-22s %5.1fs  %s" % (fn.__name__, time.monotonic() - t0, e), flush=True)

if failures:
    print("failed: %s (scratch kept at %s)" % (", ".join(failures), root))
    sys.exit(1)
if not args.keep:
    shutil.rmtree(root, ignore_errors=True)
print("all passed")
