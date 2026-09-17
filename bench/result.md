# fdm against curl, aria2 and Surge

What `bench/compare.py` measured, and what it changed. The first pass,
on real hosts, is below; the second, on a loopback shaped to be four
kinds of link, is first, because it is the one that can be run again
and come out the same.

## On a link that can be measured twice

Measured 2026-09-18 with `bench/local/`: nginx on the loopback with
three kinds of host, `tc` in front of it, fdm ReleaseSafe at the commit
that adds this section. Three rounds, interleaved. Surge's 9.0 s is its
terminal front, measured on the 100 KB file, and subtracted in `net`.

- **Machine**: Xeon Platinum 8255C, 2 threads, 7 GB, Ubuntu; a cloud VM
  whose own NIC hands out a burst allowance and then polices at
  100 Mbit — which is why the real hosts are not in this section.
- **curl** 8.5.0, **aria2** 1.37.0 (`-x16 -s16 -k1M`), **Surge** v0.12.1.

### A shared link: 100 Mbit, 40 ms round trip, 200 MB

The link is the limit and every tool should be curl. Before this pass
fdm-16 was 22.5 s here, five seconds behind curl, and every one of
those seconds was a reconnect of a segment that was slow only because
the others were fast.

| tool | best | median | worst | MB/s | cpu |
|---|---|---|---|---|---|
| fdm-16 | 16.5s | 16.6s | 16.6s | 12.1 | 0.39s |
| fdm-4 | 16.8s | 16.9s | 16.9s | 11.8 | 0.34s |
| curl | 17.2s | 17.4s | 17.4s | 11.5 | 0.34s |
| aria2-16 | 17.5s | 17.8s | 17.9s | 11.2 | 0.51s |
| surge | 17.1s | 19.1s | 19.1s | 10.5 (10.1s net) | 8.38s |

The same link at 200 ms: fdm-16 17.6 s, aria2 23.0 s, curl 24.1 s — a
single connection is window-bound at that distance and sixteen are not.

### A host whose connections differ: 300 KB/s, 1 MB/s or 3 MB/s by port, 60 MB

`mirrors.kernel.org` from one client. Before this pass fdm-16 was
12.4 s: the fast connections finished their sixteenth and the slow ones
held theirs to the end.

| tool | best | median | worst | MB/s | cpu |
|---|---|---|---|---|---|
| fdm-16 | 5.2s | 5.2s | 5.7s | 11.5 | 0.14s |
| aria2-16 | 6.4s | 8.0s | 8.5s | 7.5 | 0.18s |
| surge | 15.1s | 15.1s | 15.1s | 4.0 (6.1s net) | 5.73s |
| fdm-4 | 7.3s | 16.3s | 26.6s | 3.7 | 0.15s |
| curl | 18.9s | 57.2s | 57.2s | 1.0 | 0.11s |

5.2 s is the link: 60 MB at 12 MB/s is 4.8 s and the first second is
slow start.

### A host that caps every connection: 300 KB/s each, 60 MB

`speedtest.tele2.net`. Sixteen at 300 KB/s is 4.8 MB/s, so 12.5 s is
the floor for anything that holds sixteen; nginx lets the first bytes of
each request through unmetered, which is why fdm, with more requests,
reads under it.

| tool | best | median | worst | MB/s | cpu |
|---|---|---|---|---|---|
| fdm-16 | 9.7s | 9.9s | 9.9s | 6.1 | 0.16s |
| aria2-16 | 13.4s | 13.4s | 13.4s | 4.5 | 0.19s |
| surge | 25.1s | 25.1s | 25.1s | 2.4 (16.1s net) | 10.61s |
| fdm-4 | 44.9s | 45.0s | 45.1s | 1.3 | 0.23s |
| curl | 195.3s | 195.3s | 195.3s | 0.3 | 0.12s |

And the real one, the same evening, over the internet — 100 MB, two
rounds: fdm-16 18.9 s, aria2 20.9 s, Surge 31.1 s (22 s net).

### A burst allowance: 120 MB at wire speed, then 100 Mbit, 200 MB

The cloud VM's own link, emulated with `tbf`, twelve seconds between
runs for the bucket to refill. Before this pass fdm-16 was 8.2 s: the
first second's 136 MB/s made every second after it look like a link
with room, and the steals and reconnects that followed cost two.

| tool | best | median | worst |
|---|---|---|---|
| curl | 6.2s | 6.2s | 6.2s |
| fdm-16 | 6.3s | 6.5s | 6.9s |

### A small file: 100 KB

| tool | median |
|---|---|
| curl | 0.1s |
| aria2-16 | 0.2s |
| fdm-16 | 0.2s |
| surge | 9.1s (0.1s net) |

fdm was 0.7 s: half a second of `nilo_job`'s poll.

### What it changed

Four things in `download.zig`, each a number in `Settings` with a flag,
so a variant is a run and not a build (`compare.py --variant`):

1. **A steal or a reconnect waits for the link to have room**: the
   total under 0.75× its median second of the last ten, for two seconds
   — or under half, for one — and if the total is lower two seconds
   after an action than before it, both hold for ten seconds.
2. **The file is handed out from a frontier as connections finish**, a
   quarter planned up front, each chunk eight seconds at the rate that
   connection showed and at most a thirty-second of what is left. The
   connection is reused, so a chunk costs one round trip.
3. **The steal is judged in seconds and split by the two rates**, so
   half a megabyte on a 300 KB/s connection is taken and four on a
   10 MB/s one is not, and the thief gets the share it can carry.
4. **The queue polls at 100 ms and the loops at 50**, not 500 and 100.

## The first pass, on real hosts (2026-09-17)

### What was run

```
zig build -Doptimize=ReleaseSafe
python3 bench/compare.py --rounds 3 --fixed surge=9.0,fdm-16=0.2,fdm-4=0.2 <four urls>
python3 bench/compare.py --rounds 2 --skip curl --fixed ... http://speedtest.tele2.net/100MB.zip
```

Each round runs every tool once in a rotated order, so no tool always goes
first; every file's size is checked against the probe's `Content-Length`.
Wall seconds are from `fork` to exit. `fixed` is what each tool costs on a
100 KB file — Surge's terminal front takes 9.03 s to come and go on this
machine, fdm's 0.2 s — and the `net` columns subtract it. Read the wall
columns first: the subtraction is right for Surge on a small file and
overshoots on a large one, where part of the nine seconds overlaps the
transfer (its `net` on cdn.kernel.org comes out at 24 MB/s on an 11 MB/s
link, which is the arithmetic and not the network).

- **Machine**: AMD Ryzen 7 9700X, 16 threads, Linux 7.2.5, Wi-Fi to a
  link that tops out at **11.0–11.6 MB/s** on one stream.
- **fdm** at the commit that adds this file, ReleaseSafe. `-n 16` and
  `-n 4`; three parallel downloads is irrelevant here, there is one.
- **curl** 8.22.0, `curl -sS -L -o`. One connection.
- **Surge** v0.12.1 (Go), `--exit-when-done --no-server --no-resume`,
  driven through a pty because it needs one. Sixteen connections.
- **tele2** ran without curl in the final pass: the first pass had it at
  335.7 s and 353.8 s for 100 MB, and a third would have said nothing new.

### Numbers

#### Hosts where one connection gets the whole link

`cdn.kernel.org`, 147,906,904 bytes:

| tool | best | median | worst | MB/s |
|---|---|---|---|---|
| curl | 12.8s | 12.9s | 12.9s | 11.5 |
| fdm-16 | 12.8s | 12.9s | 13.0s | 11.5 |
| fdm-4 | 13.0s | 13.1s | 13.2s | 11.3 |
| surge | 15.0s | 15.0s | 15.0s | 9.8 (6.0s net) |

`nodejs.org`, 29,235,364 bytes:

| tool | best | median | worst | MB/s |
|---|---|---|---|---|
| curl | 2.6s | 2.7s | 2.7s | 11.0 |
| fdm-16 | 2.7s | 2.7s | 2.8s | 10.8 |
| fdm-4 | 2.7s | 2.7s | 2.7s | 10.8 |
| surge | 9.0s | 9.0s | 9.0s | 3.2 (0.0s net) |

`mirrors.kernel.org` (a 301 to `mirrors.edge.kernel.org` on every
request), 126,491,574 bytes:

| tool | best | median | worst | MB/s |
|---|---|---|---|---|
| curl | 14.0s | 14.2s | 14.2s | 8.9 |
| fdm-16 | 14.0s | 15.0s | 16.9s | 8.4 |
| fdm-4 | 14.6s | 16.7s | 18.9s | 7.6 |
| surge | 17.0s | 17.0s | 49.0s | 7.4 (8.0s net) |

**Unchanged.** When the link is the limit, sixteen connections carry what
one carries, and fdm is curl to within a second on the two clean hosts.
On mirrors.kernel.org fdm-16 is behind by a second at the median and its
spread is wider; the host itself gives each of sixteen connections a
different speed (a plain Python probe saw the same 7.9 MB take 7.2 s on
one and 15.5 s on another), and the steal is what brings the tail in.

#### A host that caps each connection

`speedtest.tele2.net`, 104,857,600 bytes, 0.3 MB/s a connection:

| tool | best | median | worst | MB/s |
|---|---|---|---|---|
| curl (first pass) | 335.7s | 344.8s | 353.8s | 0.3 |
| fdm-16 | 11.6s | 13.7s | 15.8s | 7.6 |
| fdm-4 | 67.0s | 67.8s | 68.6s | 1.5 |
| surge | 37.0s | 37.0s | 37.0s | 2.8 (28.0s net) |

**This is what the segments are for**, and it is where Surge's claim
comes from: fdm-16 is 25× curl and 2× Surge even after Surge's nine
seconds are taken off. fdm-4 at 67 s against the first pass's 37 s is the
same host on a different evening, and is why the cap is quoted as a
range rather than a figure.

#### A noisy host

`mirror.rackspace.com`, 126,491,574 bytes:

| tool | best | median | worst | MB/s |
|---|---|---|---|---|
| curl | 12.7s | 12.9s | 38.0s | 9.8 |
| fdm-16 | 17.7s | 17.8s | 22.3s | 7.1 |
| fdm-4 | 11.6s | 12.8s | 24.3s | 9.9 |
| surge | 15.0s | 27.0s | 47.0s | 4.7 (18.0s net) |

Every tool has a run here that is three times its best, in the same
hour, and an earlier pass had fdm-16 at 13.1–14.3 s against curl's
11.7–18.9 s. What the three passes agree on is that fdm-16 is not faster
than curl on this host and is sometimes slower; whether that is the
mirror giving a client with sixteen connections less than one with one,
or the hour, three rounds cannot say. Not a decision, and noted so the
next run does not treat one pass as one.

### What it changed

Three failures in the first pass were fdm's, and each moved code:

1. **fdm asked DNS once a connection; curl asks once.** On this machine
   the router does not answer an `AAAA` query for a name that has none,
   so a lookup is 0.5–6 s and one in four is `EAI_AGAIN`; rackspace
   failed three of three fdm-4 runs at 5.11 s with nothing on disk, and
   every stolen or reconnected segment sat at 0 KB/s until it was
   reconnected, which asked again. `src/dns.zig` swaps `netLookup` on
   the worker's Io for one that remembers, and the probe — the one
   request nothing else retried — gets a segment's three attempts.
2. **Every segment followed the redirect alone.** mirrors.kernel.org
   answers with a 301 and lets about eight handshakes a second through:
   fdm-16 took 39.8 s in a run curl took 14 s. The probe now returns the
   URL it ended on and the segments go there: 15.1 s, then the table
   above.
3. **A steal during a reconnect refused a correct answer.** The Range
   was built from `end`, a steal moved `end`, and the `Content-Length`
   check compared against the new value: `LengthMismatch`, one more
   reconnect. It compares against the end that was asked for.

And one about the measurement rather than the code: **a Debug fdm is 0.6
s slower on every host**, 0.67 s of CPU on TLS against 0.08 s, and one
pass was taken with one. `compare.py` prints the binary's size and says
so when it looks like Debug.

**Sixteen stays the default.** It costs nothing where it cannot win and
is the whole win where it can. Four is the number to try on a host like
rackspace, and `-n` is there for it.

### Can it be pushed further

Ranked by what the tables say, not by what would be interesting:

- **The link.** Every clean host reads 11 MB/s for everyone; nothing in
  fdm moves that, and a run on a faster link is the first thing to do
  before touching the code.
- **The mirrors.kernel.org tail.** fdm-16's worst of 16.9 s against a
  best of 14.0 s is segments finishing at different speeds. A steal
  needs 2 MB left on the victim and a reconnect needs 0.3× the mean; the
  Python probe suggests the spread is real on the server side, so the
  lever is stealing earlier and smaller near the end, and it is worth
  perhaps a second. *Done, above: the frontier, and it was worth seven
  seconds in twelve on the emulated version of this host.*
- **rackspace.** Find out whether it throttles by connection count
  before assuming so — ten rounds on a quiet hour with `-n 1`, `-n 4`,
  `-n 16` — and if it does, that is the argument for a per-host setting
  rather than a lower default.
- **Surge's nine seconds** are its terminal front and not its engine;
  its net numbers on tele2 (28 s) are the fair comparison, and fdm-16 is
  ahead of that by 2×.
