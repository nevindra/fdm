#!/bin/sh
# bench/local/shape.sh shared|rtt200|burst|off — what the loopback is,
# with tc (needs sudo). Every packet crosses `lo` once, so the rate is
# shared by both directions, which the ACKs barely touch.
#
#   shared   100 Mbit, 40 ms round trip: a home link
#   rtt200   100 Mbit, 200 ms round trip: the same link to another continent
#   burst    100 Mbit sustained after a 120 MB allowance, 40 ms: a cloud
#            VM's bucket — the first second goes at wire speed, the rest
#            at the rate. Give it 12 s between runs to refill
#            (`compare.py --rest 12`).
#   off      nothing in the way
set -e
sudo tc qdisc del dev lo root 2>/dev/null || true
case "$1" in
  shared) sudo tc qdisc add dev lo root netem delay 20ms rate 100mbit limit 10000 ;;
  rtt200) sudo tc qdisc add dev lo root netem delay 100ms rate 100mbit limit 20000 ;;
  burst)
    sudo tc qdisc add dev lo root handle 1: tbf rate 100mbit burst 120mb latency 2000ms
    sudo tc qdisc add dev lo parent 1:1 handle 10: netem delay 20ms limit 10000 ;;
  off) ;;
  *) echo "usage: $0 shared|rtt200|burst|off"; exit 2 ;;
esac
tc qdisc show dev lo
