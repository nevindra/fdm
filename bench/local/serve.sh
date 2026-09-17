#!/bin/sh
# bench/local/serve.sh up|down — nginx on 127.0.0.1:8088 over
# /tmp/fdm-www: big.bin 200 MB, med.bin 60 MB, mid.bin 20 MB, tiny.bin
# 100 KB, all from /dev/urandom. Needs nginx installed; runs as you.
set -e
here=$(dirname "$0")
case "$1" in
  up)
    mkdir -p /tmp/fdm-www /tmp/fdm-ngx
    [ -f /tmp/fdm-www/big.bin ]  || head -c 200000000 /dev/urandom > /tmp/fdm-www/big.bin
    [ -f /tmp/fdm-www/med.bin ]  || head -c 60000000  /dev/urandom > /tmp/fdm-www/med.bin
    [ -f /tmp/fdm-www/mid.bin ]  || head -c 20000000  /dev/urandom > /tmp/fdm-www/mid.bin
    [ -f /tmp/fdm-www/tiny.bin ] || head -c 100000    /dev/urandom > /tmp/fdm-www/tiny.bin
    nginx -c "$(cd "$here" && pwd)/nginx.conf"
    echo "http://127.0.0.1:8088/{big,med,mid,tiny}.bin, /capped/…, /mixed/…"
    ;;
  down)
    [ -f /tmp/fdm-ngx/nginx.pid ] && nginx -c "$(cd "$here" && pwd)/nginx.conf" -s quit
    rm -rf /tmp/fdm-www /tmp/fdm-ngx
    ;;
  *) echo "usage: $0 up|down"; exit 2 ;;
esac
