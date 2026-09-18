#!/bin/sh
# fdm, in one line, on Linux and macOS:
#
#     curl -fsSL https://raw.githubusercontent.com/nevindra/fdm/master/install.sh | sh
#
# Picks the binary for this machine off the latest release, checks it
# against the release's sha256sums.txt, and puts it at ~/.local/bin/fdm.
# From then on `fdm update` does the same thing from inside fdm.
#
#     FDM_VERSION=v0.1.0-rc1   a tag rather than the latest release
#     FDM_INSTALL_DIR=/usr/local/bin   somewhere other than ~/.local/bin
#
# Windows: take fdm-x86_64-windows.exe from the releases page, rename it
# fdm.exe, put it on PATH.
set -eu

repo="nevindra/fdm"
dir="${FDM_INSTALL_DIR:-$HOME/.local/bin}"

case "$(uname -s)" in
  Linux)  os=linux ;;
  Darwin) os=macos ;;
  *) echo "install.sh: $(uname -s) is not a platform a release is built for; see https://github.com/$repo/releases" >&2; exit 1 ;;
esac
case "$(uname -m)" in
  x86_64|amd64)  arch=x86_64 ;;
  aarch64|arm64) arch=aarch64 ;;
  *) echo "install.sh: no build for $(uname -m)" >&2; exit 1 ;;
esac
asset="fdm-$arch-$os"

# The tag: the one asked for, or what GitHub calls the latest release,
# which leaves pre-releases out the way `fdm update` does.
if [ -n "${FDM_VERSION:-}" ]; then
  tag="$FDM_VERSION"
else
  tag=$(curl -fsSL "https://api.github.com/repos/$repo/releases/latest" 2>/dev/null | sed -n 's/.*"tag_name": *"\([^"]*\)".*/\1/p' | head -n 1)
  [ -n "$tag" ] || { echo "install.sh: no release published yet; FDM_VERSION=vX.Y.Z-rcN takes a pre-release" >&2; exit 1; }
fi
base="https://github.com/$repo/releases/download/$tag"

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
echo "fdm $tag, $asset"
curl -fsSL -o "$tmp/$asset" "$base/$asset"
curl -fsSL -o "$tmp/sha256sums.txt" "$base/sha256sums.txt"

# One line of the sums file, checked with whichever tool the machine has.
want=$(awk -v a="$asset" '$2 == a { print $1 }' "$tmp/sha256sums.txt")
[ -n "$want" ] || { echo "install.sh: sha256sums.txt has no line for $asset" >&2; exit 1; }
if command -v sha256sum >/dev/null 2>&1; then
  got=$(sha256sum "$tmp/$asset" | awk '{ print $1 }')
else
  got=$(shasum -a 256 "$tmp/$asset" | awk '{ print $1 }')
fi
[ "$got" = "$want" ] || { echo "install.sh: $asset does not match sha256sums.txt; not installed" >&2; exit 1; }

mkdir -p "$dir"
chmod +x "$tmp/$asset"
mv -f "$tmp/$asset" "$dir/fdm"
echo "installed $("$dir/fdm" --version 2>&1) at $dir/fdm"

case ":$PATH:" in
  *":$dir:"*) ;;
  *) echo "$dir is not on your PATH; add it, e.g.:  export PATH=\"$dir:\$PATH\"" ;;
esac
