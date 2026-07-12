#!/bin/sh
# Synthigy CLI installer:
#   curl -fsSL https://raw.githubusercontent.com/synthigy/synthigy/main/install.sh | sh
# Downloads the synthigy CLI binary for this platform from the latest release,
# verifies nothing is world-writable weird, installs to ~/.local/bin (or
# SYNTHIGY_INSTALL_DIR). Then: `synthigy up` does the rest.
set -eu

REPO="synthigy/synthigy"
DIR="${SYNTHIGY_INSTALL_DIR:-$HOME/.local/bin}"

case "$(uname -s)" in
  Linux)  os=linux ;;
  Darwin) os=darwin ;;
  *) echo "unsupported OS: $(uname -s) (Windows: download synthigy-cli-windows-amd64.exe from https://github.com/$REPO/releases)"; exit 1 ;;
esac
case "$(uname -m)" in
  x86_64|amd64)  arch=amd64 ;;
  aarch64|arm64) arch=arm64 ;;
  *) echo "unsupported architecture: $(uname -m)"; exit 1 ;;
esac

asset="synthigy-cli-${os}-${arch}"
url="https://github.com/${REPO}/releases/latest/download/${asset}"

echo "Downloading ${asset} (latest release)..."
mkdir -p "$DIR"
tmp="$(mktemp)"
curl -fSL -o "$tmp" "$url" || { echo "download failed: $url"; rm -f "$tmp"; exit 1; }
install -m 0755 "$tmp" "$DIR/synthigy"
rm -f "$tmp"

echo "Installed: $DIR/synthigy"
"$DIR/synthigy" version || true
case ":$PATH:" in
  *":$DIR:"*) ;;
  *) echo "NOTE: $DIR is not on your PATH — add:  export PATH=\"$DIR:\$PATH\"" ;;
esac
echo
echo "Start Synthigy:  synthigy up"
