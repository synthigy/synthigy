#!/bin/sh
# Synthigy CLI installer:
#   curl -fsSL https://raw.githubusercontent.com/synthigy/synthigy/main/install.sh | sh
# Downloads the synthigy CLI binary for this platform from the latest release,
# verifies nothing is world-writable weird, installs to ~/.local/bin (or
# SYNTHIGY_INSTALL_DIR). Then: `synthigy up` does the rest.
set -eu

# Pin a version:  curl ... | sh -s -- v0.1.0     (default: latest)
REPO="synthigy/synthigy"
DIR="${SYNTHIGY_INSTALL_DIR:-$HOME/.local/bin}"
VERSION="${1:-latest}"

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
if [ "$VERSION" = "latest" ]; then
  # Newest release that actually carries this platform's binary — releases
  # can be jars-only (CLI binaries are attached in a separate step), so
  # `releases/latest` alone is not trustworthy. The API lists newest-first;
  # the first matching download URL is the one we want.
  url=$(curl -fsSL "https://api.github.com/repos/${REPO}/releases?per_page=30" \
        | grep -o "\"browser_download_url\":[^\"]*\"[^\"]*/${asset}\"" \
        | head -1 | sed 's/.*"\(https[^"]*\)"$/\1/')
  [ -n "$url" ] || { echo "no published release carries ${asset} yet"; exit 1; }
else
  url="https://github.com/${REPO}/releases/download/${VERSION}/${asset}"
fi

echo "Downloading ${asset} (${url##*/download/})..."
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
