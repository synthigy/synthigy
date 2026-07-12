#!/bin/sh
# Synthigy portal installer:
#   curl -fsSL https://raw.githubusercontent.com/synthigy/synthigy/main/install.sh | sh
# Pin a version:  curl ... | sh -s -- v0.1.0     (default: newest with binaries)
# Installs the `synthigy` command to ~/.synthigy/bin (override:
# SYNTHIGY_INSTALL_DIR) and adds it to PATH in your shell profile — both
# idempotent: re-running updates the binary and never duplicates PATH lines.
set -eu

REPO="synthigy/synthigy"
DIR="${SYNTHIGY_INSTALL_DIR:-$HOME/.synthigy/bin}"
VERSION="${1:-latest}"

case "$(uname -s)" in
  Linux)  os=linux ;;
  Darwin) os=darwin ;;
  *) echo "unsupported OS: $(uname -s) (Windows: irm https://raw.githubusercontent.com/$REPO/main/install.ps1 | iex)"; exit 1 ;;
esac
case "$(uname -m)" in
  x86_64|amd64)  arch=amd64 ;;
  aarch64|arm64) arch=arm64 ;;
  *) echo "unsupported architecture: $(uname -m)"; exit 1 ;;
esac

asset="synthigy-portal-${os}-${arch}"
if [ "$VERSION" = "latest" ]; then
  # Newest release that actually carries this platform's binary — releases
  # can be jars-only (portal binaries are attached in a separate step), so
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

# PATH setup — idempotent: guarded by a marker grep, one line, once ever.
PATH_LINE="export PATH=\"$DIR:\$PATH\"  # synthigy portal"
case "${SHELL:-}" in
  */zsh)  rc="$HOME/.zshrc" ;;
  */bash) rc="$HOME/.bashrc" ;;
  *)      rc="$HOME/.profile" ;;
esac
case ":$PATH:" in
  *":$DIR:"*) ;; # already live in this session
  *)
    if grep -qs "# synthigy portal" "$rc"; then
      echo "PATH entry already in $rc (open a new shell to use it)"
    else
      printf '\n%s\n' "$PATH_LINE" >> "$rc"
      echo "Added $DIR to PATH in $rc (open a new shell, or: export PATH=\"$DIR:\$PATH\")"
    fi
    ;;
esac
echo
echo "Start Synthigy:  synthigy up"
