#!/bin/sh
#   Synthigy — model-driven IAM and data platform
#   Copyright (C) 2026 Robert Geršak
#
#   This program is free software: you can redistribute it and/or modify
#   it under the terms of the GNU Affero General Public License as
#   published by the Free Software Foundation, either version 3 of the
#   License, or (at your option) any later version.
#
#   This program is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU Affero General Public License for more details.
#
#   You should have received a copy of the GNU Affero General Public
#   License along with this program.  If not, see
#   <https://www.gnu.org/licenses/>.
#
#   Synthigy is dual-licensed. If the AGPL does not suit you — embedding
#   in a proprietary product, or offering it as a service without
#   releasing your source under section 13 — a commercial license is
#   available: r.gersak@gmail.com  See COMMERCIAL.md.

# Synthigy portal installer:
#   curl -fsSL https://raw.githubusercontent.com/synthigy/synthigy/main/install.sh | sh
# Pin a version:  curl ... | sh -s -- v0.1.0     (default: newest with binaries)
# Installs the `synthigy` command to ~/.synthigy/bin (override:
# SYNTHIGY_INSTALL_DIR) and adds it to PATH in your shell profile — both
# idempotent: re-running updates the binary and never duplicates PATH lines.
# The binary is sha256-checked against the release's sha256sums-portal.txt.
# Corporate networks: curl honors https_proxy/HTTPS_PROXY env; behind a
# TLS-intercepting firewall set CURL_CA_BUNDLE=/path/corp-ca.pem (and later
# SYNTHIGY_CA_BUNDLE for the synthigy command itself).
set -eu

# The portal BINARY ships from its own repo, always — never the engine
# channel (SYNTHIGY_RELEASES_REPO picks db/obs jars for `synthigy up`, and
# a nightly/custom engine mirror carries no portal binaries at all; see
# runtime.PortalReleasesRepo / tooling/portal/CLAUDE.md).
REPO="${SYNTHIGY_PORTAL_RELEASES_REPO:-synthigy/tooling}"
DIR="${SYNTHIGY_INSTALL_DIR:-$HOME/.synthigy/bin}"
VERSION="${1:-latest}"

case "$(uname -s)" in
  Linux)  os=linux ;;
  Darwin) os=darwin ;;
  *) echo "unsupported OS: $(uname -s) (Windows: irm https://raw.githubusercontent.com/synthigy/synthigy/main/install.ps1 | iex)"; exit 1 ;;
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
want=$(curl -fsSL "${url%/*}/sha256sums-portal.txt" | awk -v a="$asset" '$2 == a { print $1 }')
if command -v sha256sum >/dev/null 2>&1; then got=$(sha256sum "$tmp" | awk '{ print $1 }')
else got=$(shasum -a 256 "$tmp" | awk '{ print $1 }'); fi
[ -n "$want" ] && [ "$got" = "$want" ] || { echo "checksum mismatch for ${asset} (want ${want:-none}, got ${got}) — not installed"; rm -f "$tmp"; exit 1; }
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
