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

# Synthigy portal installer for Windows (PowerShell 5.1+ / pwsh):
#   irm https://raw.githubusercontent.com/synthigy/synthigy/main/install.ps1 | iex
# Pin a version:
#   $env:SYNTHIGY_VERSION = "v0.1.0"; irm .../install.ps1 | iex
# Installs `synthigy` to ~\.synthigy\bin (override: $env:SYNTHIGY_INSTALL_DIR)
# and adds it to the user PATH — both idempotent: re-running updates the
# binary and never duplicates PATH entries.
# Corporate networks: Invoke-WebRequest uses the Windows proxy + cert store —
# if your firewall's CA is deployed via GPO this just works. For the synthigy
# command itself set $env:SYNTHIGY_CA_BUNDLE = "C:\path\corp-ca.pem".
$ErrorActionPreference = "Stop"

# The portal BINARY ships from its own repo, always — never the engine
# channel (SYNTHIGY_RELEASES_REPO picks db/obs jars for `synthigy up`, and
# a nightly/custom engine mirror carries no portal binaries at all; see
# runtime.PortalReleasesRepo / tooling/portal/CLAUDE.md).
$repo = if ($env:SYNTHIGY_PORTAL_RELEASES_REPO) { $env:SYNTHIGY_PORTAL_RELEASES_REPO } else { "synthigy/tooling" }
$dir  = if ($env:SYNTHIGY_INSTALL_DIR) { $env:SYNTHIGY_INSTALL_DIR } else { Join-Path $HOME ".synthigy\bin" }
$ver  = if ($env:SYNTHIGY_VERSION) { $env:SYNTHIGY_VERSION } else { "latest" }

$arch = switch ($env:PROCESSOR_ARCHITECTURE) {
  "AMD64" { "amd64" }
  "ARM64" { "arm64" }
  default { throw "unsupported architecture: $env:PROCESSOR_ARCHITECTURE" }
}
$asset = "synthigy-portal-windows-$arch.exe"

if ($ver -eq "latest") {
  # Newest release that actually carries this platform's binary — releases
  # can be jars-only (portal binaries are attached separately). Newest-first.
  $releases = Invoke-RestMethod "https://api.github.com/repos/$repo/releases?per_page=30"
  $hit = $releases | Where-Object { $_.assets.name -contains $asset } | Select-Object -First 1
  if (-not $hit) { throw "no published release carries $asset yet" }
  $url = ($hit.assets | Where-Object name -eq $asset).browser_download_url
  $ver = $hit.tag_name
} else {
  $url = "https://github.com/$repo/releases/download/$ver/$asset"
}

Write-Host "Downloading $asset ($ver)..."
New-Item -ItemType Directory -Force -Path $dir | Out-Null
$exe = Join-Path $dir "synthigy.exe"
$tmp = "$exe.part"
Invoke-WebRequest -Uri $url -OutFile $tmp
$sums = (Invoke-WebRequest -Uri ($url.Substring(0, $url.LastIndexOf("/")) + "/sha256sums-portal.txt")).Content
$want = ($sums -split "`n" | ForEach-Object { $f = $_.Trim() -split "\s+"; if ($f.Count -eq 2 -and $f[1] -eq $asset) { $f[0] } } | Select-Object -First 1)
$got = (Get-FileHash -Algorithm SHA256 $tmp).Hash.ToLower()
if (-not $want -or $got -ne $want.ToLower()) {
  Remove-Item $tmp -Force
  throw "checksum mismatch for $asset (want $want, got $got) — not installed"
}
Move-Item -Force $tmp $exe
Write-Host "Installed: $exe"
& $exe version

# PATH setup — idempotent: only appends when the exact dir is absent.
$userPath = [Environment]::GetEnvironmentVariable("Path", "User")
if (($userPath -split ";") -notcontains $dir) {
  [Environment]::SetEnvironmentVariable("Path", "$userPath;$dir", "User")
  Write-Host "Added $dir to your user PATH (open a new terminal to use it)"
} else {
  Write-Host "PATH already contains $dir"
}
Write-Host ""
Write-Host "Start Synthigy:  synthigy up"
