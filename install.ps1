# Synthigy portal installer for Windows (PowerShell 5.1+ / pwsh):
#   irm https://raw.githubusercontent.com/synthigy/synthigy/main/install.ps1 | iex
# Pin a version:
#   $env:SYNTHIGY_VERSION = "v0.1.0"; irm .../install.ps1 | iex
# Installs `synthigy` to ~\.synthigy\bin (override: $env:SYNTHIGY_INSTALL_DIR)
# and adds it to the user PATH — both idempotent: re-running updates the
# binary and never duplicates PATH entries.
$ErrorActionPreference = "Stop"

$repo = "synthigy/synthigy"
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
Invoke-WebRequest -Uri $url -OutFile $exe
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
