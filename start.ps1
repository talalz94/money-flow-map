<#
  Start the Money Flow Map (one command):   .\start.ps1
    -NoDashboard   API only (headless), no web page
    -NoBrowser     do not open the dashboard in the browser
    -Port 8765     another port
  Double-click start.bat for the same. Stop with Ctrl+C in this window.
#>
param([switch]$NoDashboard, [switch]$NoBrowser, [int]$Port = 0)
$ErrorActionPreference = "Stop"
Set-Location -Path $PSScriptRoot

function Find-Python {
    foreach ($cand in @(@("py", "-3.13"), @("py", "-3.12"), @("py", "-3.11"), @("python"))) {
        try {
            $exe = $cand[0]
            $rest = @()
            if ($cand.Count -gt 1) { $rest = $cand[1..($cand.Count - 1)] }
            $v = & $exe @rest -c "import sys; print('%d.%d' % sys.version_info[:2])" 2>$null
            if ($LASTEXITCODE -eq 0 -and $v) {
                $parts = $v.Trim().Split(".")
                if ([int]$parts[0] -eq 3 -and [int]$parts[1] -ge 11) { return ,$cand }
            }
        } catch { }
    }
    return $null
}

$py = Find-Python
if ($null -eq $py) {
    Write-Host "Python 3.11 or newer was not found. Install it from https://www.python.org/downloads/ and try again." -ForegroundColor Red
    exit 1
}
$exe = $py[0]
$pre = @()
if ($py.Count -gt 1) { $pre = $py[1..($py.Count - 1)] }

# runtime dependencies: install only what is missing (into the user's site-packages)
& $exe @pre -c "import aiohttp" 2>$null
if ($LASTEXITCODE -ne 0) {
    Write-Host "Installing the one runtime dependency (aiohttp) for this user..."
    & $exe @pre -m pip install --user "aiohttp>=3.9" "tzdata>=2024.1"
}
& $exe @pre -c "import zoneinfo; zoneinfo.ZoneInfo('America/New_York')" 2>$null
if ($LASTEXITCODE -ne 0) { & $exe @pre -m pip install --user "tzdata>=2024.1" }

if (-not (Test-Path ".env") -and -not (Test-Path "moneyflow.local.toml")) {
    Write-Host "Note: no .env or moneyflow.local.toml found. Public sources (Binance, Coinbase, CoinGecko, DefiLlama, Cboe, the Fed, RSS) work without keys;" -ForegroundColor Yellow
    Write-Host "      copy .env.example to .env and add Alpaca / Finnhub / FRED keys for bonds, the DXY and yields." -ForegroundColor Yellow
}

$args2 = @("-m", "moneyflow", "run")
if ($NoDashboard) { $args2 += "--no-dashboard" }
elseif (-not $NoBrowser) { $args2 += "--open" }
if ($Port -gt 0) { $args2 += @("--port", "$Port") }
& $exe @pre @args2
