<#
 Khoi dong toan bo he thong cho buoi demo: Docker (Cassandra + Redis) -> tao bang -> 2 server (A:8001, B:8002) -> mo trinh duyet.
 Chay (tu thu muc livechat):   powershell -ExecutionPolicy Bypass -File scripts\start_demo.ps1
 Neu venv khong o C:\venvs\livechat:   ... -File scripts\start_demo.ps1 -Venv "D:\duong\dan\venv"
#>
param(
  [string]$Venv = "C:\venvs\livechat",
  [switch]$NoBrowser
)
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location -LiteralPath $root
$py = Join-Path $Venv "Scripts\python.exe"
if (-not (Test-Path -LiteralPath $py)) { throw "Khong thay Python cua venv tai $py (dung tham so -Venv)" }
$env:PYTHONUTF8 = "1"

function Get-Health([string]$name) {
  $h = docker inspect --format "{{.State.Health.Status}}" $name 2>$null
  if ($LASTEXITCODE -ne 0) { return "none" }
  return ($h | Out-String).Trim()
}

Write-Host "[1/4] Bat Docker (Cassandra + Redis)..."
docker compose up -d | Out-Null

Write-Host "[2/4] Doi Cassandra va Redis san sang (toi da 4 phut)..."
$deadline = (Get-Date).AddMinutes(4)
while ($true) {
  $c = Get-Health "livechat-cassandra"; $r = Get-Health "livechat-redis"
  Write-Host ("      cassandra={0}  redis={1}" -f $c, $r)
  if ($c -eq "healthy" -and $r -eq "healthy") { break }
  if ((Get-Date) -gt $deadline) { throw "Het thoi gian cho. Kiem tra: docker compose ps" }
  Start-Sleep -Seconds 5
}

Write-Host "[3/4] Tao bang (an toan khi chay lai)..."
& $py scripts\init_db.py | Out-Null
if ($LASTEXITCODE -ne 0) { throw "init_db.py loi" }

Write-Host "[4/4] Bat 2 server (moi server 1 cua so)..."
foreach ($s in @(@("A", 8001), @("B", 8002))) {
  $cmd = "`$env:SERVER_ID='$($s[0])'; `$env:PYTHONUTF8='1'; Set-Location -LiteralPath '$root'; & '$py' -m uvicorn app.main:app --port $($s[1])"
  Start-Process powershell -ArgumentList "-NoExit", "-Command", $cmd | Out-Null
}
foreach ($p in 8001, 8002) {
  $ok = $false
  for ($i = 0; $i -lt 40; $i++) {
    try { Invoke-RestMethod "http://127.0.0.1:$p/api/health" -TimeoutSec 2 | Out-Null; $ok = $true; break } catch { Start-Sleep -Milliseconds 750 }
  }
  if (-not $ok) { throw "Server cong $p khong len. Xem cua so uvicorn tuong ung." }
  Write-Host "      server cong $p san sang"
}

if (-not $NoBrowser) {
  Start-Process "http://127.0.0.1:8001/?customer=khach_demo"
  Start-Process "http://127.0.0.1:8002/web/agent.html?agent=nv1"
}
Write-Host ""
Write-Host "SAN SANG.  Khach: http://127.0.0.1:8001/   Nhan vien: http://127.0.0.1:8002/web/agent.html"
Write-Host "Tat: dong 2 cua so uvicorn, roi 'docker compose stop'."