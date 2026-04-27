param(
  [switch]$Quiet
)

$controlRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$homologRoot = Split-Path -Parent $controlRoot
$pidRoot = Join-Path $homologRoot 'pids'
$apiPidPath = Join-Path $pidRoot 'api.pid'
$webPidPath = Join-Path $pidRoot 'web.pid'

function Write-Info {
  param([string]$Message)

  if (-not $Quiet) {
    Write-Host $Message
  }
}

function Stop-ByPidFile {
  param([string]$PidPath)

  if (-not (Test-Path $PidPath)) {
    return
  }

  $rawPid = Get-Content $PidPath -ErrorAction SilentlyContinue | Select-Object -First 1
  if ($rawPid -match '^\d+$') {
    $process = Get-Process -Id ([int]$rawPid) -ErrorAction SilentlyContinue
    if ($process) {
      Stop-Process -Id $process.Id -Force -ErrorAction SilentlyContinue
    }
  }

  Remove-Item $PidPath -Force -ErrorAction SilentlyContinue
}

function Stop-ByPort {
  param([int]$Port)

  $connections = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue
  if (-not $connections) {
    return
  }

  $connections |
    Select-Object -ExpandProperty OwningProcess -Unique |
    ForEach-Object {
      Stop-Process -Id $_ -Force -ErrorAction SilentlyContinue
    }
}

Stop-ByPidFile -PidPath $apiPidPath
Stop-ByPidFile -PidPath $webPidPath
Stop-ByPort -Port 3002
Stop-ByPort -Port 4173

Write-Info 'Homologacao parada.'