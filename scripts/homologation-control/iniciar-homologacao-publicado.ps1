param(
  [switch]$Quiet
)

$ErrorActionPreference = 'Stop'

$controlRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$homologRoot = Split-Path -Parent $controlRoot
$runtimeRoot = Join-Path $homologRoot 'runtime'
$logRoot = Join-Path $homologRoot 'logs'
$pidRoot = Join-Path $homologRoot 'pids'
$apiPidPath = Join-Path $pidRoot 'api.pid'
$webPidPath = Join-Path $pidRoot 'web.pid'
$nodeCommand = (Get-Command node -ErrorAction Stop).Source

function Write-Info {
  param([string]$Message)

  if (-not $Quiet) {
    Write-Host $Message
  }
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

if (-not (Test-Path (Join-Path $runtimeRoot 'server.js'))) {
  throw 'Pacote publicado de homologacao nao encontrado em .homolog\\runtime.'
}

& (Join-Path $controlRoot 'parar-homologacao-publicado.ps1') -Quiet

New-Item -ItemType Directory -Force -Path $logRoot | Out-Null
New-Item -ItemType Directory -Force -Path $pidRoot | Out-Null

$apiOutLog = Join-Path $logRoot 'api-out.log'
$apiErrLog = Join-Path $logRoot 'api-err.log'
$webOutLog = Join-Path $logRoot 'web-out.log'
$webErrLog = Join-Path $logRoot 'web-err.log'

$apiCommand = @"
Set-Location '$runtimeRoot'
`$env:NODE_ENV = 'production'
`$env:PGHOST = 'localhost'
`$env:PGPORT = '5432'
`$env:PGUSER = 'postgres'
`$env:PGPASSWORD = '12345'
`$env:PGDATABASE = 'teg_financ_homol'
`$env:PORT = '3002'
& '$nodeCommand' '$runtimeRoot\server.js'
"@

$webCommand = @"
Set-Location '$runtimeRoot'
`$env:HOMOL_RUNTIME_ROOT = '$runtimeRoot'
`$env:HOMOL_API_TARGET = 'http://127.0.0.1:3002'
`$env:HOMOL_WEB_HOST = '10.36.144.147'
`$env:HOMOL_WEB_PORT = '4173'
& '$nodeCommand' '$runtimeRoot\scripts\homologation-web-server.mjs'
"@

$apiProcess = Start-Process -FilePath 'powershell.exe' -ArgumentList @('-NoProfile', '-ExecutionPolicy', 'Bypass', '-Command', $apiCommand) -WindowStyle Hidden -RedirectStandardOutput $apiOutLog -RedirectStandardError $apiErrLog -PassThru
$webProcess = Start-Process -FilePath 'powershell.exe' -ArgumentList @('-NoProfile', '-ExecutionPolicy', 'Bypass', '-Command', $webCommand) -WindowStyle Hidden -RedirectStandardOutput $webOutLog -RedirectStandardError $webErrLog -PassThru

Set-Content -Path $apiPidPath -Value $apiProcess.Id -Encoding ascii
Set-Content -Path $webPidPath -Value $webProcess.Id -Encoding ascii

Start-Sleep -Seconds 2

if ($apiProcess.HasExited) {
  $apiError = if (Test-Path $apiErrLog) { Get-Content $apiErrLog -Tail 20 | Out-String } else { '' }
  throw "A API de homologacao encerrou logo apos a inicializacao.`n$apiError"
}

if ($webProcess.HasExited) {
  $webError = if (Test-Path $webErrLog) { Get-Content $webErrLog -Tail 20 | Out-String } else { '' }
  throw "O servidor web de homologacao encerrou logo apos a inicializacao.`n$webError"
}

Write-Info 'Homologacao iniciada com sucesso.'
Write-Info 'Web: http://10.36.144.147:4173'
Write-Info 'API: http://127.0.0.1:3002'