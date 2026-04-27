param(
  [switch]$Quiet
)

$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$publishedStopScript = Join-Path $projectRoot '.homolog\control\parar-homologacao-publicado.ps1'

if (-not (Test-Path $publishedStopScript)) {
  if (-not $Quiet) {
    Write-Host 'Homologacao ainda nao publicada.'
  }
  exit 0
}

$arguments = @('-ExecutionPolicy', 'Bypass', '-File', $publishedStopScript)
if ($Quiet) {
  $arguments += '-Quiet'
}

& powershell @arguments
exit $LASTEXITCODE