param(
  [switch]$Quiet
)

$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$publishedStartScript = Join-Path $projectRoot '.homolog\control\iniciar-homologacao-publicado.ps1'

if (-not (Test-Path $publishedStartScript)) {
  throw 'Homologacao ainda nao publicada. Execute uma publicacao explicita com "npm run homol:publish" antes de iniciar.'
}

$arguments = @('-ExecutionPolicy', 'Bypass', '-File', $publishedStartScript)
if ($Quiet) {
  $arguments += '-Quiet'
}

& powershell @arguments
exit $LASTEXITCODE