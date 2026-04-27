$ErrorActionPreference = 'Stop'

$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path

Push-Location $projectRoot
try {
  npm run homol:publish
  if ($LASTEXITCODE -ne 0) {
    throw 'Falha ao publicar a homologacao.'
  }
} finally {
  Pop-Location
}

Write-Host 'Publicacao de homologacao concluida.'