$taskName = 'TEG Financ Homologacao'

if (Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue) {
  Unregister-ScheduledTask -TaskName $taskName -Confirm:$false
  Write-Host "Tarefa removida: $taskName"
} else {
  Write-Host "Nenhuma tarefa encontrada com o nome $taskName"
}