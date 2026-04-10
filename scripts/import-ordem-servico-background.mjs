import { appendFile, mkdir, writeFile } from 'node:fs/promises'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const workspaceRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..')
const importXmlDirectory = path.join(workspaceRoot, 'importXML')
const baseUrl = process.env.API_BASE_URL ?? 'http://localhost:3001'
const defaultFileName = process.env.ORDEM_SERVICO_XML_FILE ?? 'OrdemServico.xml'
const requestedFileName = String(process.argv[2] ?? defaultFileName).trim()
const fileName = path.basename(requestedFileName)
const reportPath = process.env.ORDEM_SERVICO_IMPORT_REPORT_PATH
  ?? path.join(importXmlDirectory, 'ordem_servico_import_summary.json')
const logPath = process.env.ORDEM_SERVICO_IMPORT_LOG_PATH
  ?? path.join(importXmlDirectory, 'ordem_servico_import.log')
const heartbeatSeconds = Math.max(Number(process.env.ORDEM_SERVICO_IMPORT_HEARTBEAT_SECONDS ?? 15) || 15, 5)
const endpoint = `${baseUrl}/api/ordem-servico/import-xml`
const startedAt = new Date().toISOString()

await mkdir(path.dirname(reportPath), { recursive: true })
await mkdir(path.dirname(logPath), { recursive: true })

const appendLog = async (message) => {
  const timestamp = new Date().toISOString()
  await appendFile(logPath, `[${timestamp}] ${message}\n`, 'utf8')
}

await appendLog(`Inicio do import de ${fileName} para ${endpoint}`)

const heartbeatId = setInterval(() => {
  void appendLog(`Importacao de ${fileName} em andamento; aguardando resposta do endpoint.`)
}, heartbeatSeconds * 1000)

let summary

try {
  const response = await fetch(endpoint, {
    method: 'POST',
    headers: {
      Accept: 'application/json',
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ fileName }),
  })

  const responseBody = await response.json().catch(() => null)
  const finishedAt = new Date().toISOString()

  summary = {
    endpoint,
    fileName,
    startedAt,
    finishedAt,
    ok: response.ok,
    status: response.status,
    statusText: response.statusText,
    response: responseBody,
    logPath,
  }

  await writeFile(reportPath, `${JSON.stringify(summary, null, 2)}\n`, 'utf8')

  if (!response.ok) {
    const message = responseBody && typeof responseBody.message === 'string'
      ? responseBody.message
      : `HTTP ${response.status} em ${endpoint}`
    await appendLog(`Importacao finalizada com erro: ${message}`)
    console.error(JSON.stringify({ ok: false, fileName, status: response.status, message, reportPath, logPath }, null, 2))
    process.exit(1)
  }

  await appendLog(`Importacao concluida com sucesso. Processados=${responseBody?.processed ?? 'n/d'} Skipped=${responseBody?.skipped ?? 'n/d'}`)
  console.log(JSON.stringify({
    ok: true,
    fileName,
    total: responseBody?.total ?? null,
    processed: responseBody?.processed ?? null,
    inserted: responseBody?.inserted ?? null,
    updated: responseBody?.updated ?? null,
    skipped: responseBody?.skipped ?? null,
    reportPath,
    logPath,
  }, null, 2))
} catch (error) {
  const finishedAt = new Date().toISOString()
  summary = {
    endpoint,
    fileName,
    startedAt,
    finishedAt,
    ok: false,
    status: null,
    statusText: '',
    response: null,
    errorMessage: error instanceof Error ? error.message : String(error),
    logPath,
  }
  await writeFile(reportPath, `${JSON.stringify(summary, null, 2)}\n`, 'utf8')
  await appendLog(`Importacao interrompida com excecao: ${summary.errorMessage}`)
  console.error(JSON.stringify({ ok: false, fileName, message: summary.errorMessage, reportPath, logPath }, null, 2))
  process.exit(1)
} finally {
  clearInterval(heartbeatId)
}
