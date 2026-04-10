import { mkdir, writeFile } from 'node:fs/promises'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const workspaceRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..')
const importXmlDirectory = path.join(workspaceRoot, 'importXML')
const baseUrl = process.env.API_BASE_URL ?? 'http://localhost:3001'
const defaultFileName = process.env.ORDEM_SERVICO_XML_FILE ?? 'OrdemServico.xml'
const requestedFileName = String(process.argv[2] ?? defaultFileName).trim()
const reportPath = process.env.ORDEM_SERVICO_IMPORT_REPORT_PATH
  ?? path.join(importXmlDirectory, 'ordem_servico_import_summary.json')
const endpoint = `${baseUrl}/api/ordem-servico/import-xml`
const startedAt = new Date().toISOString()

const payload = {
  fileName: path.basename(requestedFileName),
}

const response = await fetch(endpoint, {
  method: 'POST',
  headers: {
    Accept: 'application/json',
    'Content-Type': 'application/json',
  },
  body: JSON.stringify(payload),
})

const responseBody = await response.json().catch(() => null)
const finishedAt = new Date().toISOString()

const summary = {
  endpoint,
  fileName: payload.fileName,
  startedAt,
  finishedAt,
  ok: response.ok,
  status: response.status,
  statusText: response.statusText,
  response: responseBody,
}

await mkdir(path.dirname(reportPath), { recursive: true })
await writeFile(reportPath, `${JSON.stringify(summary, null, 2)}\n`, 'utf8')

if (!response.ok) {
  const message = responseBody && typeof responseBody.message === 'string'
    ? responseBody.message
    : `HTTP ${response.status} em ${endpoint}`

  console.error(JSON.stringify({
    ok: false,
    fileName: payload.fileName,
    status: response.status,
    message,
    reportPath,
  }, null, 2))

  process.exit(1)
}

console.log(JSON.stringify({
  ok: true,
  fileName: payload.fileName,
  total: responseBody?.total ?? null,
  processed: responseBody?.processed ?? null,
  inserted: responseBody?.inserted ?? null,
  updated: responseBody?.updated ?? null,
  skipped: responseBody?.skipped ?? null,
  reportPath,
}, null, 2))
