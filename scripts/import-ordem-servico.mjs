import { mkdir, writeFile } from 'node:fs/promises'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const workspaceRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..')
const importXmlDirectory = path.join(workspaceRoot, 'importXML')
const defaultBaseUrl = 'http://localhost:3001'
const defaultFileName = 'OrdemServico.xml'
const rawArgs = process.argv.slice(2)

let requestedBaseUrl = defaultBaseUrl
let requestedFileName = defaultFileName

for (let index = 0; index < rawArgs.length; index += 1) {
  const currentArg = String(rawArgs[index] ?? '').trim()

  if (!currentArg) {
    continue
  }

  if (currentArg === '--base-url') {
    requestedBaseUrl = String(rawArgs[index + 1] ?? defaultBaseUrl).trim() || defaultBaseUrl
    index += 1
    continue
  }

  if (currentArg.startsWith('--base-url=')) {
    requestedBaseUrl = currentArg.slice('--base-url='.length).trim() || defaultBaseUrl
    continue
  }

  if (currentArg === '--file') {
    requestedFileName = String(rawArgs[index + 1] ?? defaultFileName).trim() || defaultFileName
    index += 1
    continue
  }

  if (currentArg.startsWith('--file=')) {
    requestedFileName = currentArg.slice('--file='.length).trim() || defaultFileName
    continue
  }

  requestedFileName = currentArg
}

const baseUrl = requestedBaseUrl
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
