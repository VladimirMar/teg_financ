import { mkdir, writeFile } from 'node:fs/promises'
import { dirname } from 'node:path'

const baseUrl = process.env.API_BASE_URL ?? 'http://localhost:3001'
const reportPath = process.env.INVALID_FIXTURE_REPORT_PATH ?? ''
const availableSuites = new Set(['all', 'condutor', 'credenciada', 'veiculo'])
const requestedSuite = (process.argv[2] ?? process.env.INVALID_FIXTURE_SUITE ?? 'all').trim().toLowerCase()

const report = {
  requestedSuite,
  status: 'running',
  startedAt: new Date().toISOString(),
  finishedAt: null,
  executedSuites: [],
  failureMessage: '',
}

const fixtureChecks = [
  {
    key: 'condutor',
    importPath: '/api/condutor/import-xml',
    rejectionPath: '/api/condutor/import-rejections',
    fileName: 'Condutor-invalid.xml',
    expectedProcessed: 0,
    expectedSkipped: 3,
    expectedPayloadChecks: [
      { index: 1, pattern: /nome do condutor invalido no XML/i },
      { index: 2, pattern: /codigo invalido no XML/i },
      { index: 3, pattern: /CPF invalido no XML/i },
    ],
    expectedRejectionPatterns: [
      /nome do condutor invalido no XML/i,
      /codigo invalido no XML/i,
      /CPF invalido no XML/i,
    ],
  },
  {
    key: 'credenciada',
    importPath: '/api/credenciada/import-xml',
    rejectionPath: '/api/credenciada/import-rejections',
    fileName: 'Credenciados-invalid.xml',
    expectedProcessed: 1,
    expectedSkipped: 2,
    expectedPayloadChecks: [
      { index: 2, pattern: /codigo invalido no XML/i },
      { index: 3, pattern: /email invalido no XML/i },
    ],
    expectedRejectionPatterns: [
      /codigo invalido no XML/i,
      /email invalido no XML/i,
    ],
  },
  {
    key: 'veiculo',
    importPath: '/api/veiculo/import-xml',
    rejectionPath: '/api/veiculo/import-rejections',
    fileName: 'Veiculo-invalid.xml',
    expectedProcessed: 1,
    expectedSkipped: 2,
    expectedPayloadChecks: [
      { index: 2, pattern: /codigo invalido no XML/i },
      { index: 3, pattern: /(tipo de bancada invalido no XML|tipo de veiculo invalido no XML)/i },
    ],
    expectedRejectionPatterns: [
      /codigo invalido no XML/i,
      /(tipo de bancada invalido no XML|tipo de veiculo invalido no XML)/i,
    ],
  },
]

const requestJson = async (path, options = {}) => {
  const response = await fetch(`${baseUrl}${path}`, {
    ...options,
    headers: {
      Accept: 'application/json',
      ...(options.body ? { 'Content-Type': 'application/json' } : {}),
      ...(options.headers ?? {}),
    },
  })

  const payload = await response.json().catch(() => null)

  if (!response.ok) {
    const message = payload && typeof payload.message === 'string'
      ? payload.message
      : `HTTP ${response.status} em ${path}`

    throw new Error(message)
  }

  return payload
}

const assert = (condition, message) => {
  if (!condition) {
    throw new Error(message)
  }
}

const logStep = (message) => {
  console.log(message)
}

const finalizeReport = async (status, failureMessage = '') => {
  report.status = status
  report.failureMessage = failureMessage
  report.finishedAt = new Date().toISOString()

  if (!reportPath) {
    return
  }

  await mkdir(dirname(reportPath), { recursive: true })
  await writeFile(reportPath, `${JSON.stringify(report, null, 2)}\n`, 'utf8')
}

const runFixtureCheck = async (fixture) => {
  logStep(`Verificando fixture invalido de ${fixture.key}: ${fixture.fileName}`)

  const suiteReport = {
    suite: fixture.key,
    fileName: fixture.fileName,
    status: 'running',
    startedAt: new Date().toISOString(),
    finishedAt: null,
    failureMessage: '',
    importSummary: null,
    rejectionReasons: [],
  }

  report.executedSuites.push(suiteReport)

  try {
    const importResult = await requestJson(fixture.importPath, {
      method: 'POST',
      body: JSON.stringify({ fileName: fixture.fileName }),
    })

    suiteReport.importSummary = {
      total: importResult.total,
      processed: importResult.processed,
      inserted: importResult.inserted,
      updated: importResult.updated,
      skipped: importResult.skipped,
      skippedRecords: Array.isArray(importResult.skippedRecords)
        ? importResult.skippedRecords.map((item) => ({
            index: item.index,
            codigoXml: item.codigoXml,
            message: item.message,
          }))
        : [],
    }

    assert(importResult.processed === fixture.expectedProcessed, `${fixture.fileName} retornou ${importResult.processed} processado(s); esperado: ${fixture.expectedProcessed}.`)
    assert(importResult.skipped === fixture.expectedSkipped, `${fixture.fileName} retornou ${importResult.skipped} recusado(s); esperado: ${fixture.expectedSkipped}.`)
    assert(Array.isArray(importResult.skippedRecords), `${fixture.fileName} nao retornou skippedRecords.`)
    assert(importResult.skippedRecords.length === fixture.expectedSkipped, `${fixture.fileName} nao retornou ${fixture.expectedSkipped} registros recusados no payload.`)

    for (const expectedCheck of fixture.expectedPayloadChecks) {
      const matchedRecord = importResult.skippedRecords.find((item) => item.index === expectedCheck.index && expectedCheck.pattern.test(String(item.message ?? '')))
      assert(Boolean(matchedRecord), `${fixture.fileName} nao retornou a recusa esperada no payload para a linha ${expectedCheck.index}.`)
    }

    const rejectionResponse = await requestJson(`${fixture.rejectionPath}?page=1&pageSize=20&search=${encodeURIComponent(fixture.fileName)}`)
    const rejectionReasons = (rejectionResponse.items ?? [])
      .filter((item) => item.arquivo_xml === fixture.fileName)
      .map((item) => String(item.motivo_recusa ?? ''))

    suiteReport.rejectionReasons = rejectionReasons

    for (const pattern of fixture.expectedRejectionPatterns) {
      assert(rejectionReasons.some((reason) => pattern.test(reason)), `${fixture.fileName} nao retornou a recusa esperada no painel de recusas.`)
    }

    suiteReport.status = 'passed'
    suiteReport.finishedAt = new Date().toISOString()
    logStep(`${fixture.fileName}: ${importResult.processed} processado(s), ${importResult.skipped} recusado(s)`)
  } catch (error) {
    suiteReport.status = 'failed'
    suiteReport.finishedAt = new Date().toISOString()
    suiteReport.failureMessage = error instanceof Error ? error.message : `Falha ao verificar ${fixture.fileName}.`
    throw error
  }
}

const main = async () => {
  if (!availableSuites.has(requestedSuite)) {
    throw new Error(`Suite invalida: ${requestedSuite}. Suites disponiveis: ${Array.from(availableSuites).join(', ')}.`)
  }

  const selectedFixtures = requestedSuite === 'all'
    ? fixtureChecks
    : fixtureChecks.filter((fixture) => fixture.key === requestedSuite)

  for (const fixture of selectedFixtures) {
    await runFixtureCheck(fixture)
  }

  await finalizeReport('passed')
  console.log('Verificacao de fixtures invalidos concluida com sucesso.')
}

main().catch(async (error) => {
  const message = error instanceof Error ? error.message : 'Falha ao verificar fixtures invalidos.'
  await finalizeReport('failed', message)
  console.error(message)
  process.exitCode = 1
})