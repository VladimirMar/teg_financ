import { mkdir, readFile, writeFile } from 'node:fs/promises'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const workspaceRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..')
const importXmlDirectory = path.join(workspaceRoot, 'importXML')
const defaultFileName = process.env.ORDEM_SERVICO_XML_FILE ?? 'OrdemServico.xml'
const fileName = String(process.argv[2] ?? defaultFileName).trim()
const validationReportPath = process.env.ORDEM_SERVICO_VALIDATION_REPORT_PATH
  ?? path.join(importXmlDirectory, 'ordem_servico_validation_summary.json')
const importReportPath = process.env.ORDEM_SERVICO_IMPORT_REPORT_PATH
  ?? path.join(importXmlDirectory, 'ordem_servico_import_summary.json')
const pipelineReportPath = process.env.ORDEM_SERVICO_PIPELINE_REPORT_PATH
  ?? path.join(importXmlDirectory, 'ordem_servico_pipeline_summary.json')
const shouldCheckDbReferences = /^(1|true|yes)$/i.test(process.env.CHECK_DB_REFERENCES ?? '')
const maxReferenceErrors = Math.max(Number(process.env.PIPELINE_MAX_REFERENCE_ERRORS ?? 0) || 0, 0)

const runNodeScript = async (scriptName, extraEnv = {}) => {
  const scriptPath = path.join(workspaceRoot, 'scripts', scriptName)
  const child = await import('node:child_process')

  return new Promise((resolve) => {
    const spawned = child.spawn(process.execPath, [scriptPath, fileName], {
      cwd: workspaceRoot,
      env: {
        ...process.env,
        ...extraEnv,
      },
      stdio: ['ignore', 'pipe', 'pipe'],
    })

    let stdout = ''
    let stderr = ''

    spawned.stdout.on('data', (chunk) => {
      stdout += chunk.toString()
    })

    spawned.stderr.on('data', (chunk) => {
      stderr += chunk.toString()
    })

    spawned.on('close', (code) => {
      resolve({ code: code ?? 1, stdout: stdout.trim(), stderr: stderr.trim() })
    })
  })
}

const safeReadJson = async (filePath) => {
  try {
    const content = await readFile(filePath, 'utf8')
    return JSON.parse(content)
  } catch {
    return null
  }
}

const startedAt = new Date().toISOString()
const validationResult = await runNodeScript('validate-ordem-servico-xml.mjs', {
  ORDEM_SERVICO_XML_FILE: fileName,
  ORDEM_SERVICO_VALIDATION_REPORT_PATH: validationReportPath,
  CHECK_DB_REFERENCES: shouldCheckDbReferences ? 'true' : '',
})
const validationReport = await safeReadJson(validationReportPath)

const structuralValid = validationReport?.structuralValid ?? validationResult.code === 0
const referenceErrorCount = validationReport?.referenceErrorCount ?? 0
const referenceThresholdAccepted = shouldCheckDbReferences
  ? referenceErrorCount <= maxReferenceErrors
  : true
const shouldImport = structuralValid && referenceThresholdAccepted

let importResult = null
let importReport = null
let imported = false

if (shouldImport) {
  imported = true
  importResult = await runNodeScript('import-ordem-servico.mjs', {
    ORDEM_SERVICO_XML_FILE: fileName,
    ORDEM_SERVICO_IMPORT_REPORT_PATH: importReportPath,
  })
  importReport = await safeReadJson(importReportPath)
}

const finishedAt = new Date().toISOString()
const pipelineSummary = {
  fileName,
  startedAt,
  finishedAt,
  checkDbReferences: shouldCheckDbReferences,
  maxReferenceErrors,
  referenceThresholdAccepted,
  validation: {
    exitCode: validationResult.code,
    stdout: validationResult.stdout,
    stderr: validationResult.stderr,
    report: validationReport,
  },
  import: imported
    ? {
        exitCode: importResult?.code ?? 1,
        stdout: importResult?.stdout ?? '',
        stderr: importResult?.stderr ?? '',
        report: importReport,
      }
    : null,
  ok: shouldImport && imported && (importResult?.code ?? 1) === 0,
}

await mkdir(path.dirname(pipelineReportPath), { recursive: true })
await writeFile(pipelineReportPath, `${JSON.stringify(pipelineSummary, null, 2)}\n`, 'utf8')

console.log(JSON.stringify({
  fileName,
  structuralValid,
  referenceErrorCount,
  maxReferenceErrors,
  referenceThresholdAccepted,
  importStarted: imported,
  importOk: imported ? (importResult?.code ?? 1) === 0 : false,
  pipelineOk: pipelineSummary.ok,
  pipelineReportPath,
}, null, 2))

process.exit(pipelineSummary.ok ? 0 : 1)
