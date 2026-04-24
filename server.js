import { createServer } from 'node:http'
import { randomBytes, scryptSync, timingSafeEqual } from 'node:crypto'
import { spawn } from 'node:child_process'
import { readFile } from 'node:fs/promises'
import { XMLParser } from 'fast-xml-parser'
import path from 'node:path'
import { Pool } from 'pg'
import { fileURLToPath } from 'node:url'

const port = Number(process.env.PORT ?? 3001)
const workspaceRoot = path.dirname(fileURLToPath(import.meta.url))
const importXmlDirectory = path.join(workspaceRoot, 'importXML')
const xmlParser = new XMLParser({
  ignoreAttributes: false,
  trimValues: true,
})
const smokeRunPath = '/api/smoke/run'
const allowedSmokeSuites = new Set(['all', 'condutor', 'credenciada', 'veiculo', 'marca-modelo'])
const invalidFixtureSmokeSuites = new Set(['all', 'condutor', 'credenciada', 'veiculo'])

const truncateCommandOutput = (value, maxLength = 12000) => {
  const normalizedValue = String(value ?? '')

  if (normalizedValue.length <= maxLength) {
    return normalizedValue
  }

  return normalizedValue.slice(-maxLength)
}

const joinCommandOutput = (sections) => truncateCommandOutput(sections
  .filter((section) => String(section?.value ?? '').trim())
  .map((section) => `[${section.title}]\n${String(section.value).trim()}`)
  .join('\n\n'))

const runWorkspaceScript = async ({ scriptName, reportFileName, reportEnvName }) => {
  const reportFilePath = path.join(workspaceRoot, '.artifacts', reportFileName)
  const command = process.platform === 'win32'
    ? (process.env.ComSpec ?? 'cmd.exe')
    : 'npm'
  const commandArgs = process.platform === 'win32'
    ? ['/d', '/s', '/c', `npm run ${scriptName}`]
    : ['run', scriptName]

  const commandResult = await new Promise((resolve, reject) => {
    const child = spawn(command, commandArgs, {
      cwd: workspaceRoot,
      env: {
        ...process.env,
        API_BASE_URL: process.env.API_BASE_URL ?? `http://localhost:${port}`,
        PORT: String(port),
        [reportEnvName]: reportFilePath,
      },
      stdio: ['ignore', 'pipe', 'pipe'],
    })

    let stdout = ''
    let stderr = ''

    child.stdout.on('data', (chunk) => {
      stdout += chunk.toString()
    })

    child.stderr.on('data', (chunk) => {
      stderr += chunk.toString()
    })

    child.on('error', reject)
    child.on('close', (exitCode) => {
      resolve({ stdout, stderr, exitCode: exitCode ?? 1 })
    })
  })

  let reportPayload = null

  try {
    const reportContent = await readFile(reportFilePath, 'utf8')
    reportPayload = JSON.parse(reportContent)
  } catch {
    reportPayload = null
  }

  return {
    scriptName,
    status: commandResult.exitCode === 0 ? 'passed' : 'failed',
    exitCode: commandResult.exitCode,
    reportPath: reportFilePath,
    report: reportPayload,
    stdout: commandResult.stdout,
    stderr: commandResult.stderr,
    stdoutTail: truncateCommandOutput(commandResult.stdout),
    stderrTail: truncateCommandOutput(commandResult.stderr),
  }
}

const runSmokeSuiteCommand = async (suite = 'all') => {
  const normalizedSuite = normalizeRequestValue(suite).toLowerCase() || 'all'

  if (!allowedSmokeSuites.has(normalizedSuite)) {
    throw new Error(`Suite de smoke invalida: ${normalizedSuite}.`)
  }

  const invalidFixtureResult = invalidFixtureSmokeSuites.has(normalizedSuite)
    ? await runWorkspaceScript({
      scriptName: normalizedSuite === 'all' ? 'verify:invalid-fixtures' : `verify:invalid-fixtures:${normalizedSuite}`,
      reportFileName: `invalid-fixture-ui-report-${normalizedSuite}-${Date.now()}-${randomBytes(4).toString('hex')}.json`,
      reportEnvName: 'INVALID_FIXTURE_REPORT_PATH',
    })
    : null

  if (invalidFixtureResult?.exitCode) {
    return {
      suite: normalizedSuite,
      scriptName: invalidFixtureResult.scriptName,
      status: 'failed',
      exitCode: invalidFixtureResult.exitCode,
      reportPath: '',
      report: null,
      stdoutTail: invalidFixtureResult.stdoutTail,
      stderrTail: invalidFixtureResult.stderrTail,
      invalidFixtureStatus: invalidFixtureResult.status,
      invalidFixtureReportPath: invalidFixtureResult.reportPath,
      invalidFixtureReport: invalidFixtureResult.report,
    }
  }

  const smokeResult = await runWorkspaceScript({
    scriptName: normalizedSuite === 'all' ? 'smoke:api' : `smoke:api:${normalizedSuite}`,
    reportFileName: `smoke-ui-report-${normalizedSuite}-${Date.now()}-${randomBytes(4).toString('hex')}.json`,
    reportEnvName: 'SMOKE_REPORT_PATH',
  })

  return {
    suite: normalizedSuite,
    scriptName: smokeResult.scriptName,
    status: smokeResult.status,
    exitCode: smokeResult.exitCode,
    reportPath: smokeResult.reportPath,
    report: smokeResult.report,
    stdoutTail: joinCommandOutput([
      invalidFixtureResult
        ? { title: invalidFixtureResult.scriptName, value: invalidFixtureResult.stdout }
        : null,
      { title: smokeResult.scriptName, value: smokeResult.stdout },
    ]),
    stderrTail: joinCommandOutput([
      invalidFixtureResult
        ? { title: invalidFixtureResult.scriptName, value: invalidFixtureResult.stderr }
        : null,
      { title: smokeResult.scriptName, value: smokeResult.stderr },
    ]),
    invalidFixtureStatus: invalidFixtureResult?.status ?? 'not-run',
    invalidFixtureReportPath: invalidFixtureResult?.reportPath ?? '',
    invalidFixtureReport: invalidFixtureResult?.report ?? null,
  }
}

const pool = new Pool({
  host: process.env.PGHOST ?? 'localhost',
  port: Number(process.env.PGPORT ?? 5432),
  user: process.env.PGUSER ?? 'postgres',
  password: process.env.PGPASSWORD ?? '12345',
  database: process.env.PGDATABASE ?? 'teg_financ',
})

const ordemServicoTableName = 'ordem_servico'
const ordemServicoImportRecusaTableName = 'ordem_servico_import_recusa'
const ordemServicoCodigoSequenceName = 'ordem_servico_codigo_seq'
const credenciamentoTermoTableName = 'termo'
const credenciamentoTermoImportRecusaTableName = 'termo_import_recusa'
const credenciamentoTermoCodigoSequenceName = 'termo_codigo_seq'
const vinculoCondutorTableName = 'vinculo_condutor'
const vinculoCondutorImportRecusaTableName = 'vinculo_condutor_import_recusa'
const vinculoMonitorTableName = 'vinculo_monitor'
const vinculoMonitorImportRecusaTableName = 'vinculo_monitor_import_recusa'
const legacyCredenciamentoOsTableName = 'credenciamento_os'
const legacyCredenciamentoOsImportRecusaTableName = 'credenciamento_os_import_recusa'
const legacyCredenciamentoOsCodigoSequenceName = 'credenciamento_os_codigo_seq'
const ordemServicoCollectionPath = '/api/ordem-servico'
const ordemServicoNextNumOsPath = '/api/ordem-servico/next-num-os'
const ordemServicoNextRevisaoPath = '/api/ordem-servico/next-revisao'
const ordemServicoActiveCpfPath = '/api/ordem-servico/active-cpf'
const ordemServicoActivePlacaPath = '/api/ordem-servico/active-placa'
const ordemServicoImportXmlPath = '/api/ordem-servico/import-xml'
const ordemServicoImportRejectionsPath = '/api/ordem-servico/import-rejections'
const credenciamentoTermoCollectionPath = '/api/termo'
const credenciamentoTermoLookupPath = '/api/termo/lookup'
const credenciamentoTermoImportXmlPath = '/api/termo/import-xml'
const credenciamentoTermoImportRejectionsPath = '/api/termo/import-rejections'
const vinculoCondutorCollectionPath = '/api/vinculo-condutor'
const vinculoCondutorImportXmlPath = '/api/vinculo-condutor/import-xml'
const vinculoCondutorImportRejectionsPath = '/api/vinculo-condutor/import-rejections'
const vinculoMonitorCollectionPath = '/api/vinculo-monitor'
const vinculoMonitorImportXmlPath = '/api/vinculo-monitor/import-xml'
const vinculoMonitorImportRejectionsPath = '/api/vinculo-monitor/import-rejections'
const cepTableName = 'ceps'
const cepImportRecusaTableName = 'cep_import_recusa'
const emissaoDocumentoParametroTableName = 'emissao_documento_parametro'
const cepCollectionPath = '/api/cep'
const cepLookupPath = '/api/cep/lookup'
const cepImportXmlPath = '/api/cep/import-xml'
const cepImportRejectionsPath = '/api/cep/import-rejections'
const emissaoDocumentoParametroCollectionPath = '/api/emissao-documento-parametro'
const emissaoDocumentoParametroResolvePath = '/api/emissao-documento-parametro/resolve'

const sendJson = (response, statusCode, payload) => {
  response.writeHead(statusCode, {
    'Content-Type': 'application/json; charset=utf-8',
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
  })
  response.end(JSON.stringify(payload))
}

const readJsonBody = (request) => new Promise((resolve, reject) => {
  let body = ''

  request.on('data', (chunk) => {
    body += chunk
  })

  request.on('end', () => {
    if (!body) {
      resolve({})
      return
    }

    try {
      resolve(JSON.parse(body))
    } catch (error) {
      reject(new Error('Corpo JSON invalido.'))
    }
  })

  request.on('error', reject)
})

const normalizeRequestValue = (value) => {
  if (value == null) {
    return ''
  }

  return String(value).trim()
}

const normalizeDbValue = (value) => normalizeRequestValue(value)

const normalizeAccessName = (value) => {
  return normalizeRequestValue(value)
    .replace(/[^ -]+/g, ' ')
    .replace(/[^ -]/g, ' ')
    .replace(/[^ -]/g, ' ')
    .replace(/[^ -]/g, ' ')
    .replace(/[^ -]/g, ' ')
    .replace(/[^ -]/g, ' ')
    .replace(/[^A-Z�-�a-z�-�\s]/g, ' ')
    .toUpperCase()
    .replace(/\s+/g, ' ')
    .trim()
}

const isAccessNameValid = (value) => {
  return /^[\p{Lu} ]{1,50}$/u.test(value)
}

const buildGeneratedAccessName = (email, sequenceNumber) => {
  const emailBaseName = normalizeAccessName(email.split('@')[0] ?? '')
    .replace(/[^\p{Lu} ]/gu, '')
  const fallbackBase = emailBaseName || 'USUARIO'
  const suffix = sequenceNumber > 1 ? ` ${sequenceNumber}` : ''

  return `${fallbackBase}${suffix}`.slice(0, 50)
}

const createToken = (email) => {
  return Buffer.from(`${email}:${Date.now()}`).toString('base64url')
}

const createPasswordHash = (password) => {
  const salt = randomBytes(16).toString('hex')
  const hash = scryptSync(password, salt, 64).toString('hex')
  return `scrypt:${salt}:${hash}`
}

const verifyPassword = (password, storedHash) => {
  const normalizedHash = normalizeDbValue(storedHash)

  if (!normalizedHash) {
    return false
  }

  if (!normalizedHash.startsWith('scrypt:')) {
    return normalizedHash === password
  }

  const [, salt, savedHash] = normalizedHash.split(':')

  if (!salt || !savedHash) {
    return false
  }

  const derivedKey = scryptSync(password, salt, 64)
  const savedBuffer = Buffer.from(savedHash, 'hex')

  if (savedBuffer.length !== derivedKey.length) {
    return false
  }

  return timingSafeEqual(savedBuffer, derivedKey)
}

const getDreCodigoFromUrl = (url) => {
  const match = url.match(/^\/api\/dre\/([^/]+)$/)
  return match ? decodeURIComponent(match[1]) : null
}

const getModalidadeCodigoFromUrl = (url) => {
  const match = url.match(/^\/api\/modalidade\/([^/]+)$/)
  return match ? decodeURIComponent(match[1]) : null
}

const getMarcaModeloCodigoFromUrl = (url) => {
  const match = url.match(/^\/api\/marca-modelo\/([^/]+)$/)
  return match ? decodeURIComponent(match[1]) : null
}

const getSeguradoraCodigoFromUrl = (url) => {
  const match = url.match(/^\/api\/seguradora\/([^/]+)$/)
  return match ? decodeURIComponent(match[1]) : null
}

const getTrocaCodigoFromUrl = (url) => {
  const match = url.match(/^\/api\/troca\/([^/]+)$/)
  return match ? decodeURIComponent(match[1]) : null
}

const getAccessCodigoFromUrl = (url) => {
  const match = url.match(/^\/api\/access\/([^/]+)$/)
  return match ? decodeURIComponent(match[1]) : null
}

const getCondutorCodigoFromUrl = (url) => {
  const match = url.match(/^\/api\/condutor\/([^/]+)$/)
  return match ? decodeURIComponent(match[1]) : null
}

const getMonitorCodigoFromUrl = (url) => {
  const match = url.match(/^\/api\/monitor\/([^/]+)$/)
  return match ? decodeURIComponent(match[1]) : null
}

const getCepFromUrl = (url) => {
  const match = url.match(/^\/api\/cep\/([^/]+)$/)
  return match ? decodeURIComponent(match[1]) : null
}

const getCredenciadaCodigoFromUrl = (url) => {
  const match = url.match(/^\/api\/credenciada\/([^/]+)$/)
  return match ? decodeURIComponent(match[1]) : null
}

const getVeiculoCodigoFromUrl = (url) => {
  const match = url.match(/^\/api\/veiculo\/([^/]+)$/)
  return match ? decodeURIComponent(match[1]) : null
}

const getCredenciamentoTermoCodigoFromUrl = (url) => {
  const match = url.match(/^\/api\/termo\/([^/]+)$/)
  return match ? decodeURIComponent(match[1]) : null
}

const getVinculoCondutorIdFromUrl = (url) => {
  const match = url.match(/^\/api\/vinculo-condutor\/([^/]+)$/)
  return match ? decodeURIComponent(match[1]) : null
}

const getVinculoMonitorIdFromUrl = (url) => {
  const match = url.match(/^\/api\/vinculo-monitor\/([^/]+)$/)
  return match ? decodeURIComponent(match[1]) : null
}

const getOrdemServicoCodigoFromUrl = (url) => {
  const match = url.match(/^\/api\/ordem-servico\/([^/]+)$/)
  return match ? decodeURIComponent(match[1]) : null
}

const getTitularCodigoFromUrl = (url) => {
  const match = url.match(/^\/api\/titular\/([^/]+)$/)
  return match ? decodeURIComponent(match[1]) : null
}

const getEmissaoDocumentoParametroDataFromUrl = (url) => {
  const match = url.match(/^\/api\/emissao-documento-parametro\/([^/]+)$/)
  return match ? decodeURIComponent(match[1]) : null
}

const getLoginDrePairFromUrl = (url) => {
  const match = url.match(/^\/api\/login-dre\/([^/]+)\/([^/]+)$/)

  if (!match) {
    return null
  }

  return {
    loginCodigo: decodeURIComponent(match[1]),
    dreCodigo: decodeURIComponent(match[2]),
  }
}

const createAccessHashPayload = (password) => {
  const passwordHash = createPasswordHash(password)

  return {
    password: passwordHash,
    descricao: passwordHash,
  }
}

const normalizeCondutorName = (value) => {
  return normalizeRequestValue(value)
    .replace(/\([^)]*\)/g, ' ')
    .replace(/[^\p{L}\s]/gu, ' ')
    .toUpperCase()
    .toUpperCase()
    .replace(/\s+/g, ' ')
    .trim()
}

const normalizeCondutorCodigo = (value) => {
  const normalizedValue = normalizeRequestValue(value)

  if (!normalizedValue) {
    return null
  }

  const numericValue = Number(normalizedValue)
  return Number.isInteger(numericValue) && numericValue > 0 ? numericValue : Number.NaN
}

const isCondutorNameValid = (value) => {
  return /^[\p{Lu} ]{1,100}$/u.test(value)
}

const isMonitorNameValid = (value) => {
  return /^[\p{Lu} ]{1,255}$/u.test(value)
}

const normalizeCpf = (value) => {
  const digits = normalizeRequestValue(value).replace(/\D/g, '').slice(0, 11)

  if (digits.length <= 3) {
    return digits
  }

  if (digits.length <= 6) {
    return `${digits.slice(0, 3)}.${digits.slice(3)}`
  }

  if (digits.length <= 9) {
    return `${digits.slice(0, 3)}.${digits.slice(3, 6)}.${digits.slice(6)}`
  }

  return `${digits.slice(0, 3)}.${digits.slice(3, 6)}.${digits.slice(6, 9)}-${digits.slice(9, 11)}`
}

const isCpfValid = (value) => {
  const digits = value.replace(/\D/g, '')
  return digits.length === 11
}

const normalizeCrmc = (value) => {
  const digits = normalizeRequestValue(value).replace(/\D/g, '').slice(0, 8)

  if (digits.length <= 3) {
    return digits
  }

  if (digits.length <= 6) {
    return `${digits.slice(0, 3)}.${digits.slice(3)}`
  }

  return `${digits.slice(0, 3)}.${digits.slice(3, 6)}-${digits.slice(6, 8)}`
}

const isCrmcValid = (value) => {
  return /^\d{3}\.\d{3}-\d{2}$/.test(value)
}

const normalizeMonitorRg = (value) => {
  return normalizeRequestValue(value)
    .toUpperCase()
    .replace(/[^0-9A-Z.\-/]/g, '')
    .slice(0, 20)
}

const isMonitorRgValid = (value) => {
  return /^[0-9A-Z.\-/]{1,20}$/.test(value)
}

const normalizeTipoVinculo = (value) => {
  const normalizedValue = normalizeRequestValue(value)

  if (!normalizedValue) {
    return ''
  }

  const normalizedKey = normalizedValue
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')

  if (normalizedKey === 'cooperado') {
    return 'Cooperado'
  }

  if (normalizedKey === 'socio') {
    return 'S\u00f3cio'
  }

  if (normalizedKey === 'funcionario') {
    return 'Funcion\u00e1rio'
  }

  return null
}

const normalizeHistorico = (value) => {
  return normalizeRequestValue(value).slice(0, 200)
}

const normalizeCredenciadaText = (value, maxLength = 255) => {
  return normalizeRequestValue(value)
    .replace(/\s+/g, ' ')
    .toUpperCase()
    .slice(0, maxLength)
}

const normalizeCredenciadaStatusValue = (value) => {
  const normalizedValue = normalizeCredenciadaText(value, 50)

  if (!normalizedValue) {
    return 'ATIVO'
  }

  return normalizedValue === 'CANCELADO' ? 'CANCELADO' : 'ATIVO'
}

const normalizeTrocaText = (value, maxLength = 255) => {
  return normalizeRequestValue(value)
    .replace(/\s+/g, ' ')
    .toUpperCase()
    .slice(0, maxLength)
}

const normalizeTrocaLookupKey = (value) => {
  return normalizeTrocaText(value, 255)
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/SUBSTITUI[^A-Z0-9]*FO/g, 'SUBSTITUICAO')
    .replace(/ALTERA[^A-Z0-9]*FO/g, 'ALTERACAO')
    .replace(/ATUALIZA[^A-Z0-9]*AO/g, 'ATUALIZACAO')
    .replace(/REVISA[^A-Z0-9]*AO/g, 'REVISAO')
    .replace(/INCLUSA[^A-Z0-9]*AO/g, 'INCLUSAO')
    .replace(/CORRE[^A-Z0-9]*AO/g, 'CORRECAO')
    .replace(/[^A-Z0-9]/g, '')
}

const normalizeOperationalCode = (value, maxLength = 255) => {
  return normalizeRequestValue(value)
    .replace(/\s+/g, ' ')
    .toUpperCase()
    .slice(0, maxLength)
}

const normalizeCredenciamentoSituacao = (value) => {
  const normalizedValue = normalizeRequestValue(value)

  if (!normalizedValue) {
    return ''
  }

  const normalizedKey = normalizedValue
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')

  if (normalizedKey === 'ativo') {
    return 'Ativo'
  }

  if (normalizedKey === 'inativo') {
    return 'Inativo'
  }

  if (normalizedKey === 'cancelado') {
    return 'Cancelado'
  }

  if (normalizedKey === 'substituido') {
    return 'Substituido'
  }

  return null
}

const normalizeCredenciamentoAnnotation = (value) => {
  return normalizeRequestValue(value)
    .replace(/\s+/g, ' ')
    .slice(0, 1000)
}

const normalizeDreOperationalCode = (value) => {
  return normalizeRequestValue(value)
    .replace(/\s+/g, ' ')
    .toUpperCase()
    .slice(0, 30)
}

const normalizeDreSigla = (value) => {
  return normalizeRequestValue(value)
    .toUpperCase()
    .replace(/[^A-Z]/g, '')
    .slice(0, 2)
}

const normalizeDreDescription = (value) => {
  return normalizeRequestValue(value)
    .replace(/\s+/g, ' ')
    .toUpperCase()
    .slice(0, 255)
}

const normalizeEmailList = (value) => {
  const normalizedValue = normalizeRequestValue(value)

  if (!normalizedValue) {
    return ''
  }

  return normalizedValue
    .split(';')
    .map((item) => item.trim().toLowerCase())
    .filter(Boolean)
    .join('; ')
    .slice(0, 255)
}

const isEmailListValid = (value) => {
  if (!value) {
    return true
  }

  return value
    .split(';')
    .map((item) => item.trim())
    .filter(Boolean)
    .every((item) => /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(item))
}

const normalizeCep = (value) => {
  const digits = normalizeRequestValue(value).replace(/\D/g, '').slice(0, 8)

  if (digits.length <= 5) {
    return digits
  }

  return `${digits.slice(0, 5)}-${digits.slice(5)}`
}

const isCepValid = (value) => {
  return /^\d{5}-\d{3}$/.test(value)
}

const normalizePhoneNumber = (value) => {
  const digits = normalizeRequestValue(value).replace(/\D/g, '').slice(0, 11)

  if (digits.length <= 4) {
    return digits
  }

  if (digits.length <= 8) {
    return `${digits.slice(0, 4)}-${digits.slice(4)}`
  }

  return `${digits.slice(0, 5)}-${digits.slice(5, 9)}`
}

const isPhoneNumberValid = (value) => {
  return /^\d{4,5}-\d{4}$/.test(value)
}

const normalizeCnpjCpf = (value) => {
  const digits = normalizeRequestValue(value).replace(/\D/g, '').slice(0, 14)

  if (digits.length <= 3) {
    return digits
  }

  if (digits.length <= 6) {
    return `${digits.slice(0, 3)}.${digits.slice(3)}`
  }

  if (digits.length <= 9) {
    return `${digits.slice(0, 3)}.${digits.slice(3, 6)}.${digits.slice(6)}`
  }

  if (digits.length <= 11) {
    return `${digits.slice(0, 3)}.${digits.slice(3, 6)}.${digits.slice(6, 9)}-${digits.slice(9, 11)}`
  }

  if (digits.length <= 12) {
    return `${digits.slice(0, 2)}.${digits.slice(2, 5)}.${digits.slice(5, 8)}/${digits.slice(8)}`
  }

  return `${digits.slice(0, 2)}.${digits.slice(2, 5)}.${digits.slice(5, 8)}/${digits.slice(8, 12)}-${digits.slice(12, 14)}`
}

const normalizeTitularDocument = (value) => {
  return normalizeRequestValue(value)
    .replace(/[^0-9.,\/-]/g, '')
    .slice(0, 18)
}

const extractDocumentDigits = (value) => {
  return normalizeRequestValue(value).replace(/\D/g, '')
}

const isCnpjCpfValid = (value) => {
  const digits = value.replace(/\D/g, '')
  return digits.length === 11 || digits.length === 14
}

const findTitularByCnpjCpf = async (cnpjCpf) => {
  const digits = extractDocumentDigits(cnpjCpf)

  if (!digits) {
    return null
  }

  const result = await pool.query(
    `SELECT
       ${titularSelectClause}
     FROM ${titularTableName}
     WHERE regexp_replace(COALESCE(cnpj_cpf, ''), '[^0-9]', '', 'g') = $1
     ORDER BY codigo ASC
     LIMIT 1`,
    [digits],
  )

  return result.rows[0] ?? null
}

const findCredenciadaByCodigo = async (codigo, executor = pool) => {
  const normalizedCodigo = normalizeCondutorCodigo(codigo)

  if (!Number.isInteger(normalizedCodigo) || normalizedCodigo <= 0) {
    return null
  }

  const result = await executor.query(
    `SELECT
       ${credenciadaSelectClause}
     FROM credenciada
     WHERE codigo = $1
     ORDER BY codigo ASC
     LIMIT 1`,
    [normalizedCodigo],
  )

  return result.rows[0] ?? null
}

const findCredenciadaByCnpjCpf = async (cnpjCpf, executor = pool) => {
  const digits = extractDocumentDigits(cnpjCpf)

  if (!digits) {
    return null
  }

  const result = await executor.query(
    `SELECT
       ${credenciadaSelectClause}
     FROM credenciada
     WHERE regexp_replace(COALESCE(cnpj_cpf, ''), '[^0-9]', '', 'g') = $1
     ORDER BY codigo ASC
     LIMIT 1`,
    [digits],
  )

  return result.rows[0] ?? null
}

const findCredenciadaByName = async (credenciado, executor = pool) => {
  const normalizedCredenciado = normalizeCredenciadaText(credenciado, 255)
  if (!normalizedCredenciado) {
    return null
  }

  const result = await executor.query(
    `SELECT
       ${credenciadaSelectClause}
     FROM credenciada
     WHERE UPPER(BTRIM(credenciado)) = $1
     ORDER BY codigo ASC
     LIMIT 1`,
    [normalizedCredenciado],
  )

  return result.rows[0] ?? null
}

const dreSelectClause = `
  CAST(codigo AS text) AS codigo,
  COALESCE(BTRIM(sigla), '') AS sigla,
  COALESCE(BTRIM(codigo_operacional), '') AS codigo_operacional,
  BTRIM(CAST(descricao AS text)) AS descricao`

const modalidadeSelectClause = `
  CAST(codigo AS text) AS codigo,
  BTRIM(CAST(descricao AS text)) AS descricao`

const normalizeModalidadeDescriptionKey = (value) => normalizeRequestValue(value)
  .toUpperCase()
  .replace(/\s+/g, '_')
  .replace(/[^A-Z0-9_]/g, '')
  .slice(0, 255)

const findModalidadeByCodigoOrDescription = async ({ codigo, descricao }) => {
  const normalizedCodigo = normalizeRequestValue(codigo)
  const normalizedDescricao = normalizeModalidadeDescriptionKey(descricao)

  if (!normalizedCodigo && !normalizedDescricao) {
    return null
  }

  const values = []
  const filters = []

  if (normalizedCodigo) {
    values.push(normalizedCodigo)
    filters.push(`CAST(codigo AS text) = $${values.length}`)
  }

  if (normalizedDescricao) {
    values.push(normalizedDescricao)
    filters.push(`REGEXP_REPLACE(UPPER(BTRIM(CAST(descricao AS text))), '[^A-Z0-9]+', '_', 'g') = $${values.length}`)
  }

  const result = await pool.query(
    `SELECT ${modalidadeSelectClause}
     FROM modalidade
     WHERE ${filters.join(' OR ')}
     ORDER BY codigo ASC
     LIMIT 1`,
    values,
  )

  return result.rows[0] ?? null
}

const deriveModalidadeDescricaoFromDreDescricao = (dreDescricao) => {
  const normalizedDescricao = normalizeRequestValue(dreDescricao).toUpperCase().slice(0, 255)
  const normalizedKey = normalizedDescricao
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^A-Z0-9]/g, '')

  if (!normalizedDescricao) {
    return ''
  }

  if (normalizedKey.includes('CRECHE')) {
    return 'TEG CRECHE'
  }

  if (normalizedKey.includes('ESPECIAL')) {
    return 'TEG ESPECIAL'
  }

  return 'TEG REGULAR'
}

const getCanonicalDreDescription = (dreDescricao) => {
  const normalizedDescricao = normalizeRequestValue(dreDescricao).toUpperCase().slice(0, 255)

  if (!normalizedDescricao) {
    return ''
  }

  return normalizedDescricao
    .replace(/[\s_]+TEG[\s_]+CRECHE$/i, '')
    .replace(/[\s_]+TEG[\s_]+ESPECIAL$/i, '')
    .trim()
}

const normalizeDreKey = (value) => normalizeRequestValue(value)
  .toUpperCase()
  .normalize('NFD')
  .replace(/[\u0300-\u036f]/g, '')
  .replace(/[^A-Z0-9]/g, '')

const isTegVariantDre = ({ descricao = '' }) => deriveModalidadeDescricaoFromDreDescricao(descricao) !== 'TEG REGULAR'

const resolveCanonicalOrdemServicoDre = async (dreItem) => {
  if (!dreItem || !isTegVariantDre({ descricao: dreItem.descricao })) {
    return dreItem
  }

  const canonicalDescricao = getCanonicalDreDescription(dreItem.descricao)

  if (!canonicalDescricao) {
    return dreItem
  }

  const canonicalDreItem = await findDreByDescription(canonicalDescricao)
  return canonicalDreItem ?? dreItem
}

const ensureDefaultModalidadeEntries = async () => {
  const defaultDescriptions = ['TEG REGULAR', 'TEG CRECHE', 'TEG ESPECIAL']

  for (const descricao of defaultDescriptions) {
    await pool.query(
      `INSERT INTO modalidade (descricao)
       SELECT CAST($1 AS varchar(255))
       WHERE NOT EXISTS (
         SELECT 1
         FROM modalidade
         WHERE UPPER(BTRIM(CAST(descricao AS text))) = CAST($1 AS text)
       )`,
      [descricao],
    )
  }
}

const backfillOrdemServicoModalidadesFromDre = async () => {
  const modalidadeResult = await pool.query(`SELECT ${modalidadeSelectClause} FROM modalidade`)
  const modalidadeByDescricao = new Map(
    modalidadeResult.rows.map((item) => [normalizeModalidadeDescriptionKey(item.descricao), item]),
  )

  const ordemServicoResult = await pool.query(
    `SELECT
       codigo,
       COALESCE(BTRIM(dre_codigo), '') AS dre_codigo,
       COALESCE(BTRIM(dre_descricao), '') AS dre_descricao,
       COALESCE(modalidade_codigo::text, '') AS modalidade_codigo,
       COALESCE(BTRIM(modalidade_descricao), '') AS modalidade_descricao
     FROM ${ordemServicoTableName}`,
  )

  const client = await pool.connect()

  try {
    await client.query('BEGIN')
    let updatedCount = 0

    for (const row of ordemServicoResult.rows) {
      const derivedDescricao = deriveModalidadeDescricaoFromDreDescricao(row.dre_descricao)
      const shouldNormalizeDre = isTegVariantDre({ descricao: row.dre_descricao })
      const canonicalDescricao = getCanonicalDreDescription(row.dre_descricao)
      const canonicalDreItem = shouldNormalizeDre && canonicalDescricao
        ? await findDreByDescription(canonicalDescricao)
        : null

      if (!derivedDescricao) {
        continue
      }

      const modalidadeItem = modalidadeByDescricao.get(derivedDescricao)

      if (!modalidadeItem) {
        continue
      }

      const currentCodigo = normalizeRequestValue(row.modalidade_codigo)
      const currentDescricao = normalizeModalidadeDescriptionKey(row.modalidade_descricao)
      const nextDreCodigo = canonicalDreItem
        ? normalizeRequestValue(canonicalDreItem.codigo_operacional || canonicalDreItem.codigo)
        : normalizeRequestValue(row.dre_codigo)
      const nextDreDescricao = canonicalDreItem
        ? normalizeRequestValue(canonicalDreItem.descricao)
        : normalizeRequestValue(row.dre_descricao)

      if (
        currentCodigo === String(modalidadeItem.codigo)
        && currentDescricao === derivedDescricao
        && nextDreCodigo === normalizeRequestValue(row.dre_codigo)
        && nextDreDescricao === normalizeRequestValue(row.dre_descricao)
      ) {
        continue
      }

      await client.query(
        `UPDATE ${ordemServicoTableName}
         SET dre_codigo = $1,
             dre_descricao = $2,
             modalidade_codigo = $3,
             modalidade_descricao = $4,
             data_modificacao = NOW()
         WHERE codigo = $5`,
        [
          normalizeOperationalCode(nextDreCodigo, 30),
          normalizeOperationalCode(nextDreDescricao, 255),
          Number(modalidadeItem.codigo),
          normalizeOperationalCode(derivedDescricao, 255),
          Number(row.codigo),
        ],
      )

      updatedCount += 1
    }

    await client.query('COMMIT')

    return updatedCount
  } catch (error) {
    await client.query('ROLLBACK')
    throw error
  } finally {
    client.release()
  }
}

const compactDreCodes = async () => {
  const dreResult = await pool.query(
    `SELECT
       codigo,
       COALESCE(BTRIM(codigo_operacional), '') AS codigo_operacional,
       BTRIM(CAST(descricao AS text)) AS descricao
     FROM dre
     ORDER BY codigo ASC`,
  )

  const dreRows = dreResult.rows.map((row, index) => ({
    codigoAtual: Number(row.codigo),
    codigoNovo: index + 1,
    codigoOperacional: normalizeRequestValue(row.codigo_operacional).toUpperCase().slice(0, 30),
    descricao: normalizeRequestValue(row.descricao).toUpperCase().slice(0, 255),
  }))

  const dreRowsToRenumber = dreRows.filter((row) => row.codigoAtual !== row.codigoNovo)

  const client = await pool.connect()

  try {
    await client.query('BEGIN')
    await client.query('LOCK TABLE dre IN ACCESS EXCLUSIVE MODE')

    const loginDreExistsResult = await client.query("SELECT to_regclass('public.login_dre') IS NOT NULL AS exists")
    const ordemServicoExistsResult = await client.query(`SELECT to_regclass('public.${ordemServicoTableName}') IS NOT NULL AS exists`)
    const loginDreExists = Boolean(loginDreExistsResult.rows[0]?.exists)
    const ordemServicoExists = Boolean(ordemServicoExistsResult.rows[0]?.exists)

    if (loginDreExists) {
      await client.query('LOCK TABLE login_dre IN ACCESS EXCLUSIVE MODE')
      await client.query('ALTER TABLE login_dre DROP CONSTRAINT IF EXISTS login_dre_dre_fk')
    }

    if (ordemServicoExists) {
      await client.query(`LOCK TABLE ${ordemServicoTableName} IN ACCESS EXCLUSIVE MODE`)
    }

    let updatedOrdemServicoCount = 0

    if (ordemServicoExists) {
      for (const row of dreRows) {
        const nextDreCodigo = row.codigoOperacional || String(row.codigoNovo)
        const updateResult = await client.query(
          `UPDATE ${ordemServicoTableName}
           SET dre_codigo = $1,
               dre_descricao = $2,
               data_modificacao = NOW()
           WHERE BTRIM(COALESCE(dre_codigo, '')) = $3`,
          [nextDreCodigo, row.descricao, String(row.codigoAtual)],
        )

        updatedOrdemServicoCount += updateResult.rowCount
      }
    }

    if (dreRowsToRenumber.length > 0) {
      for (const row of dreRowsToRenumber) {
        await client.query('UPDATE dre SET codigo = $1 WHERE codigo = $2', [-row.codigoAtual, row.codigoAtual])

        if (loginDreExists) {
          await client.query('UPDATE login_dre SET dre_codigo = $1 WHERE dre_codigo = $2', [-row.codigoAtual, row.codigoAtual])
        }
      }

      for (const row of dreRowsToRenumber) {
        await client.query('UPDATE dre SET codigo = $1 WHERE codigo = $2', [row.codigoNovo, -row.codigoAtual])

        if (loginDreExists) {
          await client.query('UPDATE login_dre SET dre_codigo = $1 WHERE dre_codigo = $2', [row.codigoNovo, -row.codigoAtual])
        }
      }
    }

    if (loginDreExists) {
      await client.query(`
        ALTER TABLE login_dre
        ADD CONSTRAINT login_dre_dre_fk
        FOREIGN KEY (dre_codigo) REFERENCES dre(codigo)
      `)
    }

    await client.query("SELECT setval('dre_codigo_seq', GREATEST(COALESCE((SELECT MAX(codigo) FROM dre), 0), 1), true)")
    await client.query('COMMIT')

    return {
      updatedDreCount: dreRowsToRenumber.length,
      updatedOrdemServicoCount,
    }
  } catch (error) {
    await client.query('ROLLBACK')
    throw error
  } finally {
    client.release()
  }
}

const findDreByCodigo = async (codigo) => {
  const normalizedCodigo = normalizeDreOperationalCode(codigo)

  if (!normalizedCodigo) {
    return null
  }

  const result = await pool.query(
    `SELECT ${dreSelectClause}
     FROM dre
     WHERE CAST(codigo AS text) = $1
        OR UPPER(BTRIM(COALESCE(codigo_operacional, ''))) = $1
     LIMIT 1`,
    [normalizedCodigo],
  )

  return result.rows[0] ?? null
}

const findDreByDescription = async (descricao) => {
  const normalizedDescricao = normalizeDreDescription(descricao)

  if (!normalizedDescricao) {
    return null
  }

  const result = await pool.query(
    `SELECT ${dreSelectClause}
     FROM dre
     WHERE UPPER(BTRIM(CAST(descricao AS text))) = $1
     LIMIT 1`,
    [normalizedDescricao],
  )

  return result.rows[0] ?? null
}

const ensureDreOperationalEntry = async ({ codigo, descricao }) => {
  const normalizedCodigo = normalizeDreOperationalCode(codigo)
  const normalizedDescricao = normalizeDreDescription(descricao)

  if (!normalizedCodigo && !normalizedDescricao) {
    return null
  }

  let existingItem = normalizedCodigo ? await findDreByCodigo(normalizedCodigo) : null

  if (!existingItem && normalizedDescricao) {
    existingItem = await findDreByDescription(normalizedDescricao)
  }

  if (existingItem) {
    if (normalizedCodigo && normalizeDreOperationalCode(existingItem.codigo_operacional) !== normalizedCodigo) {
      const updatedResult = await pool.query(
        `UPDATE dre
         SET codigo_operacional = $1
         WHERE codigo = $2
         RETURNING ${dreSelectClause}`,
        [normalizedCodigo, existingItem.codigo],
      )

      return updatedResult.rows[0] ?? existingItem
    }

    return existingItem
  }

  if (!normalizedCodigo || !normalizedDescricao) {
    return null
  }

  const insertResult = await pool.query(
    `INSERT INTO dre (codigo_operacional, descricao)
     VALUES ($1, $2)
     RETURNING ${dreSelectClause}`,
    [normalizedCodigo, normalizedDescricao],
  )

  return insertResult.rows[0] ?? null
}

const findCondutorByCodigo = async (codigo, executor = pool) => {
  const normalizedCodigo = normalizeCondutorCodigo(codigo)

  if (!Number.isInteger(normalizedCodigo) || normalizedCodigo <= 0) {
    return null
  }

  const result = await executor.query(
    `SELECT
       ${condutorSelectClause}
     FROM condutor
     WHERE codigo = $1
     ORDER BY codigo ASC
     LIMIT 1`,
    [normalizedCodigo],
  )

  return result.rows[0] ?? null
}

const findCondutorByCpf = async (cpfCondutor, executor = pool) => {
  const digits = extractDocumentDigits(cpfCondutor)

  if (!digits) {
    return null
  }

  const result = await executor.query(
    `SELECT
       ${condutorSelectClause}
     FROM condutor
     WHERE regexp_replace(COALESCE(cpf_condutor, ''), '[^0-9]', '', 'g') = $1
     ORDER BY codigo ASC
     LIMIT 1`,
    [digits],
  )

  return result.rows[0] ?? null
}

const getNextCondutorCodigo = async (executor = pool) => {
  const result = await executor.query(
    `SELECT COALESCE(MAX(codigo), 0) + 1 AS next_codigo
     FROM condutor`,
  )

  return Number(result.rows[0]?.next_codigo || 1)
}

const findMonitorByCpf = async (cpfMonitor, executor = pool) => {
  const digits = extractDocumentDigits(cpfMonitor)

  if (!digits) {
    return null
  }

  const result = await executor.query(
    `SELECT
       ${monitorSelectClause}
     FROM monitor
     WHERE regexp_replace(COALESCE(cpf_monitor, ''), '[^0-9]', '', 'g') = $1
     ORDER BY codigo ASC
     LIMIT 1`,
    [digits],
  )

  return result.rows[0] ?? null
}

const findActiveOrdemServicoByCpf = async (cpfValue, { excludeCodigo = null } = {}, executor = pool) => {
  const digits = extractDocumentDigits(cpfValue)
  const normalizedExcludeCodigo = normalizeCondutorCodigo(excludeCodigo)

  if (!digits) {
    return null
  }

  const result = await executor.query(
    `SELECT
       codigo,
       termo_adesao,
       num_os,
       revisao,
       situacao,
       CASE
         WHEN regexp_replace(COALESCE(cpf_condutor, ''), '[^0-9]', '', 'g') = $1 THEN 'condutor'
         WHEN regexp_replace(COALESCE(cpf_preposto, ''), '[^0-9]', '', 'g') = $1 THEN 'preposto'
         WHEN regexp_replace(COALESCE(cpf_monitor, ''), '[^0-9]', '', 'g') = $1 THEN 'monitor'
         ELSE ''
       END AS papel
     FROM ${ordemServicoTableName}
     WHERE UPPER(BTRIM(COALESCE(situacao, ''))) = 'ATIVO'
       AND ($2::int IS NULL OR codigo <> $2)
       AND (
         regexp_replace(COALESCE(cpf_condutor, ''), '[^0-9]', '', 'g') = $1
         OR regexp_replace(COALESCE(cpf_preposto, ''), '[^0-9]', '', 'g') = $1
         OR regexp_replace(COALESCE(cpf_monitor, ''), '[^0-9]', '', 'g') = $1
       )
     ORDER BY codigo ASC
     LIMIT 1`,
    [digits, Number.isInteger(normalizedExcludeCodigo) && normalizedExcludeCodigo > 0 ? normalizedExcludeCodigo : null],
  )

  return result.rows[0] ?? null
}

const findActiveOrdemServicoByPlaca = async (placaValue, { excludeCodigo = null } = {}, executor = pool) => {
  const normalizedPlaca = normalizeVehiclePlaca(placaValue)
  const normalizedExcludeCodigo = normalizeCondutorCodigo(excludeCodigo)

  if (!normalizedPlaca) {
    return null
  }

  const result = await executor.query(
    `SELECT
       codigo,
       termo_adesao,
       num_os,
       revisao,
       situacao,
       veiculo_placas
     FROM ${ordemServicoTableName}
     WHERE UPPER(BTRIM(COALESCE(situacao, ''))) = 'ATIVO'
       AND ($2::int IS NULL OR codigo <> $2)
       AND regexp_replace(UPPER(COALESCE(veiculo_placas, '')), '[^A-Z0-9]', '', 'g') = $1
     ORDER BY codigo ASC
     LIMIT 1`,
    [normalizedPlaca, Number.isInteger(normalizedExcludeCodigo) && normalizedExcludeCodigo > 0 ? normalizedExcludeCodigo : null],
  )

  return result.rows[0] ?? null
}

const findMonitorByCodigo = async (codigoMonitor, executor = pool) => {
  const normalizedCodigo = normalizeCondutorCodigo(codigoMonitor)

  if (!Number.isInteger(normalizedCodigo) || normalizedCodigo <= 0) {
    return null
  }

  const result = await executor.query(
    `SELECT
       ${monitorSelectClause}
     FROM monitor
     WHERE codigo = $1
     LIMIT 1`,
    [normalizedCodigo],
  )

  return result.rows[0] ?? null
}

const getNextMonitorCodigo = async (executor = pool) => {
  const result = await executor.query(
    `SELECT COALESCE(MAX(codigo), 0) + 1 AS next_codigo
     FROM monitor`,
  )

  return Number(result.rows[0]?.next_codigo || 1)
}

const findVeiculoByCrm = async (crm) => {
  const normalizedCrm = normalizeVehicleCrm(crm)

  if (!normalizedCrm) {
    return null
  }

  const result = await pool.query(
    `SELECT
       ${veiculoSelectClause}
     FROM veiculo
     WHERE UPPER(BTRIM(COALESCE(crm, ''))) = UPPER($1)
     ORDER BY codigo ASC
     LIMIT 1`,
    [normalizedCrm],
  )

  return result.rows[0] ?? null
}

const findTrocaByCodigoOrDescricao = async ({ codigo, descricao }) => {
  const normalizedCodigo = normalizeRequestValue(codigo)
  const normalizedDescricao = normalizeTrocaText(descricao, 255)
  const normalizedDescricaoKey = normalizeTrocaLookupKey(descricao)

  if (!normalizedCodigo && !normalizedDescricaoKey) {
    return null
  }

  if (normalizedCodigo) {
    const result = await pool.query(
      `SELECT ${trocaSelectClause}
       FROM tipo_troca
       WHERE CAST(codigo AS text) = $1
       ORDER BY codigo ASC
       LIMIT 1`,
      [normalizedCodigo],
    )

    if (result.rows[0]) {
      return result.rows[0]
    }
  }

  if (!normalizedDescricaoKey) {
    return null
  }

  const result = await pool.query(
    `SELECT ${trocaSelectClause}
     FROM tipo_troca
     ORDER BY codigo ASC`,
  )

  return result.rows.find((item) => normalizeTrocaLookupKey(item.lista) === normalizedDescricaoKey) ?? null
}

const buildCredenciadaLegacyFields = ({ codigo, credenciado, representante, cnpjCpf }) => {
  const digits = normalizeRequestValue(cnpjCpf).replace(/\D/g, '')
  const empresa = (normalizeCredenciadaText(credenciado, 100) || `CRED ${codigo}`).slice(0, 100)
  const condutor = (normalizeCredenciadaText(representante, 100) || empresa).slice(0, 100)
  const tipoPessoa = digits.length === 14 ? 'PJ' : 'PF'
  const placa = `CR${codigo}`.slice(0, 8)

  return {
    placa,
    empresa,
    condutor,
    tipoPessoa,
  }
}

const normalizeVehicleCrm = (value) => {
  return normalizeRequestValue(value)
    .toUpperCase()
    .replace(/[^0-9A-Z.\-/]/g, '')
    .slice(0, 20)
}

const isVehicleCrmValid = (value) => {
  return /^[0-9A-Z.\-/]{1,20}$/.test(value)
}

const normalizeVehiclePlaca = (value) => {
  return normalizeRequestValue(value)
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, '')
    .slice(0, 7)
}

const isVehiclePlacaValid = (value) => {
  return /^[A-Z]{3}-?\d{4}$/.test(value) || /^[A-Z]{3}-?\d[A-Z]\d{2}$/.test(value)
}

const getVeiculoPersistenceError = (error, fallbackMessage) => {
  if (error && typeof error === 'object') {
    const databaseError = error
    const constraintName = typeof databaseError.constraint === 'string' ? databaseError.constraint : ''
    const errorCode = typeof databaseError.code === 'string' ? databaseError.code : ''
    const errorMessage = error instanceof Error ? error.message : ''

    const isUniqueViolation = errorCode === '23505' || /viol[ao].+unicidade|duplicate key/i.test(errorMessage)

    if (constraintName === 'veiculo_crm_uk' || (isUniqueViolation && errorMessage.includes('veiculo_crm_uk'))) {
      return {
        status: 409,
        message: 'CRM j\u00e1 cadastrado',
      }
    }

    if (constraintName === 'veiculo_placa_uk' || (isUniqueViolation && errorMessage.includes('veiculo_placa_uk'))) {
      return {
        status: 409,
        message: 'Placa j\u00e1 cadastrada',
      }
    }

    if (constraintName === 'veiculo_crm_placa_uk' || (isUniqueViolation && errorMessage.includes('veiculo_crm_placa_uk'))) {
      return {
        status: 409,
        message: 'CRM e placa j\u00e1 cadastrado',
      }
    }
  }

  return {
    status: 500,
    message: error instanceof Error ? error.message : fallbackMessage,
  }
}

const getOrdemServicoPersistenceError = (error, fallbackMessage) => {
  if (error && typeof error === 'object') {
    const databaseError = error
    const constraintName = typeof databaseError.constraint === 'string' ? databaseError.constraint : ''
    const errorCode = typeof databaseError.code === 'string' ? databaseError.code : ''
    const errorMessage = error instanceof Error ? error.message : ''

    const isUniqueViolation = errorCode === '23505' || /viol[ao].+unicidade|duplicate key/i.test(errorMessage)

    if (
      constraintName === 'ordem_servico_chave_composta_unique_idx'
      || (isUniqueViolation && errorMessage.includes('ordem_servico_chave_composta_unique_idx'))
    ) {
      return {
        status: 409,
        message: 'J� existe uma Ordem de Servi�o com o mesmo termo, num OS e revis�o.',
      }
    }
  }

  return {
    status: 500,
    message: error instanceof Error ? error.message : fallbackMessage,
  }
}

const normalizeVehicleInteger = (value, maxDigits = 5) => {
  const digits = normalizeRequestValue(value).replace(/\D/g, '').slice(0, maxDigits)

  if (!digits) {
    return null
  }

  const parsed = Number(digits)
  return Number.isInteger(parsed) ? parsed : Number.NaN
}

const normalizeVehicleMoney = (value) => {
  const normalizedValue = normalizeRequestValue(value)
    .replace(/\s+/g, '')
    .replace(/\.(?=\d{3}(?:\D|$))/g, '')
    .replace(',', '.')

  if (!normalizedValue) {
    return null
  }

  const parsed = Number(normalizedValue)
  return Number.isFinite(parsed) ? Number(parsed.toFixed(2)) : Number.NaN
}

const normalizeTipoDeVeiculo = (value) => {
  const normalizedValue = normalizeRequestValue(value)

  if (!normalizedValue) {
    return ''
  }

  const normalizedKey = normalizedValue
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z]/g, '')

  if (normalizedKey === 'onibus') {
    return '\u00d4nibus'
  }

  if (normalizedKey === 'microonibus') {
    return 'Micro-\u00d4nibus'
  }

  return null
}

const normalizeTipoDeBancada = (value) => {
  const normalizedValue = normalizeRequestValue(value)

  if (!normalizedValue) {
    return ''
  }

  const normalizedKey = normalizedValue
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z]/g, '')

  if (normalizedKey === 'convencional') {
    return 'Convencional'
  }

  if (normalizedKey === 'creche') {
    return 'Creche'
  }

  if (normalizedKey === 'acessivel') {
    return 'Acess\u00edvel'
  }

  return null
}

const normalizeOsEspecial = (value) => {
  const normalizedValue = normalizeRequestValue(value)

  if (!normalizedValue) {
    return ''
  }

  const normalizedKey = normalizedValue
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z]/g, '')

  if (normalizedKey === 'sim') {
    return 'Sim'
  }

  if (normalizedKey === 'nao') {
    return 'N\u00e3o'
  }

  return null
}

const normalizeXmlDateInput = (value) => {
  const normalizedValue = normalizeRequestValue(value)

  if (!normalizedValue) {
    return ''
  }

  const match = normalizedValue.match(/^(\d{4}-\d{2}-\d{2})/)
  return match ? match[1] : ''
}

const parseCondutorXml = (xmlContent) => {
  const parsed = xmlParser.parse(xmlContent)
  const rawRecords = parsed?.dataroot?.Condutor
  const records = (Array.isArray(rawRecords)
    ? rawRecords
    : rawRecords
      ? [rawRecords]
      : [])
    .filter((record) => record && typeof record === 'object')

  return records.map((record) => {
    const validadeCrmc = normalizeXmlDateInput(record?.VAL_CRMC)
    const validadeCurso = normalizeXmlDateInput(record?.VCurso_condutor)
      || normalizeXmlDateInput(record?.Curso_condutor)
      || validadeCrmc

    return {
      codigo: normalizeRequestValue(record?.['C\u00f3digo']),
      condutor: normalizeRequestValue(record?.Condutor),
      cpfCondutor: normalizeRequestValue(record?.CPF_condutor),
      crmc: normalizeRequestValue(record?.CRMC),
      validadeCrmc,
      validadeCurso,
      tipoVinculo: normalizeRequestValue(record?.Tipo_de_vinculo),
      historico: normalizeRequestValue(record?.Historico),
    }
  })
}

const parseVinculoCondutorXml = (xmlContent) => {
  const parsed = xmlParser.parse(xmlContent)
  const rawRecords = parsed?.dataroot?.Vinculos_condutor
  const records = (Array.isArray(rawRecords)
    ? rawRecords
    : rawRecords
      ? [rawRecords]
      : [])
    .filter((record) => record && typeof record === 'object')

  return records.map((record) => ({
    codigoXml: normalizeRequestValue(record?.['C\u00f3digo']),
    empregador: normalizeRequestValue(record?.Empregador),
    dataOs: normalizeXmlDateInput(record?.Data_de_OS),
    admissao: normalizeXmlDateInput(record?.Admissao),
    cpfCondutor: normalizeRequestValue(record?.CPF_condutor),
  }))
}

const parseVinculoMonitorXml = (xmlContent) => {
  const parsed = xmlParser.parse(xmlContent)
  const rawRecords = parsed?.dataroot?.Vinculos_monitor
  const records = (Array.isArray(rawRecords)
    ? rawRecords
    : rawRecords
      ? [rawRecords]
      : [])
    .filter((record) => record && typeof record === 'object')

  return records.map((record) => ({
    codigoXml: normalizeRequestValue(record?.['C\u00f3digo']),
    empregador: normalizeRequestValue(record?.Empregador),
    dataOs: normalizeXmlDateInput(record?.Data_de_OS),
    admissao: normalizeXmlDateInput(record?.Admissao),
    cpfMonitor: normalizeRequestValue(record?.CPF_monitor),
  }))
}

const parseMonitorXml = (xmlContent) => {
  const parsed = xmlParser.parse(xmlContent)
  const rawRecords = parsed?.dataroot?.Monitor
  const records = (Array.isArray(rawRecords)
    ? rawRecords
    : rawRecords
      ? [rawRecords]
      : [])
    .filter((record) => record && typeof record === 'object')

  return records.map((record) => ({
    codigo: normalizeRequestValue(record?.['C\u00f3digo']),
    monitor: normalizeRequestValue(record?.Monitor),
    rgMonitor: normalizeRequestValue(record?.RG_monitor),
    cpfMonitor: normalizeRequestValue(record?.CPF_monitor),
    cursoMonitor: normalizeXmlDateInput(record?.Curso_monitor),
    validadeCurso: normalizeXmlDateInput(record?.VCurso_monitor),
    tipoVinculo: normalizeRequestValue(record?.Tipo_de_vinculo),
    nascimento: normalizeXmlDateInput(record?.Nascimento),
  }))
}

const parseVeiculoXml = (xmlContent) => {
  const parsed = xmlParser.parse(xmlContent)
  const rawRecords = parsed?.dataroot?.Veiculo
  const records = (Array.isArray(rawRecords)
    ? rawRecords
    : rawRecords
      ? [rawRecords]
      : [])
    .filter((record) => record && typeof record === 'object')

  return records.map((record) => ({
    codigo: normalizeRequestValue(record?.['C\u00f3digo']),
    crm: normalizeRequestValue(record?.CRM),
    placas: normalizeRequestValue(record?.Placas),
    ano: normalizeRequestValue(record?.Ano),
    capDetran: normalizeRequestValue(record?.Cap_DETRAN),
    capTeg: normalizeRequestValue(record?.Cap_TEG),
    capTegCreche: normalizeRequestValue(record?.Cap_TEG_CRECHE),
    capAcessivel: normalizeRequestValue(record?.Cap_ACESSIVEL),
    valCrm: normalizeXmlDateInput(record?.VAL_CRM),
    seguradora: normalizeRequestValue(record?.Seguradora),
    seguroInicio: normalizeXmlDateInput(record?.Seguro_inicio),
    seguroTermino: normalizeXmlDateInput(record?.Seguro_termino),
    tipoDeBancada: normalizeRequestValue(record?.Tipo_de_bancada),
    tipoDeVeiculo: normalizeRequestValue(record?.Tipo_de_veiculo),
    marcaModelo: normalizeRequestValue(record?.Marca_modelo),
    titular: normalizeRequestValue(record?.Titular),
    cnpjCpf: normalizeRequestValue(record?.CNPJ_CPF),
    valorVeiculo: normalizeRequestValue(record?.Valor_veiculo),
    osEspecial: normalizeRequestValue(record?.OS_especial),
  }))
}

const getSpreadsheetCellText = (cell) => {
  const data = cell?.Data

  if (data === null || data === undefined) {
    return ''
  }

  if (typeof data === 'string' || typeof data === 'number' || typeof data === 'boolean') {
    return normalizeRequestValue(data)
  }

  if (typeof data === 'object') {
    return normalizeRequestValue(data['#text'])
  }

  return ''
}

const getSpreadsheetRowValues = (row) => {
  const rawCells = Array.isArray(row?.Cell)
    ? row.Cell
    : row?.Cell
      ? [row.Cell]
      : []
  const values = []
  let currentColumnIndex = 0

  for (const cell of rawCells) {
    const explicitColumnIndex = Number(cell?.['@_ss:Index'])

    if (Number.isInteger(explicitColumnIndex) && explicitColumnIndex > 0) {
      currentColumnIndex = explicitColumnIndex - 1
    }

    values[currentColumnIndex] = getSpreadsheetCellText(cell)
    currentColumnIndex += 1
  }

  return values.map((value) => normalizeRequestValue(value))
}

const parseTitularXml = (xmlContent) => {
  const parsed = xmlParser.parse(xmlContent)
  const worksheets = Array.isArray(parsed?.Workbook?.Worksheet)
    ? parsed.Workbook.Worksheet
    : parsed?.Workbook?.Worksheet
      ? [parsed.Workbook.Worksheet]
      : []
  const table = worksheets[0]?.Table
  const rows = Array.isArray(table?.Row)
    ? table.Row
    : table?.Row
      ? [table.Row]
      : []

  return rows
    .map((row) => getSpreadsheetRowValues(row))
    .filter((values) => values.some(Boolean))
    .filter((values) => values[0]?.toUpperCase() !== 'CODIGO')
    .map((values) => ({
      codigo: values[0] ?? '',
      cnpjCpf: values[1] ?? '',
      titular: values[2] ?? '',
    }))
}

const normalizeImportedMonitorRecord = (record, index) => {
  const codigo = normalizeCondutorCodigo(record.codigo)
  const monitor = normalizeCondutorName(record.monitor)
  const rgMonitor = normalizeMonitorRg(record.rgMonitor)
  const cpfMonitor = normalizeCpf(record.cpfMonitor)
  const cursoMonitor = normalizeXmlDateInput(record.cursoMonitor)
  const validadeCurso = normalizeXmlDateInput(record.validadeCurso)
  const tipoVinculo = normalizeTipoVinculo(record.tipoVinculo)
  const nascimento = normalizeXmlDateInput(record.nascimento)
  const itemLabel = `Registro ${index + 1}`

  if (codigo === null || Number.isNaN(codigo)) {
    throw new Error(`${itemLabel}: codigo invalido no XML.`)
  }

  if (!monitor || !isMonitorNameValid(monitor)) {
    throw new Error(`${itemLabel}: nome do monitor invalido no XML.`)
  }

  if (!isCpfValid(cpfMonitor)) {
    throw new Error(`${itemLabel}: CPF invalido no XML.`)
  }

  if (rgMonitor && !isMonitorRgValid(rgMonitor)) {
    throw new Error(`${itemLabel}: RG invalido no XML.`)
  }

  if (validadeCurso && !isDateInputValid(validadeCurso)) {
    throw new Error(`${itemLabel}: validade do curso invalida no XML.`)
  }

  if (nascimento && !isDateInputValid(nascimento)) {
    throw new Error(`${itemLabel}: data de nascimento invalida no XML.`)
  }

  if (nascimento && !isDateBeforeToday(nascimento)) {
    throw new Error(`${itemLabel}: data de nascimento deve ser anterior ao dia da inclusao.`)
  }

  if (tipoVinculo === null) {
    throw new Error(`${itemLabel}: tipo de vinculo invalido no XML.`)
  }

  return {
    codigo,
    monitor,
    rgMonitor,
    cpfMonitor,
    cursoMonitor,
    validadeCurso,
    tipoVinculo,
    nascimento,
  }
}

const normalizeImportedVeiculoRecord = (record, index) => {
  const codigo = normalizeCondutorCodigo(record.codigo)
  const crm = normalizeVehicleCrm(record.crm)
  const placas = normalizeVehiclePlaca(record.placas)
  const ano = normalizeVehicleInteger(record.ano, 4)
  const capDetran = normalizeVehicleInteger(record.capDetran, 3)
  const capTeg = normalizeVehicleInteger(record.capTeg, 3)
  const capTegCreche = normalizeVehicleInteger(record.capTegCreche, 3)
  const capAcessivel = normalizeVehicleInteger(record.capAcessivel, 3)
  const valCrm = normalizeXmlDateInput(record.valCrm)
  const seguradora = normalizeCredenciadaText(record.seguradora, 255)
  const seguroInicio = normalizeXmlDateInput(record.seguroInicio)
  const seguroTermino = normalizeXmlDateInput(record.seguroTermino)
  const tipoDeBancada = normalizeTipoDeBancada(record.tipoDeBancada)
  const tipoDeVeiculo = normalizeTipoDeVeiculo(record.tipoDeVeiculo)
  const marcaModelo = normalizeCredenciadaText(record.marcaModelo, 255)
  const titular = normalizeCredenciadaText(record.titular, 255)
  const cnpjCpf = normalizeCnpjCpf(record.cnpjCpf)
  const valorVeiculo = normalizeVehicleMoney(record.valorVeiculo)
  const osEspecial = normalizeOsEspecial(record.osEspecial)
  const itemLabel = `Registro ${index + 1}`

  if (codigo === null || Number.isNaN(codigo)) {
    throw new Error(`${itemLabel}: codigo invalido no XML.`)
  }

  if (crm && !isVehicleCrmValid(crm)) {
    throw new Error(`${itemLabel}: CRM invalido no XML.`)
  }

  if (placas && !isVehiclePlacaValid(placas)) {
    throw new Error(`${itemLabel}: placa invalida no XML.`)
  }

  if (ano !== null && Number.isNaN(ano)) {
    throw new Error(`${itemLabel}: ano invalido no XML.`)
  }

  if (capDetran !== null && Number.isNaN(capDetran)) {
    throw new Error(`${itemLabel}: capacidade DETRAN invalida no XML.`)
  }

  if (capTeg !== null && Number.isNaN(capTeg)) {
    throw new Error(`${itemLabel}: capacidade TEG invalida no XML.`)
  }

  if (capTegCreche !== null && Number.isNaN(capTegCreche)) {
    throw new Error(`${itemLabel}: capacidade TEG creche invalida no XML.`)
  }

  if (capAcessivel !== null && Number.isNaN(capAcessivel)) {
    throw new Error(`${itemLabel}: capacidade acessivel invalida no XML.`)
  }

  if (valCrm && !isDateInputValid(valCrm)) {
    throw new Error(`${itemLabel}: validade do CRM invalida no XML.`)
  }

  if (seguroInicio && !isDateInputValid(seguroInicio)) {
    throw new Error(`${itemLabel}: inicio do seguro invalido no XML.`)
  }

  if (seguroTermino && !isDateInputValid(seguroTermino)) {
    throw new Error(`${itemLabel}: termino do seguro invalido no XML.`)
  }

  if (seguroInicio && seguroTermino && seguroTermino < seguroInicio) {
    throw new Error(`${itemLabel}: termino do seguro deve ser maior ou igual ao inicio no XML.`)
  }

  if (tipoDeBancada === null) {
    throw new Error(`${itemLabel}: tipo de bancada invalido no XML.`)
  }

  if (tipoDeVeiculo === null) {
    throw new Error(`${itemLabel}: tipo de veiculo invalido no XML.`)
  }

  if (cnpjCpf && !isCnpjCpfValid(cnpjCpf)) {
    throw new Error(`${itemLabel}: CNPJ/CPF invalido no XML.`)
  }

  if (valorVeiculo !== null && Number.isNaN(valorVeiculo)) {
    throw new Error(`${itemLabel}: valor do veiculo invalido no XML.`)
  }

  if (osEspecial === null) {
    throw new Error(`${itemLabel}: OS especial invalido no XML.`)
  }

  return {
    codigo,
    crm,
    placas,
    ano,
    capDetran,
    capTeg,
    capTegCreche,
    capAcessivel,
    valCrm,
    seguradora,
    seguroInicio,
    seguroTermino,
    tipoDeBancada,
    tipoDeVeiculo,
    marcaModelo,
    titular,
    cnpjCpf,
    valorVeiculo,
    osEspecial,
  }
}

const normalizeImportedTitularRecord = (record, index) => {
  const codigo = normalizeCondutorCodigo(record.codigo)
  const cnpjCpf = normalizeTitularDocument(record.cnpjCpf)
  const titular = normalizeCredenciadaText(record.titular, 255)
  const itemLabel = `Registro ${index + 1}`

  if (codigo === null || Number.isNaN(codigo)) {
    throw new Error(`${itemLabel}: codigo invalido no XML.`)
  }

  if (!cnpjCpf) {
    throw new Error(`${itemLabel}: CNPJ/CPF invalido no XML.`)
  }

  if (!titular) {
    throw new Error(`${itemLabel}: titular do CRM invalido no XML.`)
  }

  return {
    codigo,
    cnpjCpf,
    titular,
  }
}

const normalizeImportedCondutorRecord = (record, index) => {
  const codigo = normalizeCondutorCodigo(record.codigo)
  const condutor = normalizeCondutorName(record.condutor)
  const cpfCondutor = normalizeCpf(record.cpfCondutor)
  const crmc = normalizeCrmc(record.crmc)
  const validadeCrmc = normalizeXmlDateInput(record.validadeCrmc)
  const validadeCurso = normalizeXmlDateInput(record.validadeCurso)
  const tipoVinculo = normalizeTipoVinculo(record.tipoVinculo)
  const historico = normalizeHistorico(record.historico)
  const itemLabel = `Registro ${index + 1}`

  if (codigo === null || Number.isNaN(codigo)) {
    throw new Error(`${itemLabel}: codigo invalido no XML.`)
  }

  if (!condutor || !isCondutorNameValid(condutor)) {
    throw new Error(`${itemLabel}: nome do condutor invalido no XML.`)
  }

  if (!isCpfValid(cpfCondutor)) {
    throw new Error(`${itemLabel}: CPF invalido no XML.`)
  }

  if (tipoVinculo === null) {
    throw new Error(`${itemLabel}: tipo de vinculo invalido no XML.`)
  }

  return {
    codigo,
    condutor,
    cpfCondutor,
    crmc,
    validadeCrmc,
    validadeCurso,
    tipoVinculo,
    historico,
  }
}

const parseCredenciadaXml = (xmlContent) => {
  const parsed = xmlParser.parse(xmlContent)
  const rawRecords = parsed?.dataroot?.Credenciados
  const records = (Array.isArray(rawRecords)
    ? rawRecords
    : rawRecords
      ? [rawRecords]
      : [])
    .filter((record) => record && typeof record === 'object')

  return records.map((record) => ({
    codigo: normalizeRequestValue(record?.['C\u00f3digo']),
    credenciado: normalizeRequestValue(record?.Credenciado),
    cnpjCpf: normalizeRequestValue(record?.CNPJ_CPF),
    logradouro: normalizeRequestValue(record?.Logradouro),
    bairro: normalizeRequestValue(record?.Bairro),
    cep: normalizeRequestValue(record?.CEP),
    municipio: normalizeRequestValue(record?.Municipio),
    email: normalizeRequestValue(record?.Email),
    telefone1: normalizeRequestValue(record?.Telefone_01),
    telefone2: normalizeRequestValue(record?.Telefone_02),
    representante: normalizeRequestValue(record?.Representante),
    cpfRepresentante: normalizeRequestValue(record?.CPF_representante),
    status: normalizeRequestValue(record?.Status),
  }))
}

const parseOrdemServicoXml = (xmlContent) => {
  const parsed = xmlParser.parse(xmlContent)
  const rawRecords = parsed?.dataroot?.OrdemServico
  const records = (Array.isArray(rawRecords)
    ? rawRecords
    : rawRecords
      ? [rawRecords]
      : [])
    .filter((record) => record && typeof record === 'object')

  return records.map((record) => ({
    codigoAccess: normalizeRequestValue(record?.['C\u00f3digo']),
    termoAdesao: normalizeRequestValue(record?.Termo_de_adesao),
    osXml: normalizeRequestValue(record?.OS),
    numOs: extractOrdemServicoNumeroFromOs(record?.OS),
    revisao: extractOrdemServicoRevisionFromOs(record?.OS),
    vigenciaOs: normalizeXmlDateInput(record?.Vigencia_da_OS),
    credenciado: normalizeRequestValue(record?.Credenciado),
    dreCodigo: normalizeRequestValue(record?.DRE),
    dreDescricao: normalizeRequestValue(record?.DRE_ext),
    cpfCondutor: normalizeRequestValue(record?.CPF_condutor),
    crm: normalizeRequestValue(record?.CRM),
    cpfMonitor: normalizeRequestValue(record?.CPF_monitor),
    anotacao: normalizeRequestValue(record?.['Anota\u00e7\u00e3o']),
    situacao: normalizeRequestValue(record?.Situacao_de_OS),
    tipoTrocaDescricao: normalizeRequestValue(record?.Campo_de_Troca),
    prepostoInicio: normalizeXmlDateInput(record?.Preposto),
    prepostoDias: normalizeRequestValue(record?.Preposto_dias),
    conexao: normalizeRequestValue(record?.Conexao),
    dataEncerramento: normalizeXmlDateInput(record?.Data_de_encerramento),
    buscaVeiculo: normalizeRequestValue(record?.Busca_veiculo),
    cnpjCpf: normalizeRequestValue(record?.CNPJ_CPF),
    uniaoTermos: normalizeRequestValue(record?.Uniao_de_termos),
  }))
}

const parseTrocaXml = (xmlContent) => {
  const parsed = xmlParser.parse(xmlContent)
  const rawRecords = parsed?.dataroot?.Listagem_x0020_de_x0020_Trocas
  const records = (Array.isArray(rawRecords)
    ? rawRecords
    : rawRecords
      ? [rawRecords]
      : [])
    .filter((record) => record && typeof record === 'object')

  return records.map((record) => ({
    codigo: normalizeRequestValue(record?.['C\u00f3digo']),
    controle: normalizeRequestValue(record?.Controle),
    lista: normalizeRequestValue(record?.Lista),
  }))
}

const parseSeguradoraXml = (xmlContent) => {
  const parsed = xmlParser.parse(xmlContent)
  const rawRecords = parsed?.dataroot?.Seguradoras
  const records = (Array.isArray(rawRecords)
    ? rawRecords
    : rawRecords
      ? [rawRecords]
      : [])
    .filter((record) => record && typeof record === 'object')

  return records.map((record) => ({
    codigo: normalizeRequestValue(record?.['C\u00f3digo']),
    controle: normalizeRequestValue(record?.Controle),
    lista: normalizeRequestValue(record?.Lista),
  }))
}

const parseMarcaModeloXml = (xmlContent) => {
  const parsed = xmlParser.parse(xmlContent)
  const rawRecords = parsed?.dataroot?.['marca-modelo']
  const records = (Array.isArray(rawRecords)
    ? rawRecords
    : rawRecords
      ? [rawRecords]
      : [])
    .filter((record) => record && typeof record === 'object')

  return records
    .map((record) => ({
      descricao: normalizeRequestValue(record?.marca_modelo),
    }))
    .filter((record) => record.descricao)
}

const normalizeImportedCredenciadaRecord = (record, index) => {
  const codigo = normalizeCondutorCodigo(record.codigo)
  const credenciado = normalizeCredenciadaText(record.credenciado, 255)
  const cnpjCpf = normalizeCnpjCpf(record.cnpjCpf)
  const logradouro = normalizeCredenciadaText(record.logradouro, 255)
  const bairro = normalizeCredenciadaText(record.bairro, 120)
  const cep = normalizeCep(record.cep)
  const municipio = normalizeCredenciadaText(record.municipio, 120)
  const email = normalizeEmailList(record.email)
  const telefone1 = normalizePhoneNumber(record.telefone1)
  const telefone2 = normalizePhoneNumber(record.telefone2)
  const representante = normalizeCredenciadaText(record.representante, 255)
  const cpfRepresentante = normalizeCpf(record.cpfRepresentante)
  const rgRepresentante = normalizeCredenciadaText(record.rgRepresentante, 30)
  const status = normalizeCredenciadaStatusValue(record.status)
  const itemLabel = `Registro ${index + 1}`

  if (codigo === null || Number.isNaN(codigo)) {
    throw new Error(`${itemLabel}: codigo invalido no XML.`)
  }

  if (!credenciado) {
    throw new Error(`${itemLabel}: nome da credenciada invalido no XML.`)
  }

  if (!isCnpjCpfValid(cnpjCpf)) {
    throw new Error(`${itemLabel}: CNPJ/CPF invalido no XML.`)
  }

  if (cep && !isCepValid(cep)) {
    throw new Error(`${itemLabel}: CEP invalido no XML.`)
  }

  if (email && !isEmailListValid(email)) {
    throw new Error(`${itemLabel}: email invalido no XML.`)
  }

  if (telefone1 && !isPhoneNumberValid(telefone1)) {
    throw new Error(`${itemLabel}: telefone 1 invalido no XML.`)
  }

  if (telefone2 && !isPhoneNumberValid(telefone2)) {
    throw new Error(`${itemLabel}: telefone 2 invalido no XML.`)
  }

  if (cpfRepresentante && !isCpfValid(cpfRepresentante)) {
    throw new Error(`${itemLabel}: CPF do representante invalido no XML.`)
  }

  return {
    codigo,
    credenciado,
    cnpjCpf,
    logradouro,
    bairro,
    cep,
    municipio,
    email,
    telefone1,
    telefone2,
    representante,
    cpfRepresentante,
    rgRepresentante,
    status,
    ...buildCredenciadaLegacyFields({
      codigo,
      credenciado,
      representante,
      cnpjCpf,
    }),
  }
}

const normalizeDecimalValue = (value) => {
  const normalizedValue = normalizeRequestValue(value)

  if (!normalizedValue) {
    return null
  }

  const parsed = Number(normalizedValue.replace(/\./g, '').replace(',', '.'))
  return Number.isFinite(parsed) ? Number(parsed.toFixed(2)) : NaN
}

const currencyWordsUnits = ['', 'um', 'dois', 'três', 'quatro', 'cinco', 'seis', 'sete', 'oito', 'nove']
const currencyWordsTeens = ['dez', 'onze', 'doze', 'treze', 'quatorze', 'quinze', 'dezesseis', 'dezessete', 'dezoito', 'dezenove']
const currencyWordsTens = ['', '', 'vinte', 'trinta', 'quarenta', 'cinquenta', 'sessenta', 'setenta', 'oitenta', 'noventa']
const currencyWordsHundreds = ['', 'cento', 'duzentos', 'trezentos', 'quatrocentos', 'quinhentos', 'seiscentos', 'setecentos', 'oitocentos', 'novecentos']
const currencyWordsScales = [null, ['mil', 'mil'], ['milhão', 'milhões'], ['bilhão', 'bilhões'], ['trilhão', 'trilhões']]

const buildCurrencyWordsBelowOneThousand = (value) => {
  if (!Number.isInteger(value) || value < 0 || value > 999) {
    return ''
  }

  if (value === 0) {
    return ''
  }

  if (value === 100) {
    return 'cem'
  }

  const hundreds = Math.floor(value / 100)
  const remainder = value % 100
  const parts = []

  if (hundreds > 0) {
    parts.push(currencyWordsHundreds[hundreds])
  }

  if (remainder >= 10 && remainder < 20) {
    parts.push(currencyWordsTeens[remainder - 10])
  } else {
    const tens = Math.floor(remainder / 10)
    const units = remainder % 10

    if (tens > 0) {
      parts.push(currencyWordsTens[tens])
    }

    if (units > 0) {
      parts.push(currencyWordsUnits[units])
    }
  }

  return parts.join(' e ')
}

const joinCurrencyWordParts = (parts) => {
  if (parts.length === 0) {
    return ''
  }

  if (parts.length === 1) {
    return parts[0]
  }

  const lastPart = parts[parts.length - 1]
  return `${parts.slice(0, -1).join(', ')} e ${lastPart}`
}

const buildIntegerCurrencyWords = (value) => {
  if (!Number.isInteger(value) || value < 0) {
    return ''
  }

  if (value === 0) {
    return 'zero'
  }

  const chunkParts = []
  let remaining = value
  let scaleIndex = 0

  while (remaining > 0) {
    const chunkValue = remaining % 1000

    if (chunkValue > 0) {
      const chunkText = buildCurrencyWordsBelowOneThousand(chunkValue)

      if (scaleIndex === 0) {
        chunkParts.unshift(chunkText)
      } else if (scaleIndex === 1) {
        chunkParts.unshift(chunkValue === 1 ? 'mil' : `${chunkText} mil`)
      } else {
        const [singularLabel, pluralLabel] = currencyWordsScales[scaleIndex] ?? ['', '']
        chunkParts.unshift(`${chunkText} ${chunkValue === 1 ? singularLabel : pluralLabel}`.trim())
      }
    }

    remaining = Math.floor(remaining / 1000)
    scaleIndex += 1
  }

  return joinCurrencyWordParts(chunkParts)
}

const buildCurrencyExtenso = (value) => {
  const rawValue = normalizeRequestValue(value)
  const normalizedValue = /^-?\d+(?:\.\d+)?$/.test(rawValue)
    ? Number(rawValue)
    : normalizeDecimalValue(value)

  if (normalizedValue === null || Number.isNaN(normalizedValue)) {
    return ''
  }

  const absoluteValue = Math.abs(normalizedValue)
  let integerValue = Math.floor(absoluteValue)
  let centValue = Math.round((absoluteValue - integerValue) * 100)

  if (centValue === 100) {
    integerValue += 1
    centValue = 0
  }

  const integerText = `${buildIntegerCurrencyWords(integerValue)} ${integerValue === 1 ? 'real' : 'reais'}`

  if (centValue === 0) {
    return normalizedValue < 0 ? `menos ${integerText}` : integerText
  }

  const centText = `${buildIntegerCurrencyWords(centValue)} ${centValue === 1 ? 'centavo' : 'centavos'}`
  const fullText = `${integerText} e ${centText}`

  return normalizedValue < 0 ? `menos ${fullText}` : fullText
}

const normalizeIntegerValue = (value) => {
  const normalizedValue = normalizeRequestValue(value)

  if (!normalizedValue) {
    return null
  }

  if (!/^-?\d+$/.test(normalizedValue)) {
    return NaN
  }

  return Number(normalizedValue)
}

const normalizeBooleanInteger = (value) => {
  if (value === true) {
    return 1
  }

  if (value === false) {
    return 0
  }

  const normalizedValue = normalizeRequestValue(value).toUpperCase()

  if (!normalizedValue) {
    return null
  }

  if (['1', 'TRUE', 'SIM', 'S'].includes(normalizedValue)) {
    return 1
  }

  if (['0', 'FALSE', 'NAO', 'N�O', 'N'].includes(normalizedValue)) {
    return 0
  }

  return NaN
}

const parseCredenciamentoTermoXml = (xmlContent) => {
  const parsed = xmlParser.parse(xmlContent)
  const records = parsed?.dataroot?.Credenciamento_Termo

  if (!records) {
    return []
  }

  const normalizedRecords = Array.isArray(records) ? records : [records]
  return normalizedRecords.filter((record) => record && typeof record === 'object')
}

const buildCredenciamentoTermoBaseRecord = (record, index) => {
  const itemLabel = `Registro ${index + 1}`
  const codigoXml = normalizeCondutorCodigo(record['C�digo'])
  const termoAdesao = normalizeRequestValue(record.Termo_de_adesao).toUpperCase().slice(0, 255)
  const credenciado = normalizeCredenciadaText(record.Credenciado, 255)
  const cnpjCpf = normalizeCnpjCpf(record.CNPJ_CPF)
  const cpfRepresentante = normalizeCpf(record.CPF_representante)
  const valorContrato = normalizeDecimalValue(record.Valor_contrato)
  const valorContratoAtualizado = normalizeDecimalValue(record.Valor_contrato_atualizado)

  if (codigoXml === null || Number.isNaN(codigoXml)) {
    throw new Error(`${itemLabel}: codigo invalido no XML.`)
  }

  if (!termoAdesao) {
    throw new Error(`${itemLabel}: termo de adesao invalido no XML.`)
  }

  if (!credenciado) {
    throw new Error(`${itemLabel}: credenciado invalido no XML.`)
  }

  if (cnpjCpf && !isCnpjCpfValid(cnpjCpf)) {
    throw new Error(`${itemLabel}: CNPJ/CPF invalido no XML.`)
  }

  if (cpfRepresentante && !isCpfValid(cpfRepresentante)) {
    throw new Error(`${itemLabel}: CPF do representante invalido no XML.`)
  }

  if (Number.isNaN(valorContrato)) {
    throw new Error(`${itemLabel}: valor do contrato invalido no XML.`)
  }

  if (Number.isNaN(valorContratoAtualizado)) {
    throw new Error(`${itemLabel}: valor do contrato atualizado invalido no XML.`)
  }

  return {
    codigoXml,
    termoAdesao,
    sei: normalizeRequestValue(record.SEI).toUpperCase().slice(0, 255),
    credenciado,
    situacaoPublicacao: normalizeCredenciadaText(record.Situacao_de_publicacao, 100),
    situacaoEmissao: normalizeCredenciadaText(record.Situacao_de_emissao, 100),
    inicioVigenciaBase: normalizeXmlDateInput(record.Inicio_de_vigencia),
    terminoVigenciaBase: normalizeXmlDateInput(record.Termino_de_vigencia),
    statusTermo: normalizeCredenciadaText(record.Status_termo, 100),
    tipoTermo: normalizeCredenciadaText(record.Tipo_de_termo, 100),
    cnpjCpf,
    representante: normalizeCredenciadaText(record.Representante, 255),
    cpfRepresentante,
    rgRepresentante: normalizeCredenciadaText(record.RG_representante, 30),
    logradouro: normalizeCredenciadaText(record.Logradouro, 255),
    bairro: normalizeCredenciadaText(record.Bairro, 120),
    municipio: normalizeCredenciadaText(record.Municipio, 120),
    especificacaoSei: normalizeRequestValue(record.Especificacao_SEI).toUpperCase().slice(0, 255),
    valorContrato,
    objeto: normalizeRequestValue(record.Objeto).toUpperCase().slice(0, 1000),
    dataPublicacao: normalizeXmlDateInput(record.Data_de_publicacao),
    infoSei: normalizeRequestValue(record.Info_SEI).toUpperCase().slice(0, 100),
    valorContratoAtualizado,
    vencimentoGeral: normalizeXmlDateInput(record.Vencimento_geral),
    mesRenovacao: normalizeRequestValue(record.Mes_renovacao).toUpperCase().slice(0, 50),
    tpOptante: normalizeRequestValue(record.TpOptante).toUpperCase().slice(0, 20),
  }
}

const buildImportedCredenciamentoTermoRecords = (record, index) => {
  const baseRecord = buildCredenciamentoTermoBaseRecord(record, index)
  const aditivoRecords = [{
    ...baseRecord,
    aditivo: 0,
    inicioVigencia: baseRecord.inicioVigenciaBase,
    terminoVigencia: baseRecord.terminoVigenciaBase,
    compDataAditivo: '',
    statusAditivo: baseRecord.statusTermo,
    dataPubAditivo: baseRecord.dataPublicacao,
    checkAditivo: 1,
  }]

  for (let suffix = 2; suffix <= 5; suffix += 1) {
    const suffixStr = String(suffix).padStart(2, '0')
    const inicioVigencia = normalizeXmlDateInput(record[`Inicio_de_vigencia_aditivo${suffixStr}`])
    const terminoVigencia = normalizeXmlDateInput(record[`Termino_de_vigencia_aditivo${suffixStr}`])
    const compDataAditivo = normalizeXmlDateInput(record[`Comp_data_aditivo${suffixStr}`])
    const statusAditivo = normalizeCredenciadaText(record[`Status_aditivo${suffixStr}`], 100)
    const dataPubAditivo = normalizeXmlDateInput(record[`Data_pub_aditivo${suffixStr}`])
    const checkAditivo = normalizeBooleanInteger(record[`Check_aditivo${suffixStr}`])
    const aditivo = suffix - 1

    if (!inicioVigencia) {
      continue
    }

    if (Number.isNaN(checkAditivo)) {
      throw new Error(`Registro ${index + 1}: check do aditivo ${aditivo} invalido no XML.`)
    }

    aditivoRecords.push({
      ...baseRecord,
      aditivo,
      inicioVigencia,
      terminoVigencia,
      compDataAditivo,
      statusAditivo,
      dataPubAditivo,
      checkAditivo: checkAditivo ?? 0,
    })
  }

  return aditivoRecords
}

const normalizeImportedTrocaRecord = (record, index) => {
  const codigo = normalizeCondutorCodigo(record.codigo)
  const controle = normalizeCondutorCodigo(record.controle)
  const lista = normalizeTrocaText(record.lista, 255)
  const itemLabel = `Registro ${index + 1}`

  if (codigo === null || Number.isNaN(codigo)) {
    throw new Error(`${itemLabel}: codigo invalido no XML.`)
  }

  if (controle === null || Number.isNaN(controle)) {
    throw new Error(`${itemLabel}: controle invalido no XML.`)
  }

  if (!lista) {
    throw new Error(`${itemLabel}: descricao da troca invalida no XML.`)
  }

  return {
    codigo,
    controle,
    lista,
  }
}

const normalizeImportedSeguradoraRecord = (record, index) => {
  const codigo = normalizeCondutorCodigo(record.codigo)
  const controle = normalizeCondutorCodigo(record.controle)
  const lista = normalizeTrocaText(record.lista, 255)
  const itemLabel = `Registro ${index + 1}`

  if (codigo === null || Number.isNaN(codigo)) {
    throw new Error(`${itemLabel}: codigo invalido no XML.`)
  }

  if (controle === null || Number.isNaN(controle)) {
    throw new Error(`${itemLabel}: controle invalido no XML.`)
  }

  if (!lista) {
    throw new Error(`${itemLabel}: lista da seguradora invalida no XML.`)
  }

  return {
    codigo,
    controle,
    lista,
  }
}

const normalizeImportedMarcaModeloRecord = (record, index) => {
  const descricao = normalizeTrocaText(record.descricao, 255)
  const itemLabel = `Registro ${index + 1}`

  if (!descricao) {
    throw new Error(`${itemLabel}: descricao de marca/modelo invalida no XML.`)
  }

  return {
    descricao,
  }
}

const importOrdemServicoXmlFile = async (fileName) => {
  const sanitizedFileName = path.basename(normalizeRequestValue(fileName))

  if (!sanitizedFileName) {
    throw new Error('Nome do arquivo XML e obrigatorio.')
  }

  if (path.extname(sanitizedFileName).toLowerCase() !== '.xml') {
    throw new Error('Informe um arquivo XML valido.')
  }

  const resolvedPath = path.resolve(importXmlDirectory, sanitizedFileName)

  if (!resolvedPath.startsWith(importXmlDirectory)) {
    throw new Error('Arquivo XML invalido.')
  }

  const xmlContent = await readFile(resolvedPath, 'utf8')
  const parsedRecords = parseOrdemServicoXml(xmlContent)

  if (!parsedRecords.length) {
    throw new Error('Nenhum registro de OrdemServico foi encontrado no XML informado.')
  }

  const normalizedRecords = []
  const skippedRecords = []

  for (const [index, record] of parsedRecords.entries()) {
    try {
      await ensureDreOperationalEntry({
        codigo: record.dreCodigo,
        descricao: record.dreDescricao,
      })

      const validationResult = await validateOrdemServicoPayload({
        codigoAccess: record.codigoAccess,
        termoAdesao: record.termoAdesao,
        numOs: record.numOs,
        revisao: record.revisao,
        vigenciaOs: record.vigenciaOs,
        credenciado: record.credenciado,
        cnpjCpf: record.cnpjCpf,
        dreCodigo: record.dreCodigo,
        cpfCondutor: record.cpfCondutor,
        cpfPreposto: '',
        prepostoInicio: record.prepostoInicio,
        prepostoDias: record.prepostoDias,
        crm: record.crm,
        cpfMonitor: record.cpfMonitor,
        situacao: record.situacao,
        tipoTroca: record.tipoTrocaDescricao,
        conexao: record.conexao,
        dataEncerramento: record.dataEncerramento,
        anotacao: record.anotacao,
        uniaoTermos: record.uniaoTermos,
        importMode: true,
      })

      if (validationResult.status !== 200) {
        throw new Error(validationResult.payload.message)
      }

      normalizedRecords.push(validationResult.payload)
    } catch (error) {
      skippedRecords.push({
        index: index + 1,
        codigoAccess: normalizeRequestValue(record.codigoAccess),
        codigo_access: normalizeRequestValue(record.codigoAccess),
        codigoXml: normalizeRequestValue(record.codigoAccess),
        osXml: normalizeRequestValue(record.osXml),
        numOsXml: normalizeRequestValue(record.numOs),
        num_os_xml: normalizeRequestValue(record.numOs),
        credenciadoXml: normalizeRequestValue(record.credenciado),
        dreXml: normalizeRequestValue(record.dreCodigo),
        cpfCondutorXml: normalizeRequestValue(record.cpfCondutor),
        cpfMonitorXml: normalizeRequestValue(record.cpfMonitor),
        crmXml: normalizeRequestValue(record.crm),
        message: error instanceof Error ? error.message : `Registro ${index + 1}: erro ao validar o XML.`,
      })
    }
  }

  const client = await pool.connect()

  try {
    await client.query('BEGIN')
    await client.query(`TRUNCATE TABLE ${ordemServicoImportRecusaTableName} RESTART IDENTITY`)
    let inserted = 0
    let updated = 0

    for (const skippedRecord of skippedRecords) {
      await client.query(
        `INSERT INTO ${ordemServicoImportRecusaTableName} (
           arquivo_xml,
           linha_xml,
           codigo_xml,
           os_xml,
           num_os_xml,
           credenciado_xml,
           dre_xml,
           cpf_condutor_xml,
           cpf_monitor_xml,
           crm_xml,
           motivo_recusa,
           data_importacao
         )
         VALUES ($1, $2, NULLIF($3, ''), NULLIF($4, ''), NULLIF($5, ''), NULLIF($6, ''), NULLIF($7, ''), NULLIF($8, ''), NULLIF($9, ''), NULLIF($10, ''), $11, NOW())`,
        [
          sanitizedFileName,
          skippedRecord.index,
          skippedRecord.codigoXml,
          skippedRecord.osXml,
          skippedRecord.numOsXml,
          skippedRecord.credenciadoXml,
          skippedRecord.dreXml,
          skippedRecord.cpfCondutorXml,
          skippedRecord.cpfMonitorXml,
          skippedRecord.crmXml,
          skippedRecord.message,
        ],
      )
    }

    for (const record of normalizedRecords) {
      const existingResult = await client.query(
        `SELECT codigo
         FROM ${ordemServicoTableName}
         WHERE UPPER(BTRIM(COALESCE(termo_adesao, ''))) = UPPER($1)
           AND UPPER(BTRIM(COALESCE(num_os, ''))) = UPPER($2)
           AND UPPER(BTRIM(COALESCE(revisao, ''))) = UPPER($3)
         LIMIT 1`,
        [record.termoAdesao, record.numOs, record.revisao],
      )

      if (existingResult.rowCount > 0) {
        await client.query(
          `UPDATE ${ordemServicoTableName}
           SET codigo_access = NULLIF($1, ''),
               termo_adesao = NULLIF($2, ''),
               num_os = NULLIF($3, ''),
               revisao = NULLIF($4, ''),
               vigencia_os = NULLIF($5, '')::date,
               termo_codigo = $6,
               dre_codigo = $7,
               dre_descricao = $8,
               modalidade_codigo = $9,
               modalidade_descricao = NULLIF($10, ''),
               cpf_condutor = $11,
               condutor = $12,
               cpf_preposto = NULLIF($13, ''),
               preposto_condutor = NULLIF($14, ''),
               preposto_inicio = NULLIF($15, '')::date,
               preposto_dias = $16,
               crm = $17,
               veiculo_placas = NULLIF($18, ''),
               cpf_monitor = NULLIF($19, ''),
               monitor = NULLIF($20, ''),
               situacao = $21,
               tipo_troca_codigo = $22,
               tipo_troca_descricao = NULLIF($23, ''),
               conexao = NULLIF($24, ''),
               data_encerramento = NULLIF($25, '')::date,
               anotacao = NULLIF($26, ''),
               uniao_termos = NULLIF($27, ''),
               data_modificacao = NOW()
               WHERE codigo = $28`,
          [
            record.codigoAccess,
            record.termoAdesao,
            record.numOs,
            record.revisao,
            record.vigenciaOs,
            record.termoCodigo,
            record.dreCodigo,
            record.dreDescricao,
                record.modalidadeCodigo,
                record.modalidadeDescricao,
            record.cpfCondutor,
            record.condutor,
            record.cpfPreposto,
            record.prepostoCondutor,
            record.prepostoInicio,
            record.prepostoDias,
            record.crm,
            record.veiculoPlacas,
            record.cpfMonitor,
            record.monitor,
            record.situacao,
            record.tipoTrocaCodigo,
            record.tipoTrocaDescricao,
            record.conexao,
            record.dataEncerramento,
            record.anotacao,
            record.uniaoTermos,
            existingResult.rows[0].codigo,
          ],
        )
        updated += 1
        continue
      }

       await client.query(
        `INSERT INTO ${ordemServicoTableName} (
           codigo_access,
           termo_adesao,
             num_os,
           revisao,
           os_concat,
           vigencia_os,
           termo_codigo,
           dre_codigo,
           dre_descricao,
           modalidade_codigo,
           modalidade_descricao,
           cpf_condutor,
           condutor,
           cpf_preposto,
           preposto_condutor,
           preposto_inicio,
           preposto_dias,
           crm,
           veiculo_placas,
           cpf_monitor,
           monitor,
           situacao,
           tipo_troca_codigo,
           tipo_troca_descricao,
           conexao,
           data_encerramento,
           anotacao,
           uniao_termos,
           data_inclusao,
           data_modificacao
         )
         VALUES (NULLIF($1, ''), NULLIF($2, ''), NULLIF($3, ''), NULLIF($4, ''), NULLIF($5, ''), NULLIF($6, '')::date, $7, $8, $9, $10, NULLIF($11, ''), $12, $13, NULLIF($14, ''), NULLIF($15, ''), NULLIF($16, '')::date, $17, $18, NULLIF($19, ''), NULLIF($20, ''), NULLIF($21, ''), $22, $23, NULLIF($24, ''), NULLIF($25, ''), NULLIF($26, '')::date, NULLIF($27, ''), NULLIF($28, ''), NOW(), NOW())`,
        [
          record.codigoAccess,
          record.termoAdesao,
          record.numOs,
          record.revisao,
          record.osConcat,
          record.vigenciaOs,
          record.termoCodigo,
          record.dreCodigo,
          record.dreDescricao,
          record.modalidadeCodigo,
          record.modalidadeDescricao,
          record.cpfCondutor,
          record.condutor,
          record.cpfPreposto,
          record.prepostoCondutor,
          record.prepostoInicio,
          record.prepostoDias,
          record.crm,
          record.veiculoPlacas,
          record.cpfMonitor,
          record.monitor,
          record.situacao,
          record.tipoTrocaCodigo,
          record.tipoTrocaDescricao,
          record.conexao,
          record.dataEncerramento,
          record.anotacao,
          record.uniaoTermos,
        ],
      )
      inserted += 1
    }

    if (normalizedRecords.length > 0) {
      await rebalanceOrdemServicoRevisions(client)
      await syncCondutorVinculosFromOrdemServico(client)
      await syncMonitorVinculosFromOrdemServico(client)
    }

    if (normalizedRecords.length) {
      await client.query(`SELECT setval('${ordemServicoCodigoSequenceName}', GREATEST(COALESCE((SELECT MAX(codigo) FROM ${ordemServicoTableName}), 0), 1), true)`)
    }

    await client.query('COMMIT')

    return {
      fileName: sanitizedFileName,
      filePath: resolvedPath,
      total: parsedRecords.length,
      processed: normalizedRecords.length,
      inserted,
      updated,
      skipped: skippedRecords.length,
      skippedRecords: skippedRecords.slice(0, 20),
    }
  } catch (error) {
    await client.query('ROLLBACK')
    throw error
  } finally {
    client.release()
  }
}

const condutorSelectClause = `
  codigo::text AS codigo,
  BTRIM(condutor) AS condutor,
  BTRIM(cpf_condutor) AS cpf_condutor,
  BTRIM(crmc) AS crmc,
  TO_CHAR(validade_crmc::date, 'YYYY-MM-DD') AS validade_crmc,
  TO_CHAR(validade_curso::date, 'YYYY-MM-DD') AS validade_curso,
  COALESCE(BTRIM(tipo_vinculo), '') AS tipo_vinculo,
  COALESCE(BTRIM(historico), '') AS historico,
  TO_CHAR(data_inclusao, 'YYYY-MM-DD HH24:MI:SS') AS data_inclusao,
  TO_CHAR(data_modificacao, 'YYYY-MM-DD HH24:MI:SS') AS data_modificacao`

const condutorImportRecusaSelectClause = `
  id::text AS id,
  BTRIM(arquivo_xml) AS arquivo_xml,
  linha_xml::text AS linha_xml,
  COALESCE(BTRIM(codigo_xml), '') AS codigo_xml,
  COALESCE(BTRIM(condutor_xml), '') AS condutor_xml,
  COALESCE(BTRIM(cpf_condutor_xml), '') AS cpf_condutor_xml,
  COALESCE(BTRIM(crmc_xml), '') AS crmc_xml,
  COALESCE(BTRIM(tipo_vinculo_xml), '') AS tipo_vinculo_xml,
  BTRIM(motivo_recusa) AS motivo_recusa,
  TO_CHAR(data_importacao, 'YYYY-MM-DD HH24:MI:SS') AS data_importacao`

const monitorSelectClause = `
  codigo::text AS codigo,
  BTRIM(monitor) AS monitor,
  COALESCE(BTRIM(rg_monitor), '') AS rg_monitor,
  BTRIM(cpf_monitor) AS cpf_monitor,
  TO_CHAR(curso_monitor::date, 'YYYY-MM-DD') AS curso_monitor,
  TO_CHAR(validade_curso::date, 'YYYY-MM-DD') AS validade_curso,
  COALESCE(BTRIM(tipo_vinculo), '') AS tipo_vinculo,
  TO_CHAR(nascimento::date, 'YYYY-MM-DD') AS nascimento,
  TO_CHAR(data_inclusao, 'YYYY-MM-DD HH24:MI:SS') AS data_inclusao,
  TO_CHAR(data_modificacao, 'YYYY-MM-DD HH24:MI:SS') AS data_modificacao`

const monitorImportRecusaSelectClause = `
  id::text AS id,
  BTRIM(arquivo_xml) AS arquivo_xml,
  linha_xml::text AS linha_xml,
  COALESCE(BTRIM(codigo_xml), '') AS codigo_xml,
  COALESCE(BTRIM(monitor_xml), '') AS monitor_xml,
  COALESCE(BTRIM(cpf_monitor_xml), '') AS cpf_monitor_xml,
  COALESCE(BTRIM(rg_monitor_xml), '') AS rg_monitor_xml,
  COALESCE(BTRIM(tipo_vinculo_xml), '') AS tipo_vinculo_xml,
  BTRIM(motivo_recusa) AS motivo_recusa,
  TO_CHAR(data_importacao, 'YYYY-MM-DD HH24:MI:SS') AS data_importacao`

const cepSelectClause = `
  BTRIM(cep) AS cep,
  COALESCE(BTRIM(logradouro), '') AS logradouro,
  COALESCE(BTRIM(complemento), '') AS complemento,
  COALESCE(BTRIM(bairro), '') AS bairro,
  COALESCE(BTRIM(municipio), '') AS municipio,
  COALESCE(BTRIM(uf), '') AS uf,
  COALESCE(BTRIM(ibge), '') AS ibge,
  TO_CHAR(data_inclusao, 'YYYY-MM-DD HH24:MI:SS') AS data_inclusao,
  TO_CHAR(data_modificacao, 'YYYY-MM-DD HH24:MI:SS') AS data_modificacao`

const cepImportRecusaSelectClause = `
  id::text AS id,
  BTRIM(arquivo_xml) AS arquivo_xml,
  linha_xml::text AS linha_xml,
  COALESCE(BTRIM(cep_xml), '') AS cep_xml,
  COALESCE(BTRIM(logradouro_xml), '') AS logradouro_xml,
  COALESCE(BTRIM(municipio_xml), '') AS municipio_xml,
  COALESCE(BTRIM(uf_xml), '') AS uf_xml,
  BTRIM(motivo_recusa) AS motivo_recusa,
  TO_CHAR(data_importacao, 'YYYY-MM-DD HH24:MI:SS') AS data_importacao`

const veiculoSelectClause = `
  codigo::text AS codigo,
  COALESCE(BTRIM(crm), '') AS crm,
  COALESCE(BTRIM(placas), '') AS placas,
  COALESCE(ano::text, '') AS ano,
  COALESCE(cap_detran::text, '') AS cap_detran,
  COALESCE(cap_teg::text, '') AS cap_teg,
  COALESCE(cap_teg_creche::text, '') AS cap_teg_creche,
  COALESCE(cap_acessivel::text, '') AS cap_acessivel,
  TO_CHAR(val_crm::date, 'YYYY-MM-DD') AS val_crm,
  COALESCE(BTRIM(seguradora), '') AS seguradora,
  TO_CHAR(seguro_inicio::date, 'YYYY-MM-DD') AS seguro_inicio,
  TO_CHAR(seguro_termino::date, 'YYYY-MM-DD') AS seguro_termino,
  COALESCE(BTRIM(tipo_de_bancada), '') AS tipo_de_bancada,
  COALESCE(BTRIM(tipo_de_veiculo), '') AS tipo_de_veiculo,
  COALESCE(BTRIM(marca_modelo), '') AS marca_modelo,
  COALESCE(BTRIM(titular), '') AS titular,
  COALESCE(BTRIM(cnpj_cpf), '') AS cnpj_cpf,
  COALESCE(TO_CHAR(valor_veiculo, 'FM999999999990.00'), '') AS valor_veiculo,
  COALESCE(BTRIM(os_especial), '') AS os_especial,
  TO_CHAR(data_inclusao, 'YYYY-MM-DD HH24:MI:SS') AS data_inclusao,
  TO_CHAR(data_modificacao, 'YYYY-MM-DD HH24:MI:SS') AS data_modificacao`

const veiculoImportRecusaSelectClause = `
  id::text AS id,
  BTRIM(arquivo_xml) AS arquivo_xml,
  linha_xml::text AS linha_xml,
  COALESCE(BTRIM(codigo_xml), '') AS codigo_xml,
  COALESCE(BTRIM(crm_xml), '') AS crm_xml,
  COALESCE(BTRIM(placas_xml), '') AS placas_xml,
  COALESCE(BTRIM(tipo_de_veiculo_xml), '') AS tipo_de_veiculo_xml,
  BTRIM(motivo_recusa) AS motivo_recusa,
  TO_CHAR(data_importacao, 'YYYY-MM-DD HH24:MI:SS') AS data_importacao`

const titularSelectClause = `
  codigo::text AS codigo,
  COALESCE(BTRIM(cnpj_cpf), '') AS cnpj_cpf,
  COALESCE(BTRIM(titular), '') AS titular,
  TO_CHAR(data_inclusao, 'YYYY-MM-DD HH24:MI:SS') AS data_inclusao,
  TO_CHAR(data_modificacao, 'YYYY-MM-DD HH24:MI:SS') AS data_modificacao`
const titularTableName = '"titularCrm"'
const titularSequenceName = '"titularCrm_codigo_seq"'
const titularUniqueIndexName = '"titularCrm_codigo_unique_idx"'

const credenciadaSelectClause = `
  codigo::text AS codigo,
  BTRIM(credenciado) AS credenciado,
  COALESCE(BTRIM(tipo_pessoa), '') AS tipo_pessoa,
  BTRIM(cnpj_cpf) AS cnpj_cpf,
  COALESCE(BTRIM(cep), '') AS cep,
  COALESCE(BTRIM(numero), '') AS numero,
  COALESCE(BTRIM(complemento), '') AS complemento,
  COALESCE((SELECT BTRIM(c.logradouro) FROM ceps c WHERE c.cep = BTRIM(credenciada.cep)), '') AS logradouro,
  COALESCE((SELECT BTRIM(c.bairro) FROM ceps c WHERE c.cep = BTRIM(credenciada.cep)), '') AS bairro,
  COALESCE((SELECT BTRIM(c.municipio) FROM ceps c WHERE c.cep = BTRIM(credenciada.cep)), '') AS municipio,
  COALESCE(BTRIM(email), '') AS email,
  COALESCE(BTRIM(telefone_01), '') AS telefone_01,
  COALESCE(BTRIM(telefone_02), '') AS telefone_02,
  COALESCE(BTRIM(representante), '') AS representante,
  COALESCE(BTRIM(cpf_representante), '') AS cpf_representante,
  CASE
    WHEN UPPER(BTRIM(COALESCE(status, ''))) = 'CANCELADO' THEN 'CANCELADO'
    ELSE 'ATIVO'
  END AS status,
  TO_CHAR(data_inclusao, 'YYYY-MM-DD HH24:MI:SS') AS data_inclusao,
  TO_CHAR(data_modificacao, 'YYYY-MM-DD HH24:MI:SS') AS data_modificacao`

const credenciadaImportRecusaSelectClause = `
  id::text AS id,
  BTRIM(arquivo_xml) AS arquivo_xml,
  linha_xml::text AS linha_xml,
  COALESCE(BTRIM(codigo_xml), '') AS codigo_xml,
  COALESCE(BTRIM(credenciado_xml), '') AS credenciado_xml,
  COALESCE(BTRIM(cnpj_cpf_xml), '') AS cnpj_cpf_xml,
  COALESCE(BTRIM(representante_xml), '') AS representante_xml,
  COALESCE(BTRIM(status_xml), '') AS status_xml,
  BTRIM(motivo_recusa) AS motivo_recusa,
  TO_CHAR(data_importacao, 'YYYY-MM-DD HH24:MI:SS') AS data_importacao`

const credenciamentoTermoCredenciadoExpression = "COALESCE(BTRIM((SELECT cr.credenciado FROM credenciada cr WHERE cr.codigo = credenciada_codigo)), '')"
const credenciamentoTermoEmpresaExpression = "COALESCE(BTRIM((SELECT cr.empresa FROM credenciada cr WHERE cr.codigo = credenciada_codigo)), '')"
const credenciamentoTermoCnpjCpfExpression = "COALESCE(BTRIM((SELECT cr.cnpj_cpf FROM credenciada cr WHERE cr.codigo = credenciada_codigo)), '')"
const credenciamentoTermoEspecificacaoSeiExpression = `CASE
  WHEN ${credenciamentoTermoCredenciadoExpression} = '' AND ${credenciamentoTermoCnpjCpfExpression} = '' THEN ''
  WHEN ${credenciamentoTermoCnpjCpfExpression} = '' THEN CONCAT('CREDENCIAMENTO TEG - ', ${credenciamentoTermoCredenciadoExpression})
  WHEN ${credenciamentoTermoCredenciadoExpression} = '' THEN CONCAT('CREDENCIAMENTO TEG - CNPJ/CPF: ', ${credenciamentoTermoCnpjCpfExpression})
  ELSE CONCAT('CREDENCIAMENTO TEG - ', ${credenciamentoTermoCredenciadoExpression}, ' - CNPJ/CPF: ', ${credenciamentoTermoCnpjCpfExpression})
END`
const credenciamentoTermoMesRenovacaoExpression = `CASE
  WHEN termino_vigencia IS NULL THEN ''
  ELSE CONCAT(
    CASE EXTRACT(MONTH FROM termino_vigencia)
      WHEN 1 THEN 'jan'
      WHEN 2 THEN 'fev'
      WHEN 3 THEN 'mar'
      WHEN 4 THEN 'abr'
      WHEN 5 THEN 'mai'
      WHEN 6 THEN 'jun'
      WHEN 7 THEN 'jul'
      WHEN 8 THEN 'ago'
      WHEN 9 THEN 'set'
      WHEN 10 THEN 'out'
      WHEN 11 THEN 'nov'
      WHEN 12 THEN 'dez'
      ELSE ''
    END,
    '/',
    TO_CHAR(termino_vigencia::date, 'YYYY')
  )
END`
const credenciamentoTermoNormalizedTermoExpression = "REGEXP_REPLACE(COALESCE(BTRIM(termo_adesao), ''), '\\D', '', 'g')"

const credenciamentoTermoSelectClause = `
  codigo::text AS codigo,
  codigo_xml::text AS codigo_xml,
  credenciada_codigo::text AS credenciada_codigo,
  ${credenciamentoTermoCredenciadoExpression} AS credenciado,
  ${credenciamentoTermoEmpresaExpression} AS empresa,
  ${credenciamentoTermoCnpjCpfExpression} AS cnpj_cpf,
  COALESCE(BTRIM(termo_adesao), '') AS termo_adesao,
  COALESCE(BTRIM(sei), '') AS sei,
  aditivo::text AS aditivo,
  COALESCE(BTRIM(situacao_publicacao), '') AS situacao_publicacao,
  COALESCE(BTRIM(situacao_emissao), '') AS situacao_emissao,
  TO_CHAR(inicio_vigencia::date, 'YYYY-MM-DD') AS inicio_vigencia,
  TO_CHAR(termino_vigencia::date, 'YYYY-MM-DD') AS termino_vigencia,
  TO_CHAR(comp_data_aditivo::date, 'YYYY-MM-DD') AS comp_data_aditivo,
  COALESCE(BTRIM(status_aditivo), '') AS status_aditivo,
  TO_CHAR(data_pub_aditivo::date, 'YYYY-MM-DD') AS data_pub_aditivo,
  check_aditivo::text AS check_aditivo,
  COALESCE(BTRIM(status_termo), '') AS status_termo,
  COALESCE(BTRIM(tipo_termo), '') AS tipo_termo,
  COALESCE(BTRIM((SELECT cr.representante FROM credenciada cr WHERE cr.codigo = credenciada_codigo)), '') AS representante,
  COALESCE(BTRIM((SELECT cr.cpf_representante FROM credenciada cr WHERE cr.codigo = credenciada_codigo)), '') AS cpf_representante,
  COALESCE(BTRIM((SELECT BTRIM(cr.cep) FROM credenciada cr WHERE cr.codigo = credenciada_codigo)), '') AS credenciada_cep,
  COALESCE((SELECT BTRIM(c.logradouro) FROM ceps c WHERE c.cep = BTRIM((SELECT cr.cep FROM credenciada cr WHERE cr.codigo = credenciada_codigo))), '') AS logradouro,
  COALESCE((SELECT BTRIM(c.bairro) FROM ceps c WHERE c.cep = BTRIM((SELECT cr.cep FROM credenciada cr WHERE cr.codigo = credenciada_codigo))), '') AS bairro,
  COALESCE((SELECT BTRIM(c.municipio) FROM ceps c WHERE c.cep = BTRIM((SELECT cr.cep FROM credenciada cr WHERE cr.codigo = credenciada_codigo))), '') AS municipio,
  ${credenciamentoTermoEspecificacaoSeiExpression} AS especificacao_sei,
  valor_contrato::text AS valor_contrato,
  TO_CHAR(data_publicacao::date, 'YYYY-MM-DD') AS data_publicacao,
  valor_contrato_atualizado::text AS valor_contrato_atualizado,
  TO_CHAR(vencimento_geral::date, 'YYYY-MM-DD') AS vencimento_geral,
  ${credenciamentoTermoMesRenovacaoExpression} AS mes_renovacao,
  COALESCE(BTRIM(tp_optante), '') AS tp_optante,
  TO_CHAR(data_inclusao, 'YYYY-MM-DD HH24:MI:SS') AS data_inclusao,
  TO_CHAR(data_modificacao, 'YYYY-MM-DD HH24:MI:SS') AS data_modificacao`

const credenciamentoTermoImportRecusaSelectClause = `
  id::text AS id,
  BTRIM(arquivo_xml) AS arquivo_xml,
  linha_xml::text AS linha_xml,
  COALESCE(BTRIM(codigo_xml), '') AS codigo_xml,
  COALESCE(BTRIM(credenciado_xml), '') AS credenciado_xml,
  COALESCE(BTRIM(aditivo_xml), '') AS aditivo_xml,
  BTRIM(motivo_recusa) AS motivo_recusa,
  TO_CHAR(data_importacao, 'YYYY-MM-DD HH24:MI:SS') AS data_importacao`

const vinculoCondutorSelectClause = `
  vc.id::text AS id,
  COALESCE(BTRIM(vc.termo_adesao), '') AS termo_adesao,
  COALESCE(BTRIM(vc.num_os), '') AS num_os,
  COALESCE(BTRIM(vc.revisao), '') AS revisao,
  COALESCE(vc.credenciada_codigo::text, '') AS credenciada_codigo,
  COALESCE(BTRIM(cr.credenciado), '') AS credenciado,
  TO_CHAR(vc.data_os::date, 'YYYY-MM-DD') AS data_os,
  TO_CHAR(vc.data_admissao_condutor::date, 'YYYY-MM-DD') AS data_admissao_condutor,
  COALESCE(vc.condutor_codigo::text, '') AS condutor_codigo,
  COALESCE(BTRIM(cd.condutor), '') AS condutor,
  COALESCE(BTRIM(cd.cpf_condutor), '') AS cpf_condutor,
  TO_CHAR(vc.data_inclusao, 'YYYY-MM-DD HH24:MI:SS') AS data_inclusao,
  COALESCE(BTRIM(vc.codigo_xml), '') AS codigo_xml`

const vinculoCondutorImportRecusaSelectClause = `
  id::text AS id,
  BTRIM(arquivo_xml) AS arquivo_xml,
  linha_xml::text AS linha_xml,
  COALESCE(BTRIM(codigo_xml), '') AS codigo_xml,
  COALESCE(BTRIM(empregador_xml), '') AS empregador_xml,
  COALESCE(BTRIM(cpf_condutor_xml), '') AS cpf_condutor_xml,
  COALESCE(BTRIM(data_os_xml), '') AS data_os_xml,
  COALESCE(BTRIM(admissao_xml), '') AS admissao_xml,
  BTRIM(motivo_recusa) AS motivo_recusa,
  TO_CHAR(data_importacao, 'YYYY-MM-DD HH24:MI:SS') AS data_importacao`

const vinculoMonitorSelectClause = `
  vm.id::text AS id,
  COALESCE(BTRIM(vm.termo_adesao), '') AS termo_adesao,
  COALESCE(BTRIM(vm.num_os), '') AS num_os,
  COALESCE(BTRIM(vm.revisao), '') AS revisao,
  COALESCE(vm.credenciada_codigo::text, '') AS credenciada_codigo,
  COALESCE(BTRIM(cr.credenciado), '') AS credenciado,
  TO_CHAR(vm.data_os::date, 'YYYY-MM-DD') AS data_os,
  TO_CHAR(vm.data_admissao_monitor::date, 'YYYY-MM-DD') AS data_admissao_monitor,
  COALESCE(vm.monitor_codigo::text, '') AS monitor_codigo,
  COALESCE(BTRIM(mt.monitor), '') AS monitor,
  COALESCE(BTRIM(mt.cpf_monitor), '') AS cpf_monitor,
  TO_CHAR(vm.data_inclusao, 'YYYY-MM-DD HH24:MI:SS') AS data_inclusao,
  COALESCE(BTRIM(vm.codigo_xml), '') AS codigo_xml`

const vinculoMonitorImportRecusaSelectClause = `
  id::text AS id,
  BTRIM(arquivo_xml) AS arquivo_xml,
  linha_xml::text AS linha_xml,
  COALESCE(BTRIM(codigo_xml), '') AS codigo_xml,
  COALESCE(BTRIM(empregador_xml), '') AS empregador_xml,
  COALESCE(BTRIM(cpf_monitor_xml), '') AS cpf_monitor_xml,
  COALESCE(BTRIM(data_os_xml), '') AS data_os_xml,
  COALESCE(BTRIM(admissao_xml), '') AS admissao_xml,
  BTRIM(motivo_recusa) AS motivo_recusa,
  TO_CHAR(data_importacao, 'YYYY-MM-DD HH24:MI:SS') AS data_importacao`

const ordemServicoSelectClause = `
  codigo::text AS codigo,
  COALESCE(BTRIM(codigo_access), '') AS codigo_access,
  COALESCE(BTRIM(termo_adesao), '') AS termo_adesao,
  COALESCE(BTRIM(num_os), '') AS num_os,
  COALESCE(BTRIM(revisao), '') AS revisao,
  COALESCE(BTRIM(os_concat), '') AS os_concat,
  TO_CHAR(vigencia_os::date, 'YYYY-MM-DD') AS vigencia_os,
  COALESCE((SELECT credenciada_codigo::text FROM ${credenciamentoTermoTableName} WHERE codigo = ${ordemServicoTableName}.termo_codigo), '') AS credenciada_codigo,
  COALESCE(BTRIM((SELECT cr.credenciado FROM credenciada cr WHERE cr.codigo = (SELECT credenciada_codigo FROM ${credenciamentoTermoTableName} WHERE codigo = ${ordemServicoTableName}.termo_codigo))), '') AS credenciado,
  COALESCE(BTRIM((SELECT cr.cnpj_cpf FROM credenciada cr WHERE cr.codigo = (SELECT credenciada_codigo FROM ${credenciamentoTermoTableName} WHERE codigo = ${ordemServicoTableName}.termo_codigo))), '') AS cnpj_cpf,
  COALESCE(BTRIM(dre_codigo), '') AS dre_codigo,
  COALESCE(BTRIM(dre_descricao), '') AS dre_descricao,
  COALESCE(modalidade_codigo::text, '') AS modalidade_codigo,
  COALESCE(BTRIM(modalidade_descricao), '') AS modalidade_descricao,
  COALESCE(BTRIM(cpf_condutor), '') AS cpf_condutor,
  COALESCE(BTRIM(condutor), '') AS condutor,
  TO_CHAR(data_admissao_condutor::date, 'YYYY-MM-DD') AS data_admissao_condutor,
  COALESCE(BTRIM(cpf_preposto), '') AS cpf_preposto,
  COALESCE(BTRIM(preposto_condutor), '') AS preposto_condutor,
  TO_CHAR(preposto_inicio::date, 'YYYY-MM-DD') AS preposto_inicio,
  COALESCE(preposto_dias::text, '') AS preposto_dias,
  COALESCE(BTRIM(crm), '') AS crm,
  COALESCE(BTRIM(veiculo_placas), '') AS veiculo_placas,
  COALESCE(BTRIM(cpf_monitor), '') AS cpf_monitor,
  COALESCE(BTRIM(monitor), '') AS monitor,
  TO_CHAR(data_admissao_monitor::date, 'YYYY-MM-DD') AS data_admissao_monitor,
  COALESCE(BTRIM(situacao), '') AS situacao,
  COALESCE(tipo_troca_codigo::text, '') AS tipo_troca_codigo,
  COALESCE(BTRIM(tipo_troca_descricao), '') AS tipo_troca_descricao,
  COALESCE(BTRIM(conexao), '') AS conexao,
  TO_CHAR(data_encerramento::date, 'YYYY-MM-DD') AS data_encerramento,
  COALESCE(BTRIM(anotacao), '') AS anotacao,
  COALESCE(BTRIM(uniao_termos), '') AS uniao_termos,
  TO_CHAR(data_inclusao, 'YYYY-MM-DD HH24:MI:SS') AS data_inclusao,
  TO_CHAR(data_modificacao, 'YYYY-MM-DD HH24:MI:SS') AS data_modificacao`

const ordemServicoCompositeKeyOrderClause = `
  UPPER(BTRIM(COALESCE(${ordemServicoTableName}.termo_adesao, ''))) ASC,
  UPPER(BTRIM(COALESCE(${ordemServicoTableName}.num_os, ''))) ASC,
  UPPER(BTRIM(COALESCE(${ordemServicoTableName}.revisao, ''))) ASC,
  ${ordemServicoTableName}.codigo ASC`

const ordemServicoSemRevisaoLabel = '-S/R'

const normalizeOrdemServicoTermoAdesao = (value) => {
  const digits = normalizeRequestValue(value).replace(/\D/g, '').slice(0, 11)

  if (digits.length <= 4) {
    return digits
  }

  return `${digits.slice(0, 4)}/${digits.slice(4)}`
}

const buildOrdemServicoConcat = ({ termoAdesao, numOs, revisao }) => {
  const normalizedTermoAdesao = normalizeOrdemServicoTermoAdesao(termoAdesao)
  const normalizedNumOs = normalizeRequestValue(numOs)
  const normalizedRevisao = normalizeRequestValue(revisao) || ordemServicoSemRevisaoLabel

  return `${normalizedTermoAdesao}-${normalizedNumOs}${normalizedRevisao}`
}

const buildRevisionSequenceLabel = (sequenceNumber) => {
  if (!Number.isInteger(sequenceNumber) || sequenceNumber <= 0) {
    return ''
  }

  let currentNumber = sequenceNumber
  let label = ''

  while (currentNumber > 0) {
    currentNumber -= 1
    label = String.fromCharCode(65 + (currentNumber % 26)) + label
    currentNumber = Math.floor(currentNumber / 26)
  }

  return label
}

const parseRevisionSequenceNumber = (value) => {
  const normalizedValue = normalizeRequestValue(value).toUpperCase()

  if (!normalizedValue || normalizedValue === ordemServicoSemRevisaoLabel) {
    return 0
  }

  if (!/^[A-Z]+$/.test(normalizedValue)) {
    return 0
  }

  let sequenceNumber = 0

  for (const letter of normalizedValue) {
    sequenceNumber = (sequenceNumber * 26) + (letter.charCodeAt(0) - 64)
  }

  return sequenceNumber
}

const extractOrdemServicoNumeroFromOs = (value) => {
  const normalizedValue = normalizeRequestValue(value).toUpperCase()
  const match = normalizedValue.match(/-(\d+)[A-Z]*$/)

  return match ? match[1].slice(0, 10) : ''
}

const extractOrdemServicoRevisionFromOs = (value) => {
  const normalizedValue = normalizeRequestValue(value).toUpperCase()

  if (!normalizedValue) {
    return ''
  }

  const match = normalizedValue.match(/[A-Z]+$/)

  return match ? match[0] : ordemServicoSemRevisaoLabel
}

const fetchOrdemServicoItemByCodigo = async (executor, codigo) => {
  const result = await executor.query(
    `SELECT ${ordemServicoSelectClause}
    FROM ${ordemServicoTableName}
     WHERE codigo = $1
     LIMIT 1`,
    [codigo],
  )

  return result.rows[0] ?? null
}

const rebalanceOrdemServicoRevisions = async (executor, osValues = null) => {
  return executor.query(
    `UPDATE ${ordemServicoTableName}
     SET revisao = COALESCE(NULLIF(BTRIM(revisao), ''), $1),
         os_concat = CONCAT(
           COALESCE(BTRIM(termo_adesao), ''),
           '-',
           COALESCE(BTRIM(num_os), ''),
           COALESCE(NULLIF(BTRIM(revisao), ''), $1)
         )
     WHERE COALESCE(BTRIM(revisao), '') = ''
        OR COALESCE(BTRIM(os_concat), '') <> CONCAT(
          COALESCE(BTRIM(termo_adesao), ''),
          '-',
          COALESCE(BTRIM(num_os), ''),
          COALESCE(NULLIF(BTRIM(revisao), ''), $1)
        )`,
    [ordemServicoSemRevisaoLabel],
  )
}

const syncCondutorVinculosFromOrdemServico = async (executor) => {
  await executor.query(
    `DELETE FROM ${vinculoCondutorTableName}
     WHERE COALESCE(BTRIM(termo_adesao), '') <> ''
        OR COALESCE(BTRIM(num_os), '') <> ''
        OR COALESCE(BTRIM(revisao), '') <> ''`,
  )

  await executor.query(
    `INSERT INTO ${vinculoCondutorTableName} (
       termo_adesao,
       num_os,
       revisao,
       credenciada_codigo,
       data_admissao_condutor,
       condutor_codigo,
       data_inclusao
     )
     SELECT DISTINCT
       NULLIF(BTRIM(os.termo_adesao), ''),
       NULLIF(BTRIM(os.num_os), ''),
       COALESCE(NULLIF(BTRIM(os.revisao), ''), $1),
       t.credenciada_codigo,
       os.data_admissao_condutor,
       c.codigo,
       COALESCE(os.data_inclusao, NOW())
     FROM ${ordemServicoTableName} os
     LEFT JOIN ${credenciamentoTermoTableName} t ON t.codigo = os.termo_codigo
     LEFT JOIN condutor c
       ON BTRIM(COALESCE(c.cpf_condutor, '')) = BTRIM(COALESCE(os.cpf_condutor, ''))
     WHERE COALESCE(BTRIM(os.termo_adesao), '') <> ''
       AND COALESCE(BTRIM(os.num_os), '') <> ''
       AND os.termo_codigo IS NOT NULL
       AND t.credenciada_codigo IS NOT NULL
       AND os.data_admissao_condutor IS NOT NULL
       AND c.codigo IS NOT NULL`,
    [ordemServicoSemRevisaoLabel],
  )
}

const syncMonitorVinculosFromOrdemServico = async (executor) => {
  await executor.query(
    `DELETE FROM ${vinculoMonitorTableName}
     WHERE COALESCE(BTRIM(termo_adesao), '') <> ''
        OR COALESCE(BTRIM(num_os), '') <> ''
        OR COALESCE(BTRIM(revisao), '') <> ''`,
  )

  await executor.query(
    `INSERT INTO ${vinculoMonitorTableName} (
       termo_adesao,
       num_os,
       revisao,
       credenciada_codigo,
       data_admissao_monitor,
       monitor_codigo,
       data_inclusao
     )
     SELECT DISTINCT
       NULLIF(BTRIM(os.termo_adesao), ''),
       NULLIF(BTRIM(os.num_os), ''),
       COALESCE(NULLIF(BTRIM(os.revisao), ''), $1),
       t.credenciada_codigo,
       os.data_admissao_monitor,
       m.codigo,
       COALESCE(os.data_inclusao, NOW())
     FROM ${ordemServicoTableName} os
     LEFT JOIN ${credenciamentoTermoTableName} t ON t.codigo = os.termo_codigo
     LEFT JOIN monitor m
       ON BTRIM(COALESCE(m.cpf_monitor, '')) = BTRIM(COALESCE(os.cpf_monitor, ''))
     WHERE COALESCE(BTRIM(os.termo_adesao), '') <> ''
       AND COALESCE(BTRIM(os.num_os), '') <> ''
       AND os.termo_codigo IS NOT NULL
       AND t.credenciada_codigo IS NOT NULL
       AND os.data_admissao_monitor IS NOT NULL
       AND m.codigo IS NOT NULL`,
    [ordemServicoSemRevisaoLabel],
  )
}

const ordemServicoImportRecusaSelectClause = `
  id::text AS id,
  BTRIM(arquivo_xml) AS arquivo_xml,
  linha_xml::text AS linha_xml,
  COALESCE(BTRIM(codigo_xml), '') AS codigo_xml,
  COALESCE(BTRIM(codigo_xml), '') AS codigo_access,
  COALESCE(BTRIM(os_xml), '') AS os_xml,
  COALESCE(BTRIM(num_os_xml), '') AS num_os_xml,
  COALESCE(BTRIM(credenciado_xml), '') AS credenciado_xml,
  COALESCE(BTRIM(dre_xml), '') AS dre_xml,
  COALESCE(BTRIM(cpf_condutor_xml), '') AS cpf_condutor_xml,
  COALESCE(BTRIM(cpf_monitor_xml), '') AS cpf_monitor_xml,
  COALESCE(BTRIM(crm_xml), '') AS crm_xml,
  BTRIM(motivo_recusa) AS motivo_recusa,
  TO_CHAR(data_importacao, 'YYYY-MM-DD HH24:MI:SS') AS data_importacao`

const trocaSelectClause = `
  codigo::text AS codigo,
  controle::text AS controle,
  BTRIM(lista) AS lista,
  TO_CHAR(data_inclusao, 'YYYY-MM-DD HH24:MI:SS') AS data_inclusao,
  TO_CHAR(data_modificacao, 'YYYY-MM-DD HH24:MI:SS') AS data_modificacao`

const seguradoraSelectClause = `
  codigo::text AS codigo,
  controle::text AS controle,
  BTRIM(lista) AS descricao,
  TO_CHAR(data_inclusao, 'YYYY-MM-DD HH24:MI:SS') AS data_inclusao,
  TO_CHAR(data_modificacao, 'YYYY-MM-DD HH24:MI:SS') AS data_modificacao`

const importCondutorXmlFile = async (fileName) => {
  const sanitizedFileName = path.basename(normalizeRequestValue(fileName))

  if (!sanitizedFileName) {
    throw new Error('Nome do arquivo XML e obrigatorio.')
  }

  if (path.extname(sanitizedFileName).toLowerCase() !== '.xml') {
    throw new Error('Informe um arquivo XML valido.')
  }

  const resolvedPath = path.resolve(importXmlDirectory, sanitizedFileName)

  if (!resolvedPath.startsWith(importXmlDirectory)) {
    throw new Error('Arquivo XML invalido.')
  }

  const xmlContent = await readFile(resolvedPath, 'utf8')
  const parsedRecords = parseCondutorXml(xmlContent)

  if (!parsedRecords.length) {
    throw new Error('Nenhum registro de condutor foi encontrado no XML informado.')
  }

  const normalizedRecords = []
  const skippedRecords = []

  parsedRecords.forEach((record, index) => {
    try {
      normalizedRecords.push(normalizeImportedCondutorRecord(record, index))
    } catch (error) {
      skippedRecords.push({
        index: index + 1,
        codigoXml: normalizeRequestValue(record.codigo),
        condutorXml: normalizeRequestValue(record.condutor),
        cpfCondutorXml: normalizeRequestValue(record.cpfCondutor),
        crmcXml: normalizeRequestValue(record.crmc),
        tipoVinculoXml: normalizeRequestValue(record.tipoVinculo),
        message: error instanceof Error ? error.message : `Registro ${index + 1}: erro ao validar o XML.`,
      })
    }
  })

  const client = await pool.connect()

  try {
    await client.query('BEGIN')
    await client.query('TRUNCATE TABLE condutor_import_recusa RESTART IDENTITY')
    let inserted = 0
    let updated = 0

    for (const skippedRecord of skippedRecords) {
      await client.query(
        `INSERT INTO condutor_import_recusa (
           arquivo_xml,
           linha_xml,
           codigo_xml,
           condutor_xml,
           cpf_condutor_xml,
           crmc_xml,
           tipo_vinculo_xml,
           motivo_recusa,
           data_importacao
         )
         VALUES ($1, $2, NULLIF($3, ''), NULLIF($4, ''), NULLIF($5, ''), NULLIF($6, ''), NULLIF($7, ''), $8, NOW())`,
        [
          sanitizedFileName,
          skippedRecord.index,
          skippedRecord.codigoXml,
          skippedRecord.condutorXml,
          skippedRecord.cpfCondutorXml,
          skippedRecord.crmcXml,
          skippedRecord.tipoVinculoXml,
          skippedRecord.message,
        ],
      )
    }

    for (const record of normalizedRecords) {
      const existingResult = await client.query('SELECT 1 FROM condutor WHERE codigo = $1 LIMIT 1', [record.codigo])

      if (existingResult.rowCount > 0) {
        await client.query(
          `UPDATE condutor
           SET condutor = $1,
               cpf_condutor = $2,
               crmc = $3,
                validade_crmc = NULLIF($4, '')::date,
               validade_curso = NULLIF($5, '')::date,
               tipo_vinculo = NULLIF($6, ''),
               historico = NULLIF($7, ''),
               data_modificacao = NOW()
           WHERE codigo = $8`,
          [
            record.condutor,
            record.cpfCondutor,
            record.crmc,
            record.validadeCrmc,
            record.validadeCurso,
            record.tipoVinculo,
            record.historico,
            record.codigo,
          ],
        )
        updated += 1
        continue
      }

      await client.query(
        `INSERT INTO condutor (
           codigo,
           condutor,
           cpf_condutor,
           crmc,
           validade_crmc,
           validade_curso,
           tipo_vinculo,
           historico,
           data_inclusao,
           data_modificacao
         )
         VALUES ($1, $2, $3, $4, NULLIF($5, '')::date, NULLIF($6, '')::date, NULLIF($7, ''), NULLIF($8, ''), NOW(), NOW())`,
        [
          record.codigo,
          record.condutor,
          record.cpfCondutor,
          record.crmc,
          record.validadeCrmc,
          record.validadeCurso,
          record.tipoVinculo,
          record.historico,
        ],
      )
      inserted += 1
    }

    if (normalizedRecords.length) {
      await client.query('SELECT setval(\'condutor_codigo_seq\', GREATEST(COALESCE((SELECT MAX(codigo) FROM condutor), 0), 1), true)')
    }
    await client.query('COMMIT')

    return {
      fileName: sanitizedFileName,
      filePath: resolvedPath,
      total: parsedRecords.length,
      processed: normalizedRecords.length,
      inserted,
      updated,
      skipped: skippedRecords.length,
      skippedRecords: skippedRecords.slice(0, 20),
    }
  } catch (error) {
    await client.query('ROLLBACK')
    throw error
  } finally {
    client.release()
  }
}

const importMonitorXmlFile = async (fileName) => {
  const sanitizedFileName = path.basename(normalizeRequestValue(fileName))

  if (!sanitizedFileName) {
    throw new Error('Nome do arquivo XML e obrigatorio.')
  }

  if (path.extname(sanitizedFileName).toLowerCase() !== '.xml') {
    throw new Error('Informe um arquivo XML valido.')
  }

  const resolvedPath = path.resolve(importXmlDirectory, sanitizedFileName)

  if (!resolvedPath.startsWith(importXmlDirectory)) {
    throw new Error('Arquivo XML invalido.')
  }

  const xmlContent = await readFile(resolvedPath, 'utf8')
  const parsedRecords = parseMonitorXml(xmlContent)

  if (!parsedRecords.length) {
    throw new Error('Nenhum registro de monitor foi encontrado no XML informado.')
  }

  const normalizedRecords = []
  const skippedRecords = []

  parsedRecords.forEach((record, index) => {
    try {
      normalizedRecords.push(normalizeImportedMonitorRecord(record, index))
    } catch (error) {
      skippedRecords.push({
        index: index + 1,
        codigoXml: normalizeRequestValue(record.codigo),
        monitorXml: normalizeRequestValue(record.monitor),
        cpfMonitorXml: normalizeRequestValue(record.cpfMonitor),
        rgMonitorXml: normalizeRequestValue(record.rgMonitor),
        tipoVinculoXml: normalizeRequestValue(record.tipoVinculo),
        message: error instanceof Error ? error.message : `Registro ${index + 1}: erro ao validar o XML.`,
      })
    }
  })

  const client = await pool.connect()

  try {
    await client.query('BEGIN')
    await client.query('TRUNCATE TABLE monitor_import_recusa RESTART IDENTITY')
    let inserted = 0
    let updated = 0

    for (const skippedRecord of skippedRecords) {
      await client.query(
        `INSERT INTO monitor_import_recusa (
           arquivo_xml,
           linha_xml,
           codigo_xml,
           monitor_xml,
           cpf_monitor_xml,
           rg_monitor_xml,
           tipo_vinculo_xml,
           motivo_recusa,
           data_importacao
         )
         VALUES ($1, $2, NULLIF($3, ''), NULLIF($4, ''), NULLIF($5, ''), NULLIF($6, ''), NULLIF($7, ''), $8, NOW())`,
        [
          sanitizedFileName,
          skippedRecord.index,
          skippedRecord.codigoXml,
          skippedRecord.monitorXml,
          skippedRecord.cpfMonitorXml,
          skippedRecord.rgMonitorXml,
          skippedRecord.tipoVinculoXml,
          skippedRecord.message,
        ],
      )
    }

    for (const record of normalizedRecords) {
      const existingResult = await client.query('SELECT 1 FROM monitor WHERE codigo = $1 LIMIT 1', [record.codigo])

      if (existingResult.rowCount > 0) {
        await client.query(
          `UPDATE monitor
           SET monitor = $1,
               rg_monitor = NULLIF($2, ''),
               cpf_monitor = $3,
               curso_monitor = NULLIF($4, '')::date,
               validade_curso = NULLIF($5, '')::date,
               tipo_vinculo = NULLIF($6, ''),
               nascimento = NULLIF($7, '')::date,
               data_modificacao = NOW()
           WHERE codigo = $8`,
          [
            record.monitor,
            record.rgMonitor,
            record.cpfMonitor,
            record.cursoMonitor,
            record.validadeCurso,
            record.tipoVinculo,
            record.nascimento,
            record.codigo,
          ],
        )
        updated += 1
        continue
      }

      await client.query(
        `INSERT INTO monitor (
           codigo,
           monitor,
           rg_monitor,
           cpf_monitor,
           curso_monitor,
           validade_curso,
           tipo_vinculo,
           nascimento,
           data_inclusao,
           data_modificacao
         )
         VALUES ($1, $2, NULLIF($3, ''), $4, NULLIF($5, '')::date, NULLIF($6, '')::date, NULLIF($7, ''), NULLIF($8, '')::date, NOW(), NOW())`,
        [
          record.codigo,
          record.monitor,
          record.rgMonitor,
          record.cpfMonitor,
          record.cursoMonitor,
          record.validadeCurso,
          record.tipoVinculo,
          record.nascimento,
        ],
      )
      inserted += 1
    }

    if (normalizedRecords.length) {
      await client.query('SELECT setval(\'monitor_codigo_seq\', GREATEST(COALESCE((SELECT MAX(codigo) FROM monitor), 0), 1), true)')
    }
    await client.query('COMMIT')

    return {
      fileName: sanitizedFileName,
      filePath: resolvedPath,
      total: parsedRecords.length,
      processed: normalizedRecords.length,
      inserted,
      updated,
      skipped: skippedRecords.length,
      skippedRecords: skippedRecords.slice(0, 20),
    }
  } catch (error) {
    await client.query('ROLLBACK')
    throw error
  } finally {
    client.release()
  }
}

const parseCepsXml = (xmlContent) => {
  const parsed = xmlParser.parse(xmlContent)
  const rawRecords = parsed?.dataroot?.Cep
  const records = (Array.isArray(rawRecords)
    ? rawRecords
    : rawRecords
      ? [rawRecords]
      : [])
    .filter((record) => record && typeof record === 'object')

  return records.map((record) => ({
    cep: normalizeRequestValue(record?.CEP),
    logradouro: normalizeRequestValue(record?.Logradouro),
    complemento: normalizeRequestValue(record?.Complemento),
    bairro: normalizeRequestValue(record?.Bairro),
    municipio: normalizeRequestValue(record?.Municipio),
    uf: normalizeRequestValue(record?.UF),
    ibge: normalizeRequestValue(record?.IBGE),
  }))
}

const normalizeImportedCepRecord = (record, index) => {
  const cep = normalizeCep(record.cep)
  const logradouro = normalizeCredenciadaText(record.logradouro, 255)
  const complemento = normalizeCredenciadaText(record.complemento, 255)
  const bairro = normalizeCredenciadaText(record.bairro, 120)
  const municipio = normalizeCredenciadaText(record.municipio, 120)
  const uf = normalizeRequestValue(record.uf).toUpperCase().replace(/[^A-Z]/g, '').slice(0, 2)
  const ibge = normalizeRequestValue(record.ibge).replace(/\D/g, '').slice(0, 10)
  const itemLabel = `Registro ${index + 1}`

  if (!cep || !isCepValid(cep)) {
    throw new Error(`${itemLabel}: CEP invalido no XML.`)
  }

  if (!municipio) {
    throw new Error(`${itemLabel}: municipio obrigatorio no XML.`)
  }

  if (!uf || uf.length !== 2) {
    throw new Error(`${itemLabel}: UF invalida no XML.`)
  }

  return { cep, logradouro, complemento, bairro, municipio, uf, ibge }
}

const importCepsXmlFile = async (fileName) => {
  const sanitizedFileName = path.basename(normalizeRequestValue(fileName))

  if (!sanitizedFileName) {
    throw new Error('Nome do arquivo XML e obrigatorio.')
  }

  if (path.extname(sanitizedFileName).toLowerCase() !== '.xml') {
    throw new Error('Informe um arquivo XML valido.')
  }

  const resolvedPath = path.resolve(importXmlDirectory, sanitizedFileName)

  if (!resolvedPath.startsWith(importXmlDirectory)) {
    throw new Error('Arquivo XML invalido.')
  }

  const xmlContent = await readFile(resolvedPath, 'utf8')
  const parsedRecords = parseCepsXml(xmlContent)

  if (!parsedRecords.length) {
    throw new Error('Nenhum registro de CEP foi encontrado no XML informado.')
  }

  const normalizedRecords = []
  const skippedRecords = []

  parsedRecords.forEach((record, index) => {
    try {
      normalizedRecords.push(normalizeImportedCepRecord(record, index))
    } catch (error) {
      skippedRecords.push({
        index: index + 1,
        cepXml: normalizeRequestValue(record.cep),
        logradouroXml: normalizeRequestValue(record.logradouro),
        municipioXml: normalizeRequestValue(record.municipio),
        ufXml: normalizeRequestValue(record.uf),
        message: error instanceof Error ? error.message : `Registro ${index + 1}: erro ao validar o XML.`,
      })
    }
  })

  const client = await pool.connect()

  try {
    await client.query('BEGIN')
    await client.query(`TRUNCATE TABLE ${cepImportRecusaTableName} RESTART IDENTITY`)
    let inserted = 0
    let updated = 0

    for (const skippedRecord of skippedRecords) {
      await client.query(
        `INSERT INTO ${cepImportRecusaTableName} (
           arquivo_xml,
           linha_xml,
           cep_xml,
           logradouro_xml,
           municipio_xml,
           uf_xml,
           motivo_recusa,
           data_importacao
         )
         VALUES ($1, $2, NULLIF($3, ''), NULLIF($4, ''), NULLIF($5, ''), NULLIF($6, ''), $7, NOW())`,
        [
          sanitizedFileName,
          skippedRecord.index,
          skippedRecord.cepXml,
          skippedRecord.logradouroXml,
          skippedRecord.municipioXml,
          skippedRecord.ufXml,
          skippedRecord.message,
        ],
      )
    }

    for (const record of normalizedRecords) {
      const existingResult = await client.query(
        `SELECT 1 FROM ${cepTableName} WHERE BTRIM(cep) = $1 LIMIT 1`,
        [record.cep],
      )

      if (existingResult.rowCount > 0) {
        await client.query(
          `UPDATE ${cepTableName}
           SET logradouro = NULLIF($1, ''),
               complemento = NULLIF($2, ''),
               bairro = NULLIF($3, ''),
               municipio = $4,
               uf = $5,
               ibge = NULLIF($6, ''),
               data_modificacao = NOW()
           WHERE BTRIM(cep) = $7`,
          [record.logradouro, record.complemento, record.bairro, record.municipio, record.uf, record.ibge, record.cep],
        )
        updated += 1
        continue
      }

      await client.query(
        `INSERT INTO ${cepTableName} (
           cep,
           logradouro,
           complemento,
           bairro,
           municipio,
           uf,
           ibge,
           data_inclusao,
           data_modificacao
         )
         VALUES ($1, NULLIF($2, ''), NULLIF($3, ''), NULLIF($4, ''), $5, $6, NULLIF($7, ''), NOW(), NOW())`,
        [record.cep, record.logradouro, record.complemento, record.bairro, record.municipio, record.uf, record.ibge],
      )
      inserted += 1
    }

    await client.query('COMMIT')

    return {
      fileName: sanitizedFileName,
      filePath: resolvedPath,
      total: parsedRecords.length,
      processed: normalizedRecords.length,
      inserted,
      updated,
      skipped: skippedRecords.length,
      skippedRecords: skippedRecords.slice(0, 20),
    }
  } catch (error) {
    await client.query('ROLLBACK')
    throw error
  } finally {
    client.release()
  }
}

const importVeiculoXmlFile = async (fileName) => {
  const sanitizedFileName = path.basename(normalizeRequestValue(fileName))

  if (!sanitizedFileName) {
    throw new Error('Nome do arquivo XML e obrigatorio.')
  }

  if (path.extname(sanitizedFileName).toLowerCase() !== '.xml') {
    throw new Error('Informe um arquivo XML valido.')
  }

  const resolvedPath = path.resolve(importXmlDirectory, sanitizedFileName)

  if (!resolvedPath.startsWith(importXmlDirectory)) {
    throw new Error('Arquivo XML invalido.')
  }

  const xmlContent = await readFile(resolvedPath, 'utf8')
  const parsedRecords = parseVeiculoXml(xmlContent)

  if (!parsedRecords.length) {
    throw new Error('Nenhum registro de veiculo foi encontrado no XML informado.')
  }

  const normalizedRecords = []
  const skippedRecords = []

  parsedRecords.forEach((record, index) => {
    try {
      normalizedRecords.push(normalizeImportedVeiculoRecord(record, index))
    } catch (error) {
      skippedRecords.push({
        index: index + 1,
        codigoXml: normalizeRequestValue(record.codigo),
        crmXml: normalizeRequestValue(record.crm),
        placasXml: normalizeRequestValue(record.placas),
        tipoDeVeiculoXml: normalizeRequestValue(record.tipoDeVeiculo),
        message: error instanceof Error ? error.message : `Registro ${index + 1}: erro ao validar o XML.`,
      })
    }
  })

  const client = await pool.connect()

  try {
    await client.query('BEGIN')
    await client.query('TRUNCATE TABLE veiculo_import_recusa RESTART IDENTITY')
    let inserted = 0
    let updated = 0

    for (const skippedRecord of skippedRecords) {
      await client.query(
        `INSERT INTO veiculo_import_recusa (
           arquivo_xml,
           linha_xml,
           codigo_xml,
           crm_xml,
           placas_xml,
           tipo_de_veiculo_xml,
           motivo_recusa,
           data_importacao
         )
         VALUES ($1, $2, NULLIF($3, ''), NULLIF($4, ''), NULLIF($5, ''), NULLIF($6, ''), $7, NOW())`,
        [
          sanitizedFileName,
          skippedRecord.index,
          skippedRecord.codigoXml,
          skippedRecord.crmXml,
          skippedRecord.placasXml,
          skippedRecord.tipoDeVeiculoXml,
          skippedRecord.message,
        ],
      )
    }

    for (const record of normalizedRecords) {
      const existingResult = await client.query('SELECT 1 FROM veiculo WHERE codigo = $1 LIMIT 1', [record.codigo])

      if (existingResult.rowCount > 0) {
        await client.query(
          `UPDATE veiculo
           SET crm = NULLIF($1, ''),
               placas = NULLIF($2, ''),
               ano = $3,
               cap_detran = $4,
               cap_teg = $5,
               cap_teg_creche = $6,
               cap_acessivel = $7,
               val_crm = NULLIF($8, '')::date,
               seguradora = NULLIF($9, ''),
               seguro_inicio = NULLIF($10, '')::date,
               seguro_termino = NULLIF($11, '')::date,
               tipo_de_bancada = NULLIF($12, ''),
               tipo_de_veiculo = NULLIF($13, ''),
               marca_modelo = NULLIF($14, ''),
               titular = NULLIF($15, ''),
               cnpj_cpf = NULLIF($16, ''),
               valor_veiculo = $17,
               os_especial = NULLIF($18, ''),
               data_modificacao = NOW()
           WHERE codigo = $19`,
          [
            record.crm,
            record.placas,
            record.ano,
            record.capDetran,
            record.capTeg,
            record.capTegCreche,
            record.capAcessivel,
            record.valCrm,
            record.seguradora,
            record.seguroInicio,
            record.seguroTermino,
            record.tipoDeBancada,
            record.tipoDeVeiculo,
            record.marcaModelo,
            record.titular,
            record.cnpjCpf,
            record.valorVeiculo,
            record.osEspecial,
            record.codigo,
          ],
        )
        updated += 1
        continue
      }

      await client.query(
        `INSERT INTO veiculo (
           codigo,
           crm,
           placas,
           ano,
           cap_detran,
           cap_teg,
           cap_teg_creche,
           cap_acessivel,
           val_crm,
           seguradora,
           seguro_inicio,
           seguro_termino,
           tipo_de_bancada,
           tipo_de_veiculo,
           marca_modelo,
           titular,
           cnpj_cpf,
           valor_veiculo,
           os_especial,
           data_inclusao,
           data_modificacao
         )
         VALUES ($1, NULLIF($2, ''), NULLIF($3, ''), $4, $5, $6, $7, $8, NULLIF($9, '')::date, NULLIF($10, ''), NULLIF($11, '')::date, NULLIF($12, '')::date, NULLIF($13, ''), NULLIF($14, ''), NULLIF($15, ''), NULLIF($16, ''), NULLIF($17, ''), $18, NULLIF($19, ''), NOW(), NOW())`,
        [
          record.codigo,
          record.crm,
          record.placas,
          record.ano,
          record.capDetran,
          record.capTeg,
          record.capTegCreche,
          record.capAcessivel,
          record.valCrm,
          record.seguradora,
          record.seguroInicio,
          record.seguroTermino,
          record.tipoDeBancada,
          record.tipoDeVeiculo,
          record.marcaModelo,
          record.titular,
          record.cnpjCpf,
          record.valorVeiculo,
          record.osEspecial,
        ],
      )
      inserted += 1
    }

    if (normalizedRecords.length) {
      await client.query('SELECT setval(\'veiculo_codigo_seq\', GREATEST(COALESCE((SELECT MAX(codigo) FROM veiculo), 0), 1), true)')
    }
    await client.query('COMMIT')

    return {
      fileName: sanitizedFileName,
      filePath: resolvedPath,
      total: parsedRecords.length,
      processed: normalizedRecords.length,
      inserted,
      updated,
      skipped: skippedRecords.length,
      skippedRecords: skippedRecords.slice(0, 20),
    }
  } catch (error) {
    await client.query('ROLLBACK')
    throw error
  } finally {
    client.release()
  }
}

const importCredenciamentoTermoXmlFile = async (fileName) => {
  const sanitizedFileName = path.basename(normalizeRequestValue(fileName))

  if (!sanitizedFileName) {
    throw new Error('Nome do arquivo XML e obrigatorio.')
  }

  if (path.extname(sanitizedFileName).toLowerCase() !== '.xml') {
    throw new Error('Informe um arquivo XML valido.')
  }

  const resolvedPath = path.resolve(importXmlDirectory, sanitizedFileName)

  if (!resolvedPath.startsWith(importXmlDirectory)) {
    throw new Error('Arquivo XML invalido.')
  }

  const xmlContent = await readFile(resolvedPath, 'utf8')
  const parsedRecords = parseCredenciamentoTermoXml(xmlContent)

  if (!parsedRecords.length) {
    throw new Error('Nenhum registro de credenciamento termo foi encontrado no XML informado.')
  }

  const normalizedRecords = []
  const skippedRecords = []

  parsedRecords.forEach((record, index) => {
    try {
      normalizedRecords.push(...buildImportedCredenciamentoTermoRecords(record, index))
    } catch (error) {
      skippedRecords.push({
        index: index + 1,
        codigoXml: normalizeRequestValue(record['C�digo']),
        credenciadoXml: normalizeRequestValue(record.Credenciado),
        aditivoXml: '',
        message: error instanceof Error ? error.message : `Registro ${index + 1}: erro ao validar o XML.`,
      })
    }
  })

  const client = await pool.connect()

  try {
    await client.query('BEGIN')
    await client.query(`TRUNCATE TABLE ${credenciamentoTermoImportRecusaTableName} RESTART IDENTITY`)
    let inserted = 0
    let updated = 0
    const allowedAditivosByCodigoXml = new Map()

    for (const record of normalizedRecords) {
      const currentAditivos = allowedAditivosByCodigoXml.get(record.codigoXml) ?? new Set()
      currentAditivos.add(record.aditivo)
      allowedAditivosByCodigoXml.set(record.codigoXml, currentAditivos)
    }

    for (const [codigoXml, aditivos] of allowedAditivosByCodigoXml.entries()) {
      await client.query(
        `DELETE FROM ${credenciamentoTermoTableName}
         WHERE codigo_xml = $1
           AND NOT (aditivo = ANY($2::integer[]))`,
        [codigoXml, Array.from(aditivos)],
      )
    }

    for (const skippedRecord of skippedRecords) {
      await client.query(
        `INSERT INTO ${credenciamentoTermoImportRecusaTableName} (
           arquivo_xml,
           linha_xml,
           codigo_xml,
           credenciado_xml,
           aditivo_xml,
           motivo_recusa,
           data_importacao
         )
         VALUES ($1, $2, NULLIF($3, ''), NULLIF($4, ''), NULLIF($5, ''), $6, NOW())`,
        [
          sanitizedFileName,
          skippedRecord.index,
          skippedRecord.codigoXml,
          skippedRecord.credenciadoXml,
          skippedRecord.aditivoXml,
          skippedRecord.message,
        ],
      )
    }

    for (const record of normalizedRecords) {
      const credenciadaItem = await findCredenciadaByName(record.credenciado, client)

      if (!credenciadaItem) {
        await client.query(
          `INSERT INTO ${credenciamentoTermoImportRecusaTableName} (
             arquivo_xml,
             linha_xml,
             codigo_xml,
             credenciado_xml,
             aditivo_xml,
             motivo_recusa,
             data_importacao
           )
           VALUES ($1, $2, $3, $4, $5, $6, NOW())`,
          [
            sanitizedFileName,
            record.codigoXml,
            String(record.codigoXml),
            record.credenciado,
            String(record.aditivo),
            'Credenciado nao encontrado na tabela credenciada.',
          ],
        )
        continue
      }

      const existingResult = await client.query(
        `SELECT codigo
         FROM ${credenciamentoTermoTableName}
         WHERE codigo_xml = $1
           AND aditivo = $2
         LIMIT 1`,
        [record.codigoXml, record.aditivo],
      )

      const values = [
        record.codigoXml,
        Number(credenciadaItem.codigo),
        record.termoAdesao,
        record.sei,
        record.aditivo,
        record.situacaoPublicacao,
        record.situacaoEmissao,
        record.inicioVigencia,
        record.terminoVigencia,
        record.compDataAditivo,
        record.statusAditivo,
        record.dataPubAditivo,
        record.checkAditivo,
        record.statusTermo,
        record.tipoTermo,
        record.logradouro,
        record.bairro,
        record.municipio,
        record.especificacaoSei,
        record.valorContrato,
        record.objeto,
        record.folhas,
        record.dataPublicacao,
        record.infoSei,
        record.valorContratoAtualizado,
        record.vencimentoGeral,
        record.mesRenovacao,
        record.tpOptante,
      ]

      if (existingResult.rowCount > 0) {
        await client.query(
          `UPDATE ${credenciamentoTermoTableName}
           SET codigo_xml = $1,
               credenciada_codigo = $2,
               termo_adesao = $3,
               sei = NULLIF($4, ''),
               aditivo = $5,
               situacao_publicacao = NULLIF($6, ''),
               situacao_emissao = NULLIF($7, ''),
               inicio_vigencia = NULLIF($8, '')::date,
               termino_vigencia = NULLIF($9, '')::date,
               comp_data_aditivo = NULLIF($10, '')::date,
               status_aditivo = NULLIF($11, ''),
               data_pub_aditivo = NULLIF($12, '')::date,
               check_aditivo = $13,
               status_termo = NULLIF($14, ''),
               tipo_termo = NULLIF($15, ''),
               logradouro = NULLIF($16, ''),
               bairro = NULLIF($17, ''),
               municipio = NULLIF($18, ''),
               especificacao_sei = NULLIF($19, ''),
               valor_contrato = $20,
                objeto = NULLIF($21, ''),
               data_publicacao = NULLIF($22, '')::date,
               info_sei = NULLIF($23, ''),
               valor_contrato_atualizado = $24,
               vencimento_geral = NULLIF($25, '')::date,
               mes_renovacao = NULLIF($26, ''),
               tp_optante = NULLIF($27, ''),
               data_modificacao = NOW()
              WHERE codigo = $28`,
          [...values, existingResult.rows[0].codigo],
        )
        updated += 1
        continue
      }

      await client.query(
        `INSERT INTO ${credenciamentoTermoTableName} (
           codigo_xml,
           credenciada_codigo,
           termo_adesao,
           sei,
           aditivo,
           situacao_publicacao,
           situacao_emissao,
           inicio_vigencia,
           termino_vigencia,
           comp_data_aditivo,
           status_aditivo,
           data_pub_aditivo,
           check_aditivo,
           status_termo,
           tipo_termo,
           logradouro,
           bairro,
           municipio,
           especificacao_sei,
           valor_contrato,
           objeto,
           data_publicacao,
           info_sei,
           valor_contrato_atualizado,
           vencimento_geral,
           mes_renovacao,
           tp_optante,
           data_inclusao,
           data_modificacao
         )
         VALUES ($1, $2, $3, NULLIF($4, ''), $5, NULLIF($6, ''), NULLIF($7, ''), NULLIF($8, '')::date, NULLIF($9, '')::date, NULLIF($10, '')::date, NULLIF($11, ''), NULLIF($12, '')::date, $13, NULLIF($14, ''), NULLIF($15, ''), NULLIF($16, ''), NULLIF($17, ''), NULLIF($18, ''), NULLIF($19, ''), $20, NULLIF($21, '')::date, NULLIF($22, ''), $23, NULLIF($24, '')::date, NULLIF($25, ''), NULLIF($26, ''), NOW(), NOW())`,
        values,
      )
      inserted += 1
    }

    if (normalizedRecords.length) {
      await client.query(`SELECT setval('${credenciamentoTermoCodigoSequenceName}', GREATEST(COALESCE((SELECT MAX(codigo) FROM ${credenciamentoTermoTableName}), 0), 1), true)`)
    }

    await client.query('COMMIT')

    return {
      fileName: sanitizedFileName,
      filePath: resolvedPath,
      total: parsedRecords.length,
      processed: normalizedRecords.length,
      inserted,
      updated,
      skipped: skippedRecords.length,
      skippedRecords: skippedRecords.slice(0, 20),
    }
  } catch (error) {
    await client.query('ROLLBACK')
    throw error
  } finally {
    client.release()
  }
}

const fetchCredenciamentoTermoItemByCodigo = async (executor, codigo) => {
  const normalizedCodigo = normalizeCondutorCodigo(codigo)

  if (!Number.isInteger(normalizedCodigo) || normalizedCodigo <= 0) {
    return null
  }

  const result = await executor.query(
    `SELECT
       ${credenciamentoTermoSelectClause}
     FROM ${credenciamentoTermoTableName}
     WHERE codigo = $1
     LIMIT 1`,
    [normalizedCodigo],
  )

  const item = result.rows[0] ?? null

  if (!item) {
    return null
  }

  return {
    ...item,
    valorContratoExtenso: buildCurrencyExtenso(item.valor_contrato),
  }
}

const fetchVinculoCondutorItemById = async (executor, id) => {
  const normalizedId = normalizeCondutorCodigo(id)

  if (!Number.isInteger(normalizedId) || normalizedId <= 0) {
    return null
  }

  const result = await executor.query(
    `SELECT
       ${vinculoCondutorSelectClause}
     FROM ${vinculoCondutorTableName} vc
     LEFT JOIN credenciada cr
       ON cr.codigo = vc.credenciada_codigo
     LEFT JOIN condutor cd
       ON cd.codigo = vc.condutor_codigo
     WHERE vc.id = $1
     LIMIT 1`,
    [normalizedId],
  )

  return result.rows[0] ?? null
}

const validateVinculoCondutorPayload = async ({
  termoAdesao,
  numOs,
  revisao,
  credenciadaCodigo,
  credenciado,
  dataOs,
  dataAdmissaoCondutor,
  condutorCodigo,
  cpfCondutor,
}, executor = pool) => {
  const errors = {}
  const normalizedTermoAdesao = normalizeRequestValue(termoAdesao).toUpperCase().slice(0, 255)
  const normalizedNumOs = normalizeRequestValue(numOs).toUpperCase().slice(0, 10)
  const normalizedRevisao = normalizeRequestValue(revisao).toUpperCase().slice(0, 30)
  const normalizedCpfCondutor = normalizeCpf(cpfCondutor)
  const normalizedDataOs = normalizeXmlDateInput(dataOs) || normalizeRequestValue(dataOs)
  const normalizedDataAdmissaoCondutor = normalizeXmlDateInput(dataAdmissaoCondutor) || normalizeRequestValue(dataAdmissaoCondutor)

  if (normalizedDataOs && !isDateInputValid(normalizedDataOs)) {
    errors.dataOs = 'Informe uma data_OS valida.'
  }

  if (normalizedDataAdmissaoCondutor && !isDateInputValid(normalizedDataAdmissaoCondutor)) {
    errors.dataAdmissaoCondutor = 'Informe uma data de admissao valida.'
  }

  let credenciadaItem = null
  const normalizedCredenciadaCodigo = normalizeCondutorCodigo(credenciadaCodigo)

  if (Number.isInteger(normalizedCredenciadaCodigo) && normalizedCredenciadaCodigo > 0) {
    credenciadaItem = await findCredenciadaByCodigo(normalizedCredenciadaCodigo, executor)
  }

  if (!credenciadaItem) {
    errors.credenciado = 'Credenciado nao encontrado na tabela credenciada.'
  }

  let condutorItem = null
  const normalizedCondutorCodigoValue = normalizeCondutorCodigo(condutorCodigo)

  if (Number.isInteger(normalizedCondutorCodigoValue) && normalizedCondutorCodigoValue > 0) {
    condutorItem = await findCondutorByCodigo(normalizedCondutorCodigoValue, executor)
  }

  if (!condutorItem && normalizedCpfCondutor && isCpfValid(normalizedCpfCondutor)) {
    condutorItem = await findCondutorByCpf(normalizedCpfCondutor, executor)
  }

  if (!normalizedCpfCondutor) {
    errors.cpfCondutor = 'CPF do condutor e obrigatorio.'
  } else if (!isCpfValid(normalizedCpfCondutor)) {
    errors.cpfCondutor = 'CPF do condutor deve conter 11 digitos.'
  } else if (!condutorItem) {
    errors.cpfCondutor = 'Condutor nao encontrado para o CPF informado.'
  }

  if (Object.keys(errors).length > 0) {
    return {
      status: 400,
      payload: {
        message: 'Corrija os campos do vinculo do condutor para continuar.',
        errors,
      },
    }
  }

  return {
    status: 200,
    payload: {
      termoAdesao: normalizedTermoAdesao,
      numOs: normalizedNumOs,
      revisao: normalizedRevisao,
      credenciadaCodigo: Number(credenciadaItem.codigo),
      credenciado: credenciadaItem.credenciado,
      dataOs: normalizedDataOs,
      dataAdmissaoCondutor: normalizedDataAdmissaoCondutor,
      condutorCodigo: Number(condutorItem.codigo),
      cpfCondutor: condutorItem.cpf_condutor,
      condutor: condutorItem.condutor,
    },
  }
}

const getVinculoCondutorPersistenceError = (error, fallbackMessage) => {
  if (error && typeof error === 'object') {
    const databaseError = error
    const constraintName = typeof databaseError.constraint === 'string' ? databaseError.constraint : ''
    const errorCode = typeof databaseError.code === 'string' ? databaseError.code : ''
    const errorMessage = error instanceof Error ? error.message : ''

    const isUniqueViolation = errorCode === '23505' || /duplicate key|unicidade/i.test(errorMessage)

    if (constraintName === 'vinculo_condutor_importado_uk' || constraintName === 'vinculo_condutor_chave_unique_idx' || isUniqueViolation) {
      return {
        status: 409,
        message: 'Vinculo do condutor ja cadastrado.',
      }
    }
  }

  return {
    status: 500,
    message: fallbackMessage,
  }
}

const fetchVinculoMonitorItemById = async (executor, id) => {
  const normalizedId = normalizeCondutorCodigo(id)

  if (!Number.isInteger(normalizedId) || normalizedId <= 0) {
    return null
  }

  const result = await executor.query(
    `SELECT
       ${vinculoMonitorSelectClause}
     FROM ${vinculoMonitorTableName} vm
     LEFT JOIN credenciada cr
       ON cr.codigo = vm.credenciada_codigo
     LEFT JOIN monitor mt
       ON mt.codigo = vm.monitor_codigo
     WHERE vm.id = $1
     LIMIT 1`,
    [normalizedId],
  )

  return result.rows[0] ?? null
}

const validateVinculoMonitorPayload = async ({
  termoAdesao,
  numOs,
  revisao,
  credenciadaCodigo,
  credenciado,
  dataOs,
  dataAdmissaoMonitor,
  monitorCodigo,
  cpfMonitor,
}, executor = pool) => {
  const errors = {}
  const normalizedTermoAdesao = normalizeRequestValue(termoAdesao).toUpperCase().slice(0, 255)
  const normalizedNumOs = normalizeRequestValue(numOs).toUpperCase().slice(0, 10)
  const normalizedRevisao = normalizeRequestValue(revisao).toUpperCase().slice(0, 30)
  const normalizedCpfMonitor = normalizeCpf(cpfMonitor)
  const normalizedDataOs = normalizeXmlDateInput(dataOs) || normalizeRequestValue(dataOs)
  const normalizedDataAdmissaoMonitor = normalizeXmlDateInput(dataAdmissaoMonitor) || normalizeRequestValue(dataAdmissaoMonitor)

  if (normalizedDataOs && !isDateInputValid(normalizedDataOs)) {
    errors.dataOs = 'Informe uma data_OS valida.'
  }

  if (normalizedDataAdmissaoMonitor && !isDateInputValid(normalizedDataAdmissaoMonitor)) {
    errors.dataAdmissaoMonitor = 'Informe uma data de admissao valida.'
  }

  let credenciadaItem = null
  const normalizedCredenciadaCodigo = normalizeCondutorCodigo(credenciadaCodigo)

  if (Number.isInteger(normalizedCredenciadaCodigo) && normalizedCredenciadaCodigo > 0) {
    credenciadaItem = await findCredenciadaByCodigo(normalizedCredenciadaCodigo, executor)
  }

  if (!credenciadaItem) {
    errors.credenciado = 'Credenciado nao encontrado na tabela credenciada.'
  }

  let monitorItem = null
  const normalizedMonitorCodigoValue = normalizeCondutorCodigo(monitorCodigo)

  if (Number.isInteger(normalizedMonitorCodigoValue) && normalizedMonitorCodigoValue > 0) {
    monitorItem = await findMonitorByCodigo(normalizedMonitorCodigoValue, executor)
  }

  if (!monitorItem && normalizedCpfMonitor && isCpfValid(normalizedCpfMonitor)) {
    monitorItem = await findMonitorByCpf(normalizedCpfMonitor, executor)
  }

  if (!normalizedCpfMonitor) {
    errors.cpfMonitor = 'CPF do monitor e obrigatorio.'
  } else if (!isCpfValid(normalizedCpfMonitor)) {
    errors.cpfMonitor = 'CPF do monitor deve conter 11 digitos.'
  } else if (!monitorItem) {
    errors.cpfMonitor = 'Monitor nao encontrado para o CPF informado.'
  }

  if (Object.keys(errors).length > 0) {
    return {
      status: 400,
      payload: {
        message: 'Corrija os campos do vinculo do monitor para continuar.',
        errors,
      },
    }
  }

  return {
    status: 200,
    payload: {
      termoAdesao: normalizedTermoAdesao,
      numOs: normalizedNumOs,
      revisao: normalizedRevisao,
      credenciadaCodigo: Number(credenciadaItem.codigo),
      credenciado: credenciadaItem.credenciado,
      dataOs: normalizedDataOs,
      dataAdmissaoMonitor: normalizedDataAdmissaoMonitor,
      monitorCodigo: Number(monitorItem.codigo),
      cpfMonitor: monitorItem.cpf_monitor,
      monitor: monitorItem.monitor,
    },
  }
}

const getVinculoMonitorPersistenceError = (error, fallbackMessage) => {
  if (error && typeof error === 'object') {
    const databaseError = error
    const constraintName = typeof databaseError.constraint === 'string' ? databaseError.constraint : ''
    const errorCode = typeof databaseError.code === 'string' ? databaseError.code : ''
    const errorMessage = error instanceof Error ? error.message : ''

    const isUniqueViolation = errorCode === '23505' || /duplicate key|unicidade/i.test(errorMessage)

    if (constraintName === 'vinculo_monitor_importado_uk' || constraintName === 'vinculo_monitor_chave_unique_idx' || isUniqueViolation) {
      return {
        status: 409,
        message: 'Vinculo do monitor ja cadastrado.',
      }
    }
  }

  return {
    status: 500,
    message: fallbackMessage,
  }
}

const importVinculoMonitorXmlFile = async (fileName) => {
  const sanitizedFileName = path.basename(normalizeRequestValue(fileName))

  if (!sanitizedFileName) {
    throw new Error('Nome do arquivo XML e obrigatorio.')
  }

  if (path.extname(sanitizedFileName).toLowerCase() !== '.xml') {
    throw new Error('Informe um arquivo XML valido.')
  }

  const resolvedPath = path.resolve(importXmlDirectory, sanitizedFileName)

  if (!resolvedPath.startsWith(importXmlDirectory)) {
    throw new Error('Arquivo XML invalido.')
  }

  const xmlContent = await readFile(resolvedPath, 'utf8')
  const parsedRecords = parseVinculoMonitorXml(xmlContent)

  if (!parsedRecords.length) {
    throw new Error('Nenhum registro de vinculo do monitor foi encontrado no XML informado.')
  }

  const skippedRecords = []
  let inserted = 0
  let updated = 0
  const client = await pool.connect()

  try {
    await client.query('BEGIN')
    await client.query(`TRUNCATE TABLE ${vinculoMonitorImportRecusaTableName} RESTART IDENTITY`)

    for (let index = 0; index < parsedRecords.length; index += 1) {
      const record = parsedRecords[index]

      try {
        const normalizedCodigoXml = normalizeRequestValue(record.codigoXml)
        const normalizedEmpregador = normalizeCredenciadaText(record.empregador, 255)
        const normalizedCpfMonitor = normalizeCpf(record.cpfMonitor)
        const normalizedDataOs = normalizeXmlDateInput(record.dataOs)
        const normalizedAdmissao = normalizeXmlDateInput(record.admissao)

        if (!normalizedEmpregador) {
          throw new Error('Empregador nao informado no XML.')
        }

        if (!normalizedCpfMonitor || !isCpfValid(normalizedCpfMonitor)) {
          throw new Error('CPF do monitor invalido no XML.')
        }

        if (normalizedDataOs && !isDateInputValid(normalizedDataOs)) {
          throw new Error('data_OS invalida no XML.')
        }

        if (normalizedAdmissao && !isDateInputValid(normalizedAdmissao)) {
          throw new Error('Data de admissao invalida no XML.')
        }

        const credenciadaItem = await findCredenciadaByName(normalizedEmpregador, client)

        if (!credenciadaItem) {
          throw new Error('Empregador nao encontrado na tabela de credenciada.')
        }

        const monitorItem = await findMonitorByCpf(normalizedCpfMonitor, client)

        if (!monitorItem) {
          throw new Error('CPF do monitor nao encontrado na tabela de monitor.')
        }

        if (normalizedCodigoXml) {
          const existingByCodigoXml = await client.query(
            `SELECT id FROM ${vinculoMonitorTableName} WHERE BTRIM(codigo_xml) = $1 LIMIT 1`,
            [normalizedCodigoXml],
          )
          if (existingByCodigoXml.rowCount > 0) {
            updated += 1
            continue
          }

          // If an unclaimed (codigo_xml IS NULL) record exists for the same pair, claim it
          const unclaimedResult = await client.query(
            `SELECT id
               FROM ${vinculoMonitorTableName}
              WHERE credenciada_codigo = $1
                AND monitor_codigo = $2
                AND COALESCE(BTRIM(termo_adesao), '') = ''
                AND COALESCE(BTRIM(num_os), '') = ''
                AND COALESCE(BTRIM(revisao), '') = ''
                AND codigo_xml IS NULL
              ORDER BY id ASC
              LIMIT 1`,
            [Number(credenciadaItem.codigo), Number(monitorItem.codigo)],
          )

          if (unclaimedResult.rowCount > 0) {
            await client.query(
              `UPDATE ${vinculoMonitorTableName}
                  SET codigo_xml = $1,
                      data_os = NULLIF($2, '')::date,
                      data_admissao_monitor = NULLIF($3, '')::date
                WHERE id = $4`,
              [normalizedCodigoXml, normalizedDataOs, normalizedAdmissao, unclaimedResult.rows[0].id],
            )
            updated += 1
            continue
          }

          await client.query(
            `INSERT INTO ${vinculoMonitorTableName} (
               termo_adesao,
               num_os,
               revisao,
               credenciada_codigo,
               data_os,
               data_admissao_monitor,
               monitor_codigo,
               codigo_xml,
               data_inclusao
             )
             VALUES (NULL, NULL, NULL, $1, NULLIF($2, '')::date, NULLIF($3, '')::date, $4, $5, NOW())`,
            [Number(credenciadaItem.codigo), normalizedDataOs, normalizedAdmissao, Number(monitorItem.codigo), normalizedCodigoXml],
          )

          inserted += 1
          continue
        }

        const existingResult = await client.query(
          `SELECT id
             FROM ${vinculoMonitorTableName}
            WHERE credenciada_codigo = $1
              AND monitor_codigo = $2
              AND COALESCE(BTRIM(termo_adesao), '') = ''
              AND COALESCE(BTRIM(num_os), '') = ''
              AND COALESCE(BTRIM(revisao), '') = ''
              AND codigo_xml IS NULL
            ORDER BY id ASC
            LIMIT 1`,
          [Number(credenciadaItem.codigo), Number(monitorItem.codigo)],
        )

        if (existingResult.rowCount > 0) {
          await client.query(
            `UPDATE ${vinculoMonitorTableName}
                SET data_os = NULLIF($1, '')::date,
                    data_admissao_monitor = NULLIF($2, '')::date
              WHERE id = $3`,
            [normalizedDataOs, normalizedAdmissao, existingResult.rows[0].id],
          )
          updated += 1
          continue
        }

        await client.query(
          `INSERT INTO ${vinculoMonitorTableName} (
             termo_adesao,
             num_os,
             revisao,
             credenciada_codigo,
             data_os,
             data_admissao_monitor,
             monitor_codigo,
             data_inclusao
           )
           VALUES (NULL, NULL, NULL, $1, NULLIF($2, '')::date, NULLIF($3, '')::date, $4, NOW())`,
          [Number(credenciadaItem.codigo), normalizedDataOs, normalizedAdmissao, Number(monitorItem.codigo)],
        )

        inserted += 1
      } catch (error) {
        skippedRecords.push({
          index: index + 1,
          codigoXml: normalizeRequestValue(record.codigoXml),
          empregadorXml: normalizeRequestValue(record.empregador),
          cpfMonitorXml: normalizeRequestValue(record.cpfMonitor),
          dataOsXml: normalizeRequestValue(record.dataOs),
          admissaoXml: normalizeRequestValue(record.admissao),
          message: error instanceof Error ? error.message : `Registro ${index + 1}: erro ao validar o XML.`,
        })
      }
    }

    for (const skippedRecord of skippedRecords) {
      await client.query(
        `INSERT INTO ${vinculoMonitorImportRecusaTableName} (
           arquivo_xml,
           linha_xml,
           codigo_xml,
           empregador_xml,
           cpf_monitor_xml,
           data_os_xml,
           admissao_xml,
           motivo_recusa,
           data_importacao
         )
         VALUES ($1, $2, NULLIF($3, ''), NULLIF($4, ''), NULLIF($5, ''), NULLIF($6, ''), NULLIF($7, ''), $8, NOW())`,
        [
          sanitizedFileName,
          skippedRecord.index,
          skippedRecord.codigoXml,
          skippedRecord.empregadorXml,
          skippedRecord.cpfMonitorXml,
          skippedRecord.dataOsXml,
          skippedRecord.admissaoXml,
          skippedRecord.message,
        ],
      )
    }

    await client.query('COMMIT')

    return {
      fileName: sanitizedFileName,
      filePath: resolvedPath,
      total: parsedRecords.length,
      inserted,
      updated,
      skipped: skippedRecords.length,
      skippedRecords: skippedRecords.slice(0, 20),
    }
  } catch (error) {
    await client.query('ROLLBACK')
    throw error
  } finally {
    client.release()
  }
}

const importVinculoCondutorXmlFile = async (fileName) => {
  const sanitizedFileName = path.basename(normalizeRequestValue(fileName))

  if (!sanitizedFileName) {
    throw new Error('Nome do arquivo XML e obrigatorio.')
  }

  if (path.extname(sanitizedFileName).toLowerCase() !== '.xml') {
    throw new Error('Informe um arquivo XML valido.')
  }

  const resolvedPath = path.resolve(importXmlDirectory, sanitizedFileName)

  if (!resolvedPath.startsWith(importXmlDirectory)) {
    throw new Error('Arquivo XML invalido.')
  }

  const xmlContent = await readFile(resolvedPath, 'utf8')
  const parsedRecords = parseVinculoCondutorXml(xmlContent)

  if (!parsedRecords.length) {
    throw new Error('Nenhum registro de vinculo do condutor foi encontrado no XML informado.')
  }

  const skippedRecords = []
  let inserted = 0
  let updated = 0
  const client = await pool.connect()

  try {
    await client.query('BEGIN')
    await client.query(`TRUNCATE TABLE ${vinculoCondutorImportRecusaTableName} RESTART IDENTITY`)

    for (let index = 0; index < parsedRecords.length; index += 1) {
      const record = parsedRecords[index]

      try {
        const normalizedCodigoXml = normalizeRequestValue(record.codigoXml)
        const normalizedEmpregador = normalizeCredenciadaText(record.empregador, 255)
        const normalizedCpfCondutor = normalizeCpf(record.cpfCondutor)
        const normalizedDataOs = normalizeXmlDateInput(record.dataOs)
        const normalizedAdmissao = normalizeXmlDateInput(record.admissao)

        if (!normalizedCpfCondutor) {
          throw new Error('CPF do condutor nao informado no XML.')
        }

        if (normalizedDataOs && !isDateInputValid(normalizedDataOs)) {
          throw new Error('data_OS invalida no XML.')
        }

        if (normalizedAdmissao && !isDateInputValid(normalizedAdmissao)) {
          throw new Error('Data de admissao invalida no XML.')
        }

        const credenciadaItem = await findCredenciadaByName(normalizedEmpregador, client)

        if (!credenciadaItem) {
          throw new Error('Empregador nao encontrado na tabela de credenciada.')
        }

        let condutorItem = await findCondutorByCpf(normalizedCpfCondutor, client)

        if (!condutorItem) {
          const newCondutorResult = await client.query(
            `INSERT INTO condutor (condutor, cpf_condutor, crmc, validade_crmc, validade_curso, data_inclusao, data_modificacao)
             VALUES ($1, $2, '', '9999-12-31', '9999-12-31', NOW(), NOW())
             RETURNING codigo, cpf_condutor, condutor`,
            [normalizedCpfCondutor, normalizedCpfCondutor],
          )
          condutorItem = newCondutorResult.rows[0]
        }

        if (normalizedCodigoXml) {
          const existingByCodigoXml = await client.query(
            `SELECT id FROM ${vinculoCondutorTableName} WHERE BTRIM(codigo_xml) = $1 LIMIT 1`,
            [normalizedCodigoXml],
          )
          if (existingByCodigoXml.rowCount > 0) {
            updated += 1
            continue
          }

          // If an unclaimed (codigo_xml IS NULL) record exists for the same pair, claim it
          const unclaimedResult = await client.query(
            `SELECT id
               FROM ${vinculoCondutorTableName}
              WHERE credenciada_codigo = $1
                AND condutor_codigo = $2
                AND COALESCE(BTRIM(termo_adesao), '') = ''
                AND COALESCE(BTRIM(num_os), '') = ''
                AND COALESCE(BTRIM(revisao), '') = ''
                AND codigo_xml IS NULL
              ORDER BY id ASC
              LIMIT 1`,
            [Number(credenciadaItem.codigo), Number(condutorItem.codigo)],
          )

          if (unclaimedResult.rowCount > 0) {
            await client.query(
              `UPDATE ${vinculoCondutorTableName}
                  SET codigo_xml = $1,
                      data_os = NULLIF($2, '')::date,
                      data_admissao_condutor = NULLIF($3, '')::date
                WHERE id = $4`,
              [normalizedCodigoXml, normalizedDataOs, normalizedAdmissao, unclaimedResult.rows[0].id],
            )
            updated += 1
            continue
          }

          await client.query(
            `INSERT INTO ${vinculoCondutorTableName} (
               termo_adesao,
               num_os,
               revisao,
               credenciada_codigo,
               data_os,
               data_admissao_condutor,
               condutor_codigo,
               codigo_xml,
               data_inclusao
             )
             VALUES (NULL, NULL, NULL, $1, NULLIF($2, '')::date, NULLIF($3, '')::date, $4, $5, NOW())`,
            [Number(credenciadaItem.codigo), normalizedDataOs, normalizedAdmissao, Number(condutorItem.codigo), normalizedCodigoXml],
          )

          inserted += 1
          continue
        }

        const existingResult = await client.query(
          `SELECT id
             FROM ${vinculoCondutorTableName}
            WHERE credenciada_codigo = $1
              AND condutor_codigo = $2
              AND COALESCE(BTRIM(termo_adesao), '') = ''
              AND COALESCE(BTRIM(num_os), '') = ''
              AND COALESCE(BTRIM(revisao), '') = ''
              AND codigo_xml IS NULL
            ORDER BY id ASC
            LIMIT 1`,
          [Number(credenciadaItem.codigo), Number(condutorItem.codigo)],
        )

        if (existingResult.rowCount > 0) {
          await client.query(
            `UPDATE ${vinculoCondutorTableName}
                SET data_os = NULLIF($1, '')::date,
                    data_admissao_condutor = NULLIF($2, '')::date
              WHERE id = $3`,
            [normalizedDataOs, normalizedAdmissao, existingResult.rows[0].id],
          )
          updated += 1
          continue
        }

        await client.query(
          `INSERT INTO ${vinculoCondutorTableName} (
             termo_adesao,
             num_os,
             revisao,
             credenciada_codigo,
             data_os,
             data_admissao_condutor,
             condutor_codigo,
             data_inclusao
           )
           VALUES (NULL, NULL, NULL, $1, NULLIF($2, '')::date, NULLIF($3, '')::date, $4, NOW())`,
          [Number(credenciadaItem.codigo), normalizedDataOs, normalizedAdmissao, Number(condutorItem.codigo)],
        )

        inserted += 1
      } catch (error) {
        skippedRecords.push({
          index: index + 1,
          codigoXml: normalizeRequestValue(record.codigoXml),
          empregadorXml: normalizeRequestValue(record.empregador),
          cpfCondutorXml: normalizeRequestValue(record.cpfCondutor),
          dataOsXml: normalizeRequestValue(record.dataOs),
          admissaoXml: normalizeRequestValue(record.admissao),
          message: error instanceof Error ? error.message : `Registro ${index + 1}: erro ao validar o XML.`,
        })
      }
    }

    for (const skippedRecord of skippedRecords) {
      await client.query(
        `INSERT INTO ${vinculoCondutorImportRecusaTableName} (
           arquivo_xml,
           linha_xml,
           codigo_xml,
           empregador_xml,
           cpf_condutor_xml,
           data_os_xml,
           admissao_xml,
           motivo_recusa,
           data_importacao
         )
         VALUES ($1, $2, NULLIF($3, ''), NULLIF($4, ''), NULLIF($5, ''), NULLIF($6, ''), NULLIF($7, ''), $8, NOW())`,
        [
          sanitizedFileName,
          skippedRecord.index,
          skippedRecord.codigoXml,
          skippedRecord.empregadorXml,
          skippedRecord.cpfCondutorXml,
          skippedRecord.dataOsXml,
          skippedRecord.admissaoXml,
          skippedRecord.message,
        ],
      )
    }

    await client.query('COMMIT')

    return {
      fileName: sanitizedFileName,
      filePath: resolvedPath,
      total: parsedRecords.length,
      processed: parsedRecords.length - skippedRecords.length,
      inserted,
      updated,
      skipped: skippedRecords.length,
      skippedRecords: skippedRecords.slice(0, 20),
    }
  } catch (error) {
    await client.query('ROLLBACK')
    throw error
  } finally {
    client.release()
  }
}

const seedTitularTableFromXmlIfEmpty = async () => {
  const countResult = await pool.query(`SELECT COUNT(*)::int AS total FROM ${titularTableName}`)
  const total = countResult.rows[0]?.total ?? 0

  if (total > 0) {
    return null
  }

  const resolvedPath = path.resolve(importXmlDirectory, 'titular.xml')
  const xmlContent = await readFile(resolvedPath, 'utf8')
  const parsedRecords = parseTitularXml(xmlContent)

  if (!parsedRecords.length) {
    throw new Error('Nenhum registro de titular do CRM foi encontrado no XML informado.')
  }

  const normalizedRecords = parsedRecords.map((record, index) => normalizeImportedTitularRecord(record, index))
  const client = await pool.connect()

  try {
    await client.query('BEGIN')

    for (const record of normalizedRecords) {
      await client.query(
        `INSERT INTO ${titularTableName} (
           codigo,
           cnpj_cpf,
           titular,
           data_inclusao,
           data_modificacao
         )
         VALUES ($1, $2, $3, NOW(), NOW())
         ON CONFLICT (codigo) DO UPDATE
         SET cnpj_cpf = EXCLUDED.cnpj_cpf,
             titular = EXCLUDED.titular,
             data_modificacao = NOW()`,
        [record.codigo, record.cnpjCpf, record.titular],
      )
    }

    await client.query(`SELECT setval('${titularSequenceName}', GREATEST(COALESCE((SELECT MAX(codigo) FROM ${titularTableName}), 0), 1), true)`)
    await client.query('COMMIT')

    return {
      fileName: 'titular.xml',
      filePath: resolvedPath,
      total: parsedRecords.length,
      processed: normalizedRecords.length,
      inserted: normalizedRecords.length,
      updated: 0,
    }
  } catch (error) {
    await client.query('ROLLBACK')
    throw error
  } finally {
    client.release()
  }
}

const ensureCredenciadaImportCepExists = async (record) => {
  if (!record.cep) return null
  const result = await pool.query('SELECT 1 FROM ceps WHERE cep = $1 LIMIT 1', [record.cep])
  return result.rowCount > 0 ? record.cep : null
}

const importCredenciadaXmlFile = async (fileName) => {
  const sanitizedFileName = path.basename(normalizeRequestValue(fileName))

  if (!sanitizedFileName) {
    throw new Error('Nome do arquivo XML e obrigatorio.')
  }

  if (path.extname(sanitizedFileName).toLowerCase() !== '.xml') {
    throw new Error('Informe um arquivo XML valido.')
  }

  const resolvedPath = path.resolve(importXmlDirectory, sanitizedFileName)

  if (!resolvedPath.startsWith(importXmlDirectory)) {
    throw new Error('Arquivo XML invalido.')
  }

  const xmlContent = await readFile(resolvedPath, 'utf8')
  const parsedRecords = parseCredenciadaXml(xmlContent)

  if (!parsedRecords.length) {
    throw new Error('Nenhum registro de credenciada foi encontrado no XML informado.')
  }

  const normalizedRecords = []
  const skippedRecords = []

  parsedRecords.forEach((record, index) => {
    try {
      normalizedRecords.push(normalizeImportedCredenciadaRecord(record, index))
    } catch (error) {
      skippedRecords.push({
        index: index + 1,
        codigoXml: normalizeRequestValue(record.codigo),
        credenciadoXml: normalizeRequestValue(record.credenciado),
        cnpjCpfXml: normalizeRequestValue(record.cnpjCpf),
        representanteXml: normalizeRequestValue(record.representante),
        statusXml: normalizeRequestValue(record.status),
        message: error instanceof Error ? error.message : `Registro ${index + 1}: erro ao validar o XML.`,
      })
    }
  })

  const client = await pool.connect()

  try {
    await client.query('BEGIN')
    await client.query('TRUNCATE TABLE credenciada_import_recusa RESTART IDENTITY')
    let inserted = 0
    let updated = 0

    for (const skippedRecord of skippedRecords) {
      await client.query(
        `INSERT INTO credenciada_import_recusa (
           arquivo_xml,
           linha_xml,
           codigo_xml,
           credenciado_xml,
           cnpj_cpf_xml,
           representante_xml,
           status_xml,
           motivo_recusa,
           data_importacao
         )
         VALUES ($1, $2, NULLIF($3, ''), NULLIF($4, ''), NULLIF($5, ''), NULLIF($6, ''), NULLIF($7, ''), $8, NOW())`,
        [
          sanitizedFileName,
          skippedRecord.index,
          skippedRecord.codigoXml,
          skippedRecord.credenciadoXml,
          skippedRecord.cnpjCpfXml,
          skippedRecord.representanteXml,
          skippedRecord.statusXml,
          skippedRecord.message,
        ],
      )
    }

    for (const record of normalizedRecords) {
      const existingResult = await client.query('SELECT 1 FROM credenciada WHERE codigo = $1 LIMIT 1', [record.codigo])
      const ensuredCep = await ensureCredenciadaImportCepExists(record)

      if (existingResult.rowCount > 0) {
        await client.query(
          `UPDATE credenciada
           SET placa = $1,
               empresa = $2,
               condutor = $3,
               tipo_pessoa = $4,
               credenciado = $5,
               cnpj_cpf = $6,
               cep = NULLIF($7, ''),
               email = NULLIF($8, ''),
               telefone_01 = $9,
               telefone_02 = NULLIF($10, ''),
               representante = NULLIF($11, ''),
               cpf_representante = NULLIF($12, ''),
                 status = NULLIF($13, ''),
               data_modificacao = NOW()
               WHERE codigo = $14`,
          [
            record.placa,
            record.empresa,
            record.condutor,
            record.tipoPessoa,
            record.credenciado,
            record.cnpjCpf,
            ensuredCep,
            record.email,
            record.telefone1,
            record.telefone2,
            record.representante,
            record.cpfRepresentante,
            record.status,
            record.codigo,
          ],
        )
        updated += 1
        continue
      }

      await client.query(
        `INSERT INTO credenciada (
           codigo,
           placa,
           empresa,
           condutor,
           tipo_pessoa,
           credenciado,
           cnpj_cpf,
           cep,
           email,
           telefone_01,
           telefone_02,
           representante,
           cpf_representante,
           status,
           data_inclusao,
           data_modificacao
         )
         VALUES ($1, $2, $3, $4, $5, $6, $7, NULLIF($8, ''), $9, NULLIF($10, ''), NULLIF($11, ''), NULLIF($12, ''), NULLIF($13, ''), NULLIF($14, ''), NOW(), NOW())`,
        [
          record.codigo,
          record.placa,
          record.empresa,
          record.condutor,
          record.tipoPessoa,
          record.credenciado,
          record.cnpjCpf,
          ensuredCep,
          record.email,
          record.telefone1,
          record.telefone2,
          record.representante,
          record.cpfRepresentante,
          record.rgRepresentante,
          record.status,
        ],
      )
      inserted += 1
    }

    if (normalizedRecords.length) {
      await client.query('SELECT setval(\'credenciada_codigo_seq\', GREATEST(COALESCE((SELECT MAX(codigo) FROM credenciada), 0), 1), true)')
    }
    await client.query('COMMIT')

    return {
      fileName: sanitizedFileName,
      filePath: resolvedPath,
      total: parsedRecords.length,
      processed: normalizedRecords.length,
      inserted,
      updated,
      skipped: skippedRecords.length,
      skippedRecords: skippedRecords.slice(0, 20),
    }
  } catch (error) {
    await client.query('ROLLBACK')
    throw error
  } finally {
    client.release()
  }
}

const importTrocaXmlFile = async (fileName) => {
  const sanitizedFileName = path.basename(normalizeRequestValue(fileName))

  if (!sanitizedFileName) {
    throw new Error('Nome do arquivo XML e obrigatorio.')
  }

  if (path.extname(sanitizedFileName).toLowerCase() !== '.xml') {
    throw new Error('Informe um arquivo XML valido.')
  }

  const resolvedPath = path.resolve(importXmlDirectory, sanitizedFileName)

  if (!resolvedPath.startsWith(importXmlDirectory)) {
    throw new Error('Arquivo XML invalido.')
  }

  const xmlContent = await readFile(resolvedPath, 'utf8')
  const parsedRecords = parseTrocaXml(xmlContent)

  if (!parsedRecords.length) {
    throw new Error('Nenhum registro de troca foi encontrado no XML informado.')
  }

  const normalizedRecords = parsedRecords.map((record, index) => normalizeImportedTrocaRecord(record, index))
  const client = await pool.connect()

  try {
    await client.query('BEGIN')
    let inserted = 0
    let updated = 0

    for (const record of normalizedRecords) {
      const existingResult = await client.query('SELECT 1 FROM tipo_troca WHERE codigo = $1 LIMIT 1', [record.codigo])

      if (existingResult.rowCount > 0) {
        await client.query(
          `UPDATE tipo_troca
           SET controle = $1,
               lista = $2,
               data_modificacao = NOW()
           WHERE codigo = $3`,
          [record.controle, record.lista, record.codigo],
        )
        updated += 1
        continue
      }

      await client.query(
        `INSERT INTO tipo_troca (
           codigo,
           controle,
           lista,
           data_inclusao,
           data_modificacao
         )
         VALUES ($1, $2, $3, NOW(), NOW())`,
        [record.codigo, record.controle, record.lista],
      )
      inserted += 1
    }

    await client.query('COMMIT')

    return {
      fileName: sanitizedFileName,
      filePath: resolvedPath,
      total: parsedRecords.length,
      processed: normalizedRecords.length,
      inserted,
      updated,
    }
  } catch (error) {
    await client.query('ROLLBACK')
    throw error
  } finally {
    client.release()
  }
}

const importSeguradoraXmlFile = async (fileName) => {
  const sanitizedFileName = path.basename(normalizeRequestValue(fileName))

  if (!sanitizedFileName) {
    throw new Error('Nome do arquivo XML e obrigatorio.')
  }

  if (path.extname(sanitizedFileName).toLowerCase() !== '.xml') {
    throw new Error('Arquivo XML invalido.')
  }

  const resolvedPath = path.resolve(importXmlDirectory, sanitizedFileName)

  if (!resolvedPath.startsWith(importXmlDirectory)) {
    throw new Error('Arquivo XML invalido.')
  }

  const xmlContent = await readFile(resolvedPath, 'utf8')
  const parsedRecords = parseSeguradoraXml(xmlContent)

  if (!parsedRecords.length) {
    throw new Error('Nenhum registro de seguradora foi encontrado no XML informado.')
  }

  const normalizedRecords = parsedRecords.map((record, index) => normalizeImportedSeguradoraRecord(record, index))
  const client = await pool.connect()

  try {
    await client.query('BEGIN')
    let inserted = 0
    let updated = 0

    for (const record of normalizedRecords) {
      const existingResult = await client.query('SELECT 1 FROM seguradora WHERE codigo = $1 LIMIT 1', [record.codigo])

      if (existingResult.rowCount > 0) {
        await client.query(
          `UPDATE seguradora
           SET controle = $1,
               lista = $2,
               data_modificacao = NOW()
           WHERE codigo = $3`,
          [record.controle, record.lista, record.codigo],
        )
        updated += 1
        continue
      }

      await client.query(
        `INSERT INTO seguradora (
           codigo,
           controle,
           lista,
           data_inclusao,
           data_modificacao
         )
         VALUES ($1, $2, $3, NOW(), NOW())`,
        [record.codigo, record.controle, record.lista],
      )
      inserted += 1
    }

    await client.query('COMMIT')

    return {
      fileName: sanitizedFileName,
      filePath: resolvedPath,
      total: parsedRecords.length,
      processed: normalizedRecords.length,
      inserted,
      updated,
    }
  } catch (error) {
    await client.query('ROLLBACK')
    throw error
  } finally {
    client.release()
  }
}

const importMarcaModeloXmlFile = async (fileName) => {
  const sanitizedFileName = path.basename(normalizeRequestValue(fileName))

  if (!sanitizedFileName) {
    throw new Error('Nome do arquivo XML e obrigatorio.')
  }

  if (path.extname(sanitizedFileName).toLowerCase() !== '.xml') {
    throw new Error('Arquivo XML invalido.')
  }

  const resolvedPath = path.resolve(importXmlDirectory, sanitizedFileName)

  if (!resolvedPath.startsWith(importXmlDirectory)) {
    throw new Error('Arquivo XML invalido.')
  }

  const xmlContent = await readFile(resolvedPath, 'utf8')
  const parsedRecords = parseMarcaModeloXml(xmlContent)

  if (!parsedRecords.length) {
    throw new Error('Nenhum registro de marca/modelo foi encontrado no XML informado.')
  }

  const normalizedRecords = parsedRecords.map((record, index) => normalizeImportedMarcaModeloRecord(record, index))
  const uniqueRecords = Array.from(new Set(normalizedRecords.map((record) => record.descricao)))
    .map((descricao) => ({ descricao }))
  const client = await pool.connect()

  try {
    await client.query('BEGIN')
    let inserted = 0
    let updated = 0

    for (const [index, record] of uniqueRecords.entries()) {
      const upsertResult = await client.query(
        `INSERT INTO marca_modelo (
           codigo,
           descricao,
           data_inclusao,
           data_modificacao
         )
         VALUES ($1, $2, NOW(), NOW())
         ON CONFLICT (codigo) DO UPDATE
         SET descricao = EXCLUDED.descricao,
             data_modificacao = NOW()
         RETURNING (xmax = 0) AS inserted_record`,
        [String(index + 1), record.descricao],
      )

      if (upsertResult.rows[0]?.inserted_record) {
        inserted += 1
      } else {
        updated += 1
      }
    }

    await client.query('COMMIT')

    return {
      fileName: sanitizedFileName,
      filePath: resolvedPath,
      total: parsedRecords.length,
      processed: uniqueRecords.length,
      inserted,
      updated,
    }
  } catch (error) {
    await client.query('ROLLBACK')
    throw error
  } finally {
    client.release()
  }
}

const seedTrocaTableFromXmlIfEmpty = async () => {
  const countResult = await pool.query('SELECT COUNT(*)::int AS total FROM tipo_troca')
  const total = countResult.rows[0]?.total ?? 0

  if (total > 0) {
    return null
  }

  return importTrocaXmlFile('Listagem de Trocas.xml')
}

const seedSeguradoraTableFromXmlIfEmpty = async () => {
  const countResult = await pool.query('SELECT COUNT(*)::int AS total FROM seguradora')
  const total = countResult.rows[0]?.total ?? 0

  if (total > 0) {
    return null
  }

  return importSeguradoraXmlFile('seguradoras.xml')
}

const seedMarcaModeloTableFromXmlIfEmpty = async () => {
  const countResult = await pool.query('SELECT COUNT(*)::int AS total FROM marca_modelo')
  const total = countResult.rows[0]?.total ?? 0

  if (total > 0) {
    return null
  }

  return importMarcaModeloXmlFile('marca-modelo.xml')
}

const seedOrdemServicoTableFromXmlIfEmpty = async () => {
  const countResult = await pool.query(`SELECT COUNT(*)::int AS total FROM ${ordemServicoTableName}`)
  const total = countResult.rows[0]?.total ?? 0

  if (total > 0) {
    return null
  }

  const dependencyResult = await pool.query(`
    SELECT
      (SELECT COUNT(*)::int FROM credenciada) AS total_credenciada,
      (SELECT COUNT(*)::int FROM condutor) AS total_condutor,
      (SELECT COUNT(*)::int FROM monitor) AS total_monitor,
      (SELECT COUNT(*)::int FROM veiculo) AS total_veiculo,
      (SELECT COUNT(*)::int FROM dre) AS total_dre
  `)
  const dependencies = dependencyResult.rows[0] ?? {}

  if (!dependencies.total_credenciada || !dependencies.total_condutor || !dependencies.total_monitor || !dependencies.total_veiculo || !dependencies.total_dre) {
    return null
  }

  return importOrdemServicoXmlFile('OrdemServico.xml')
}

const isDateInputValid = (value) => {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    return false
  }

  const parsed = new Date(`${value}T00:00:00`)
  return !Number.isNaN(parsed.getTime())
}

const getCurrentDateInputValue = () => {
  const today = new Date()
  const year = today.getFullYear()
  const month = String(today.getMonth() + 1).padStart(2, '0')
  const day = String(today.getDate()).padStart(2, '0')
  return `${year}-${month}-${day}`
}

const normalizeEmissaoDocumentoDateKey = (value) => {
  const normalizedValue = normalizeRequestValue(value)

  if (!normalizedValue) {
    return ''
  }

  if (/^\d{4}-\d{2}-\d{2}$/.test(normalizedValue)) {
    const [year, month, day] = normalizedValue.split('-')
    return `${day}/${month}/${year}`
  }

  if (/^\d{2}\/\d{2}\/\d{4}$/.test(normalizedValue)) {
    return normalizedValue
  }

  return ''
}

const buildCurrentEmissaoDocumentoDateKey = () => normalizeEmissaoDocumentoDateKey(getCurrentDateInputValue())

const isEmissaoDocumentoDateKeyValid = (value) => {
  if (!/^\d{2}\/\d{2}\/\d{4}$/.test(value)) {
    return false
  }

  const [day, month, year] = value.split('/').map(Number)
  const parsed = new Date(year, month - 1, day)

  return parsed.getFullYear() === year
    && parsed.getMonth() === month - 1
    && parsed.getDate() === day
}

const normalizeEmissaoDocumentoParamText = (value, maxLength = 4000) => {
  return normalizeRequestValue(value)
    .replace(/\s+/g, ' ')
    .slice(0, maxLength)
}

const normalizeEmissaoDocumentoParamMultilineText = (value, maxLength = 4000) => {
  return String(value ?? '')
    .replace(/\r/g, '')
    .replace(/\t/g, ' ')
    .replace(/[ ]{2,}/g, ' ')
    .replace(/\n{3,}/g, '\n\n')
    .trim()
    .slice(0, maxLength)
}

const defaultEmissaoDocumentoTitulo = `SECRETARIA MUNICIPAL DE MOBILIDADE URBANA E TRANSPORTE
DIVISAO DE TRANSPORTE ESCOLAR GRATUITO
Rua Joaquim Carlos, 655 - Bairro Pari - Sao Paulo/SP`

const defaultEmissaoDocumentoDiretor = `Leandro Gabrelon
Diretor(a)
Em 31/03/2026,`

const defaultEmissaoDocumentoObjetoLicitacao = 'TERMO de Adesao ao Credenciamento, respectivos aditivos e atos correlatos, destinados a formalizacao da prestacao do Transporte Escolar Gratuito - TEG.'
const defaultEmissaoDocumentoCredenciante = 'MUNICIPIO DE SAO PAULO, por intermedio da SECRETARIA MUNICIPAL DE MOBILIDADE URBANA E TRANSPORTE - SMT, por meio do DEPARTAMENTO DE TRANSPORTES PUBLICOS - DTP.'
const defaultEmissaoDocumentoTituloAditivo = 'TERMO ADITIVO DE PRORROGACAO AO TERMO DE ADESAO AO CREDENCIAMENTO'
const defaultEmissaoDocumentoTermoSmt = 'SMT/SETRAM/DTP/DTEG N. ________'
const defaultEmissaoDocumentoDescricaoAditivo = `Aos <termo/inicio de vigencia/por extenso>, as partes qualificadas neste instrumento celebram o presente TERMO ADITIVO DE PRORROGACAO ao Termo de Adesao ao Credenciamento {{termo_adesao}}, vinculado ao Processo SEI {{processo}} e ao Edital de Chamamento Publico n {{edital}}.

De um lado, {{credenciante}}, doravante CREDENCIANTE. De outro, {{credenciado}}, inscrito(a) no CPF/CNPJ sob n {{cnpj_cpf}}, representado(a) por {{representante}}, CPF n {{cpf_representante}}, doravante CREDENCIADO(A).

As partes ajustam a prorrogacao do instrumento originario, com eficacia condicionada a publicacao oficial, na forma das clausulas a seguir:`
const defaultEmissaoDocumentoCorpoAditivo = `CLAUSULA PRIMEIRA - DO OBJETO
1.1. Constitui objeto do presente Termo Aditivo a prorrogacao da vigencia do Termo de Adesao ao Credenciamento {{termo_adesao}}, referente a {{objeto_licitacao}}.

CLAUSULA SEGUNDA - DO PRAZO
2.1. A vigencia do presente aditamento inicia-se em {{inicio_vigencia}}, observadas as condicoes do edital, do termo originario e da legislacao aplicavel.

CLAUSULA TERCEIRA - DO VALOR E DOS RECURSOS ORCAMENTARIOS
3.1. O valor previsto para o periodo prorrogado corresponde a {{valor_previsto}}.
3.2. As despesas decorrentes deste instrumento correrao por conta das dotacoes orcamentarias proprias da Secretaria Municipal de Educacao, inclusive em exercicios subsequentes, se cabivel.

CLAUSULA QUARTA - DA EFICACIA
4.1. A eficacia deste Termo Aditivo fica condicionada a sua publicacao no Diario Oficial da Cidade.

CLAUSULA QUINTA - DA RATIFICACAO
5.1. Permanecem ratificadas as demais clausulas e condicoes do termo originario que nao conflitarem com o presente instrumento.`
const defaultEmissaoDocumentoAssinaturasAditivo = `(Assinado eletronicamente)
{{credenciante}}

______________________________________________________________________
{{credenciado}}

(Assinado eletronicamente)
TESTEMUNHA

(Assinado eletronicamente)
TESTEMUNHA`
const defaultEmissaoDocumentoTextoDespacho = `I A vista dos elementos e documentos que instruem o processo SEI n {{processo}}, Edital de Chamamento Publico n {{edital}} e Anexos, nos termos da delegacao contida na Portaria n 38 - SMT.GAB, firmo, com fundamento no artigo 25, caput, da Lei Federal n 8.666/93, o presente TERMO DE ADITAMENTO a contar de {{inicio_vigencia}}, com o valor previsto de {{valor_previsto}}, com {{credenciado}}, CPF/CNPJ n {{cnpj_cpf}} para {{objeto_licitacao}}.

II As despesas decorrentes deste Termo de Aditamento onerarao as dotacoes orcamentarias especificas da Secretaria Municipal de Educacao - SME para o exercicio de {{ano_publicacao}} abaixo referidas: 16.10.12.367.3010.2.848.33903900.00; 16.10.12.365.3025.2.849.33903900.00; 16.10.12.361.3010.2.850.33903900.00; 16.10.12.367.3010.2.848.33903600.00; 16.10.12.365.3025.2.849.33903600.00; 16.10.12.361.3010.2.850.33903600.00; 16.10.12.367.3010.2.848.33904700.00; 16.10.12.365.3025.2.849.33904700.00; 16.10.12.361.3010.2.850.33904700.00 e nos exercicios subsequentes onerarao as dotacoes orcamentarias proprias e especificas do Programa, da Secretaria Municipal da Educacao.

III Autorizo a emissao da respectiva Nota de Empenho.

IV O DISPOSITIVO LEGAL baseia-se no Artigo 25, caput, da Lei Federal 8.666/93.`

const emissaoDocumentoParametroSelectClause = `
  BTRIM(data_referencia) AS data_referencia,
  COALESCE(BTRIM(objeto), '') AS objeto,
  COALESCE(BTRIM(objeto_licitacao), '') AS objeto_licitacao,
  COALESCE(BTRIM(credenciante), '') AS credenciante,
  COALESCE(BTRIM(titulo_aditivo), '') AS titulo_aditivo,
  COALESCE(BTRIM(termo_smt), '') AS termo_smt,
  COALESCE(BTRIM(descricao_aditivo), '') AS descricao_aditivo,
  COALESCE(BTRIM(corpo_aditivo), '') AS corpo_aditivo,
  COALESCE(BTRIM(assinaturas_aditivo), '') AS assinaturas_aditivo,
  COALESCE(BTRIM(descricao_contrato_pf), '') AS descricao_contrato_pf,
  COALESCE(BTRIM(descricao_contrato_pj), '') AS descricao_contrato_pj,
  COALESCE(BTRIM(corpo_contrato_pf), '') AS corpo_contrato_pf,
  COALESCE(BTRIM(corpo_contrato_pj), '') AS corpo_contrato_pj,
  COALESCE(BTRIM(link_modelo_relatorio_contrato_pf), '') AS link_modelo_relatorio_contrato_pf,
  COALESCE(BTRIM(link_modelo_relatorio_contrato_pj), '') AS link_modelo_relatorio_contrato_pj,
  COALESCE(BTRIM(texto_despacho), '') AS texto_despacho,
  COALESCE(BTRIM(edital_chamamento_publico), '') AS edital_chamamento_publico,
  COALESCE(BTRIM(obs_01_emissao), '') AS obs_01_emissao,
  COALESCE(BTRIM(obs_02_emissao), '') AS obs_02_emissao,
  COALESCE(BTRIM(rodape_emissao), '') AS rodape_emissao,
  COALESCE(prefeitura_imagem, '') AS prefeitura_imagem,
  COALESCE(titulo_emissao, '') AS titulo_emissao,
  COALESCE(diretor_emissao, '') AS diretor_emissao`

const validateEmissaoDocumentoParametroPayload = async ({
  dataReferencia,
  objeto,
  objetoLicitacao,
  credenciante,
  tituloAditivo,
  termoSmt,
  descricaoAditivo,
  corpoAditivo,
  assinaturasAditivo,
  descricaoContratoPf,
  descricaoContratoPj,
  corpoContratoPf,
  corpoContratoPj,
  linkModeloRelatorioContratoPf,
  linkModeloRelatorioContratoPj,
  textoDespacho,
  editalChamamentoPublico,
  obs01Emissao,
  obs02Emissao,
  rodapeEmissao,
  prefeituraImagem,
  tituloEmissao,
  diretorEmissao,
  originalDataReferencia = null,
}) => {
  const normalizedDataReferencia = normalizeEmissaoDocumentoDateKey(dataReferencia)
  const normalizedOriginalDataReferencia = normalizeEmissaoDocumentoDateKey(originalDataReferencia)
  const normalizedObjeto = normalizeEmissaoDocumentoParamText(objeto)
  const normalizedObjetoLicitacao = normalizeEmissaoDocumentoParamText(objetoLicitacao)
  const normalizedCredenciante = normalizeEmissaoDocumentoParamMultilineText(credenciante)
  const normalizedTituloAditivo = normalizeEmissaoDocumentoParamMultilineText(tituloAditivo)
  const normalizedTermoSmt = normalizeEmissaoDocumentoParamMultilineText(termoSmt)
  const normalizedDescricaoAditivo = normalizeEmissaoDocumentoParamMultilineText(descricaoAditivo, 12000)
  const normalizedCorpoAditivo = normalizeEmissaoDocumentoParamMultilineText(corpoAditivo, 12000)
  const normalizedAssinaturasAditivo = normalizeEmissaoDocumentoParamMultilineText(assinaturasAditivo, 12000)
  const normalizedDescricaoContratoPf = normalizeEmissaoDocumentoParamMultilineText(descricaoContratoPf, 20000)
  const normalizedDescricaoContratoPj = normalizeEmissaoDocumentoParamMultilineText(descricaoContratoPj, 20000)
  const normalizedCorpoContratoPf = normalizeEmissaoDocumentoParamMultilineText(corpoContratoPf, 250000)
  const normalizedCorpoContratoPj = normalizeEmissaoDocumentoParamMultilineText(corpoContratoPj, 250000)
  const normalizedLinkModeloRelatorioContratoPf = normalizeEmissaoDocumentoParamText(linkModeloRelatorioContratoPf)
  const normalizedLinkModeloRelatorioContratoPj = normalizeEmissaoDocumentoParamText(linkModeloRelatorioContratoPj)
  const normalizedTextoDespacho = normalizeEmissaoDocumentoParamMultilineText(textoDespacho, 8000)
  const normalizedEditalChamamentoPublico = normalizeEmissaoDocumentoParamText(editalChamamentoPublico, 50)
  const normalizedObs01Emissao = normalizeEmissaoDocumentoParamText(obs01Emissao)
  const normalizedObs02Emissao = normalizeEmissaoDocumentoParamText(obs02Emissao)
  const normalizedRodapeEmissao = normalizeEmissaoDocumentoParamText(rodapeEmissao)
  const normalizedPrefeituraImagem = String(prefeituraImagem ?? '').trim().slice(0, 2_000_000)
  const normalizedTituloEmissao = normalizeEmissaoDocumentoParamMultilineText(tituloEmissao || defaultEmissaoDocumentoTitulo)
  const normalizedDiretorEmissao = normalizeEmissaoDocumentoParamMultilineText(diretorEmissao || defaultEmissaoDocumentoDiretor)

  if (!normalizedDataReferencia || !isEmissaoDocumentoDateKeyValid(normalizedDataReferencia)) {
    return { status: 400, payload: { message: 'Data de referencia invalida. Use dd/mm/yyyy.' } }
  }

  if (!normalizedObjeto) {
    return { status: 400, payload: { message: 'Objeto e obrigatorio.' } }
  }

  if (!normalizedEditalChamamentoPublico) {
    return { status: 400, payload: { message: 'Edital de Chamamento Publico e obrigatorio.' } }
  }

  if (!normalizedObjetoLicitacao) {
    return { status: 400, payload: { message: 'Objeto da licitacao e obrigatorio.' } }
  }

  if (!normalizedCredenciante) {
    return { status: 400, payload: { message: 'Credenciante e obrigatorio.' } }
  }

  if (!normalizedTituloAditivo) {
    return { status: 400, payload: { message: 'Titulo do aditivo e obrigatorio.' } }
  }

  if (!normalizedTermoSmt) {
    return { status: 400, payload: { message: 'Termo SMT e obrigatorio.' } }
  }

  if (!normalizedDescricaoAditivo) {
    return { status: 400, payload: { message: 'Descricao do aditivo e obrigatoria.' } }
  }

  if (!normalizedCorpoAditivo) {
    return { status: 400, payload: { message: 'Corpo do aditivo e obrigatorio.' } }
  }

  if (!normalizedAssinaturasAditivo) {
    return { status: 400, payload: { message: 'Assinaturas do aditivo sao obrigatorias.' } }
  }

  if (!normalizedTextoDespacho) {
    return { status: 400, payload: { message: 'Texto do despacho e obrigatorio.' } }
  }

  if (!normalizedObs01Emissao) {
    return { status: 400, payload: { message: 'Obs 01 da emissao e obrigatoria.' } }
  }

  if (!normalizedObs02Emissao) {
    return { status: 400, payload: { message: 'Obs 02 da emissao e obrigatoria.' } }
  }

  if (!normalizedRodapeEmissao) {
    return { status: 400, payload: { message: 'Rodape da emissao e obrigatorio.' } }
  }

  if (!normalizedTituloEmissao) {
    return { status: 400, payload: { message: 'Titulo da emissao e obrigatorio.' } }
  }

  if (!normalizedDiretorEmissao) {
    return { status: 400, payload: { message: 'Nome do diretor da emissao e obrigatorio.' } }
  }

  const duplicateResult = await pool.query(
    `SELECT 1
     FROM ${emissaoDocumentoParametroTableName}
     WHERE BTRIM(data_referencia) = $1
       AND ($2::text IS NULL OR BTRIM(data_referencia) <> $2)
     LIMIT 1`,
    [normalizedDataReferencia, normalizedOriginalDataReferencia || null],
  )

  if (duplicateResult.rowCount > 0) {
    return { status: 409, payload: { message: 'Ja existe parametro de emissao cadastrado para esta data.' } }
  }

  return {
    status: 200,
    payload: {
      dataReferencia: normalizedDataReferencia,
      objeto: normalizedObjeto,
      objetoLicitacao: normalizedObjetoLicitacao,
      credenciante: normalizedCredenciante,
      tituloAditivo: normalizedTituloAditivo,
      termoSmt: normalizedTermoSmt,
      descricaoAditivo: normalizedDescricaoAditivo,
      corpoAditivo: normalizedCorpoAditivo,
      assinaturasAditivo: normalizedAssinaturasAditivo,
      descricaoContratoPf: normalizedDescricaoContratoPf,
      descricaoContratoPj: normalizedDescricaoContratoPj,
      corpoContratoPf: normalizedCorpoContratoPf,
      corpoContratoPj: normalizedCorpoContratoPj,
      linkModeloRelatorioContratoPf: normalizedLinkModeloRelatorioContratoPf,
      linkModeloRelatorioContratoPj: normalizedLinkModeloRelatorioContratoPj,
      textoDespacho: normalizedTextoDespacho,
      editalChamamentoPublico: normalizedEditalChamamentoPublico,
      obs01Emissao: normalizedObs01Emissao,
      obs02Emissao: normalizedObs02Emissao,
      rodapeEmissao: normalizedRodapeEmissao,
      prefeituraImagem: normalizedPrefeituraImagem,
      tituloEmissao: normalizedTituloEmissao,
      diretorEmissao: normalizedDiretorEmissao,
    },
  }
}

const isDateBeforeToday = (value) => isDateInputValid(value) && value < getCurrentDateInputValue()
const isDateAfterToday = (value) => isDateInputValid(value) && value > getCurrentDateInputValue()

const validateCondutorPayload = async ({
  codigo,
  condutor,
  cpfCondutor,
  crmc,
  validadeCrmc,
  validadeCurso,
  tipoVinculo,
  historico,
  originalCodigo = null,
}) => {
  const normalizedCodigo = normalizeCondutorCodigo(codigo)
  const normalizedCondutor = normalizeCondutorName(condutor)
  const normalizedCpf = normalizeCpf(cpfCondutor)
  const normalizedCrmc = normalizeCrmc(crmc)
  const normalizedValidadeCrmc = normalizeRequestValue(validadeCrmc)
  const normalizedValidadeCurso = normalizeRequestValue(validadeCurso)
  const normalizedTipoVinculo = normalizeTipoVinculo(tipoVinculo)
  const normalizedHistorico = normalizeHistorico(historico)

  if (normalizedCodigo === null) {
    return { status: 400, payload: { message: 'Codigo e obrigatorio.' } }
  }

  if (Number.isNaN(normalizedCodigo)) {
    return { status: 400, payload: { message: 'Codigo deve ser um numero inteiro positivo.' } }
  }

  if (!normalizedCondutor) {
    return { status: 400, payload: { message: 'Nome do condutor e obrigatorio.' } }
  }

  if (!isCondutorNameValid(normalizedCondutor)) {
    return { status: 400, payload: { message: 'Condutor deve conter apenas letras maiusculas e no maximo 100 caracteres.' } }
  }

  if (!normalizedCpf) {
    return { status: 400, payload: { message: 'CPF do condutor e obrigatorio.' } }
  }

  if (!isCpfValid(normalizedCpf)) {
    return { status: 400, payload: { message: 'CPF do condutor deve conter 11 digitos.' } }
  }

  if (!normalizedCrmc) {
    return { status: 400, payload: { message: 'CRMC e obrigatorio.' } }
  }

  if (!isCrmcValid(normalizedCrmc)) {
    return { status: 400, payload: { message: 'CRMC deve ter no maximo 10 caracteres alfanumericos.' } }
  }

  if (!normalizedValidadeCrmc) {
    return { status: 400, payload: { message: 'Validade do CRMC e obrigatoria.' } }
  }

  if (!isDateInputValid(normalizedValidadeCrmc)) {
    return { status: 400, payload: { message: 'Validade do CRMC invalida.' } }
  }

  if (!isDateAfterToday(normalizedValidadeCrmc)) {
    return { status: 400, payload: { message: 'Validade do CRMC deve ser futura.' } }
  }

  if (!normalizedValidadeCurso) {
    return { status: 400, payload: { message: 'Validade do curso e obrigatoria.' } }
  }

  if (!isDateInputValid(normalizedValidadeCurso)) {
    return { status: 400, payload: { message: 'Validade do curso invalida.' } }
  }

  if (!isDateAfterToday(normalizedValidadeCurso)) {
    return { status: 400, payload: { message: 'Validade do curso deve ser futura.' } }
  }

  if (normalizedTipoVinculo === null) {
    return { status: 400, payload: { message: 'Tipo de vinculo invalido.' } }
  }

  const duplicateCodeResult = await pool.query(
    `SELECT 1
     FROM condutor
     WHERE codigo = $1
       AND ($2::int IS NULL OR codigo <> $2)
     LIMIT 1`,
    [normalizedCodigo, originalCodigo],
  )

  if (duplicateCodeResult.rowCount > 0) {
    return { status: 409, payload: { message: 'Codigo ja cadastrado.' } }
  }

  return {
    status: 200,
    payload: {
      codigo: normalizedCodigo,
      condutor: normalizedCondutor,
      cpfCondutor: normalizedCpf,
      crmc: normalizedCrmc,
      validadeCrmc: normalizedValidadeCrmc,
      validadeCurso: normalizedValidadeCurso,
      tipoVinculo: normalizedTipoVinculo,
      historico: normalizedHistorico,
    },
  }
}

const validateMonitorPayload = async ({
  codigo,
  monitor,
  rgMonitor,
  cpfMonitor,
  cursoMonitor,
  validadeCurso,
  tipoVinculo,
  nascimento,
  originalCodigo = null,
}) => {
  const normalizedCodigo = normalizeCondutorCodigo(codigo)
  const normalizedMonitor = normalizeCondutorName(monitor)
  const normalizedRgMonitor = normalizeMonitorRg(rgMonitor)
  const normalizedCpfMonitor = normalizeCpf(cpfMonitor)
  const normalizedCursoMonitor = normalizeRequestValue(cursoMonitor)
  const normalizedValidadeCurso = normalizeRequestValue(validadeCurso)
  const normalizedTipoVinculo = normalizeTipoVinculo(tipoVinculo)
  const normalizedNascimento = normalizeRequestValue(nascimento)

  if (normalizedCodigo === null) {
    return { status: 400, payload: { message: 'Codigo e obrigatorio.' } }
  }

  if (Number.isNaN(normalizedCodigo)) {
    return { status: 400, payload: { message: 'Codigo deve ser um numero inteiro positivo.' } }
  }

  if (!normalizedMonitor) {
    return { status: 400, payload: { message: 'Nome do monitor e obrigatorio.' } }
  }

  if (!isMonitorNameValid(normalizedMonitor)) {
    return { status: 400, payload: { message: 'Monitor deve conter apenas letras maiusculas e no maximo 255 caracteres.' } }
  }

  if (!normalizedCpfMonitor) {
    return { status: 400, payload: { message: 'CPF do monitor e obrigatorio.' } }
  }

  if (!isCpfValid(normalizedCpfMonitor)) {
    return { status: 400, payload: { message: 'CPF do monitor deve conter 11 digitos.' } }
  }

  if (normalizedRgMonitor && !isMonitorRgValid(normalizedRgMonitor)) {
    return { status: 400, payload: { message: 'RG do monitor invalido.' } }
  }

  if (!normalizedNascimento) {
    return { status: 400, payload: { message: 'Data de nascimento e obrigatoria.' } }
  }

  if (!normalizedCursoMonitor) {
    return { status: 400, payload: { message: 'Data do curso e obrigatoria.' } }
  }

  if (!normalizedValidadeCurso) {
    return { status: 400, payload: { message: 'Validade do curso e obrigatoria.' } }
  }

  if (normalizedCursoMonitor && !isDateInputValid(normalizedCursoMonitor)) {
    return { status: 400, payload: { message: 'Data do curso invalida.' } }
  }

  if (normalizedCursoMonitor && !isDateBeforeToday(normalizedCursoMonitor)) {
    return { status: 400, payload: { message: 'Data do curso deve ser anterior ao dia da inclusao.' } }
  }

  if (normalizedValidadeCurso && !isDateInputValid(normalizedValidadeCurso)) {
    return { status: 400, payload: { message: 'Validade do curso invalida.' } }
  }

  if (normalizedValidadeCurso && !isDateAfterToday(normalizedValidadeCurso)) {
    return { status: 400, payload: { message: 'Validade do curso deve ser futura.' } }
  }

  if (normalizedCursoMonitor && normalizedValidadeCurso && normalizedValidadeCurso < normalizedCursoMonitor) {
    return { status: 400, payload: { message: 'Validade do curso deve ser maior ou igual a data do curso.' } }
  }

  if (normalizedNascimento && !isDateInputValid(normalizedNascimento)) {
    return { status: 400, payload: { message: 'Data de nascimento invalida.' } }
  }

  if (normalizedNascimento && !isDateBeforeToday(normalizedNascimento)) {
    return { status: 400, payload: { message: 'Data de nascimento deve ser anterior ao dia da inclusao.' } }
  }

  if (normalizedTipoVinculo === null) {
    return { status: 400, payload: { message: 'Tipo de vinculo invalido.' } }
  }

  const duplicateCodeResult = await pool.query(
    `SELECT 1
     FROM monitor
     WHERE codigo = $1
       AND ($2::int IS NULL OR codigo <> $2)
     LIMIT 1`,
    [normalizedCodigo, originalCodigo],
  )

  if (duplicateCodeResult.rowCount > 0) {
    return { status: 409, payload: { message: 'Codigo ja cadastrado.' } }
  }

  return {
    status: 200,
    payload: {
      codigo: normalizedCodigo,
      monitor: normalizedMonitor,
      rgMonitor: normalizedRgMonitor,
      cpfMonitor: normalizedCpfMonitor,
      cursoMonitor: normalizedCursoMonitor,
      validadeCurso: normalizedValidadeCurso,
      tipoVinculo: normalizedTipoVinculo,
      nascimento: normalizedNascimento,
    },
  }
}

const validateCepPayload = async ({ cep, logradouro, complemento, bairro, municipio, uf, ibge, originalCep = null }) => {
  const normalizedCep = normalizeCep(cep)
  const normalizedLogradouro = normalizeCredenciadaText(logradouro, 255)
  const normalizedComplemento = normalizeCredenciadaText(complemento, 255)
  const normalizedBairro = normalizeCredenciadaText(bairro, 120)
  const normalizedMunicipio = normalizeCredenciadaText(municipio, 120)
  const normalizedUf = normalizeRequestValue(uf).toUpperCase().replace(/[^A-Z]/g, '').slice(0, 2)
  const normalizedIbge = normalizeRequestValue(ibge).replace(/\D/g, '').slice(0, 10)

  if (!normalizedCep || !isCepValid(normalizedCep)) {
    return { status: 400, payload: { message: 'CEP invalido. Formato esperado: 00000-000.' } }
  }

  if (!normalizedMunicipio) {
    return { status: 400, payload: { message: 'Municipio e obrigatorio.' } }
  }

  if (!normalizedUf || normalizedUf.length !== 2) {
    return { status: 400, payload: { message: 'UF invalida. Informe 2 letras (ex: SP).' } }
  }

  const duplicateCepResult = await pool.query(
    `SELECT 1 FROM ${cepTableName} WHERE BTRIM(cep) = $1 AND ($2::text IS NULL OR BTRIM(cep) <> $2) LIMIT 1`,
    [normalizedCep, originalCep],
  )

  if (duplicateCepResult.rowCount > 0) {
    return { status: 409, payload: { message: 'CEP ja cadastrado.' } }
  }

  return {
    status: 200,
    payload: {
      cep: normalizedCep,
      logradouro: normalizedLogradouro,
      complemento: normalizedComplemento,
      bairro: normalizedBairro,
      municipio: normalizedMunicipio,
      uf: normalizedUf,
      ibge: normalizedIbge,
    },
  }
}

const validateVeiculoPayload = async ({
  codigo,
  crm,
  placas,
  ano,
  capDetran,
  capTeg,
  capTegCreche,
  capAcessivel,
  valCrm,
  seguradora,
  seguroInicio,
  seguroTermino,
  tipoDeBancada,
  tipoDeVeiculo,
  marcaModelo,
  titular,
  cnpjCpf,
  valorVeiculo,
  osEspecial,
  originalCodigo = null,
}) => {
  const normalizedCodigo = normalizeCondutorCodigo(codigo)
  const normalizedCrm = normalizeVehicleCrm(crm)
  const normalizedPlacas = normalizeVehiclePlaca(placas)
  const normalizedAno = normalizeVehicleInteger(ano, 4)
  const normalizedCapDetran = normalizeVehicleInteger(capDetran, 3)
  const normalizedCapTeg = normalizeVehicleInteger(capTeg, 3)
  const normalizedCapTegCreche = normalizeVehicleInteger(capTegCreche, 3)
  const normalizedCapAcessivel = normalizeVehicleInteger(capAcessivel, 3)
  const normalizedValCrm = normalizeRequestValue(valCrm)
  const normalizedSeguradora = normalizeCredenciadaText(seguradora, 255)
  const normalizedSeguroInicio = normalizeRequestValue(seguroInicio)
  const normalizedSeguroTermino = normalizeRequestValue(seguroTermino)
  const normalizedTipoDeBancada = normalizeTipoDeBancada(tipoDeBancada)
  const normalizedTipoDeVeiculo = normalizeTipoDeVeiculo(tipoDeVeiculo)
  const normalizedMarcaModelo = normalizeCredenciadaText(marcaModelo, 255)
  let normalizedTitular = normalizeCredenciadaText(titular, 255)
  const normalizedCnpjCpf = normalizeCnpjCpf(cnpjCpf)
  const normalizedValorVeiculo = normalizeVehicleMoney(valorVeiculo)
  const normalizedOsEspecial = normalizeOsEspecial(osEspecial)

  if (normalizedCodigo !== null && Number.isNaN(normalizedCodigo)) {
    return { status: 400, payload: { message: 'Codigo deve ser um numero inteiro positivo.' } }
  }

  if (normalizedCrm && !isVehicleCrmValid(normalizedCrm)) {
    return { status: 400, payload: { message: 'CRM invalido.' } }
  }

  if (normalizedPlacas && !isVehiclePlacaValid(normalizedPlacas)) {
    return { status: 400, payload: { message: 'Placa deve seguir o formato ABC-1234 ou ABC-1D23.' } }
  }

  if (normalizedAno !== null && Number.isNaN(normalizedAno)) {
    return { status: 400, payload: { message: 'Ano invalido.' } }
  }

  if (normalizedCapDetran !== null && Number.isNaN(normalizedCapDetran)) {
    return { status: 400, payload: { message: 'Capacidade DETRAN invalida.' } }
  }

  if (normalizedCapTeg !== null && Number.isNaN(normalizedCapTeg)) {
    return { status: 400, payload: { message: 'Capacidade TEG invalida.' } }
  }

  if (normalizedCapTegCreche !== null && Number.isNaN(normalizedCapTegCreche)) {
    return { status: 400, payload: { message: 'Capacidade TEG creche invalida.' } }
  }

  if (normalizedCapAcessivel !== null && Number.isNaN(normalizedCapAcessivel)) {
    return { status: 400, payload: { message: 'Capacidade acessivel invalida.' } }
  }

  if (normalizedValCrm && !isDateInputValid(normalizedValCrm)) {
    return { status: 400, payload: { message: 'Validade do CRM invalida.' } }
  }

  if (normalizedValCrm && normalizedValCrm < getCurrentDateInputValue()) {
    return { status: 400, payload: { message: 'Validade do CRM nao pode ser passada.' } }
  }

  if (normalizedSeguroInicio && !isDateInputValid(normalizedSeguroInicio)) {
    return { status: 400, payload: { message: 'Data inicial do seguro invalida.' } }
  }

  if (normalizedSeguroTermino && !isDateInputValid(normalizedSeguroTermino)) {
    return { status: 400, payload: { message: 'Data final do seguro invalida.' } }
  }

  if (normalizedSeguroTermino && normalizedSeguroTermino < getCurrentDateInputValue()) {
    return { status: 400, payload: { message: 'Data final do seguro nao pode ser passada.' } }
  }

  if (normalizedSeguroInicio && normalizedSeguroTermino && normalizedSeguroTermino < normalizedSeguroInicio) {
    return { status: 400, payload: { message: 'Data final do seguro deve ser maior ou igual a data inicial.' } }
  }

  if (normalizedTipoDeBancada === null) {
    return { status: 400, payload: { message: 'Tipo de bancada invalido.' } }
  }

  if (normalizedTipoDeVeiculo === null) {
    return { status: 400, payload: { message: 'Tipo de veiculo invalido.' } }
  }

  if (normalizedCnpjCpf && !isCnpjCpfValid(normalizedCnpjCpf)) {
    return { status: 400, payload: { message: 'CNPJ/CPF deve conter 11 ou 14 digitos.' } }
  }

  if (normalizedCnpjCpf) {
    const titularLinkedItem = await findTitularByCnpjCpf(normalizedCnpjCpf)

    if (!titularLinkedItem) {
      return { status: 400, payload: { message: 'CNPJ/CPF nao encontrado na tabela titularCrm.' } }
    }

    normalizedTitular = normalizeCredenciadaText(titularLinkedItem.titular, 255)
  }

  if (normalizedValorVeiculo !== null && Number.isNaN(normalizedValorVeiculo)) {
    return { status: 400, payload: { message: 'Valor do veiculo invalido.' } }
  }

  if (normalizedOsEspecial === null) {
    return { status: 400, payload: { message: 'OS especial invalido.' } }
  }

  if (normalizedCodigo !== null) {
    const duplicateCodeResult = await pool.query(
      `SELECT 1
       FROM veiculo
       WHERE codigo = $1
         AND ($2::int IS NULL OR codigo <> $2)
       LIMIT 1`,
      [normalizedCodigo, originalCodigo],
    )

    if (duplicateCodeResult.rowCount > 0) {
      return { status: 409, payload: { message: 'Codigo ja cadastrado.' } }
    }
  }

  return {
    status: 200,
    payload: {
      codigo: normalizedCodigo,
      crm: normalizedCrm,
      placas: normalizedPlacas,
      ano: normalizedAno,
      capDetran: normalizedCapDetran,
      capTeg: normalizedCapTeg,
      capTegCreche: normalizedCapTegCreche,
      capAcessivel: normalizedCapAcessivel,
      valCrm: normalizedValCrm,
      seguradora: normalizedSeguradora,
      seguroInicio: normalizedSeguroInicio,
      seguroTermino: normalizedSeguroTermino,
      tipoDeBancada: normalizedTipoDeBancada,
      tipoDeVeiculo: normalizedTipoDeVeiculo,
      marcaModelo: normalizedMarcaModelo,
      titular: normalizedTitular,
      cnpjCpf: normalizedCnpjCpf,
      valorVeiculo: normalizedValorVeiculo,
      osEspecial: normalizedOsEspecial,
    },
  }
}

const validateTitularPayload = async ({
  codigo,
  cnpjCpf,
  titular,
  originalCodigo = null,
}) => {
  const normalizedCodigo = normalizeCondutorCodigo(codigo)
  const normalizedCnpjCpf = normalizeTitularDocument(cnpjCpf)
  const normalizedTitular = normalizeCredenciadaText(titular, 255)

  if (normalizedCodigo !== null && Number.isNaN(normalizedCodigo)) {
    return { status: 400, payload: { message: 'Codigo deve ser um numero inteiro positivo.' } }
  }

  if (!normalizedCnpjCpf) {
    return { status: 400, payload: { message: 'CNPJ/CPF e obrigatorio.' } }
  }

  if (!normalizedTitular) {
    return { status: 400, payload: { message: 'Titular do CRM e obrigatorio.' } }
  }

  if (normalizedCodigo !== null) {
    const duplicateCodeResult = await pool.query(
      `SELECT 1
       FROM ${titularTableName}
       WHERE codigo = $1
         AND ($2::int IS NULL OR codigo <> $2)
       LIMIT 1`,
      [normalizedCodigo, originalCodigo],
    )

    if (duplicateCodeResult.rowCount > 0) {
      return { status: 409, payload: { message: 'Codigo ja cadastrado.' } }
    }
  }

  return {
    status: 200,
    payload: {
      codigo: normalizedCodigo,
      cnpjCpf: normalizedCnpjCpf,
      titular: normalizedTitular,
    },
  }
}

const validateCredenciadaPayload = async ({
  codigo,
  credenciado,
  tipoPessoa,
  cnpjCpf,
  cep,
  numero,
  complemento,
  email,
  telefone1,
  telefone2,
  representante,
  cpfRepresentante,
  status,
  originalCodigo = null,
}) => {
  const normalizedCodigo = normalizeCondutorCodigo(codigo)
  const normalizedCredenciado = normalizeCredenciadaText(credenciado, 255)
  const normalizedCnpjCpf = normalizeCnpjCpf(cnpjCpf)
  const normalizedTipoPessoaSource = normalizeRequestValue(tipoPessoa).slice(0, 20)
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
  const normalizedCep = normalizeCep(cep)
  const normalizedNumero = normalizeCredenciadaText(numero, 30)
  const normalizedComplemento = normalizeCredenciadaText(complemento, 30)
  const normalizedEmail = normalizeEmailList(email)
  const normalizedTelefone1 = normalizePhoneNumber(telefone1)
  const normalizedTelefone2 = normalizePhoneNumber(telefone2)
  const normalizedRepresentante = normalizeCredenciadaText(representante, 120)
  const normalizedCpfRepresentante = normalizeCpf(cpfRepresentante)
  const normalizedStatus = normalizeCredenciadaStatusValue(status)

  if (normalizedCodigo === null) {
    return { status: 400, payload: { message: 'Codigo e obrigatorio.' } }
  }

  if (Number.isNaN(normalizedCodigo)) {
    return { status: 400, payload: { message: 'Codigo deve ser um numero inteiro positivo.' } }
  }

  if (!normalizedCredenciado) {
    return { status: 400, payload: { message: 'Credenciada e obrigatoria.' } }
  }

  if (!normalizedCnpjCpf) {
    return { status: 400, payload: { message: 'CNPJ/CPF e obrigatorio.' } }
  }

  if (!isCnpjCpfValid(normalizedCnpjCpf)) {
    return { status: 400, payload: { message: 'CNPJ/CPF deve conter 11 ou 14 digitos.' } }
  }

  const documentDigitsLength = extractDocumentDigits(normalizedCnpjCpf).length
  const normalizedTipoPessoa = documentDigitsLength === 11
    ? 'PESSOA FISICA'
    : normalizedTipoPessoaSource === 'COOPERATIVA'
      ? 'COOPERATIVA'
      : normalizedTipoPessoaSource === 'PESSOA JURIDICA'
        ? 'PESSOA JURIDICA'
        : ''

  if (documentDigitsLength === 14 && !normalizedTipoPessoa) {
    return { status: 400, payload: { message: 'Para CNPJ, tipo de termo deve ser Pessoa Juridica ou Cooperativa.' } }
  }

  if (normalizedCep && !isCepValid(normalizedCep)) {
    return { status: 400, payload: { message: 'CEP invalido.' } }
  }

  if (normalizedEmail && !isEmailListValid(normalizedEmail)) {
    return { status: 400, payload: { message: 'Email invalido.' } }
  }

  if (normalizedTelefone1 && !isPhoneNumberValid(normalizedTelefone1)) {
    return { status: 400, payload: { message: 'Telefone 1 invalido.' } }
  }

  if (normalizedTelefone2 && !isPhoneNumberValid(normalizedTelefone2)) {
    return { status: 400, payload: { message: 'Telefone 2 invalido.' } }
  }

  if (normalizedCpfRepresentante && !isCpfValid(normalizedCpfRepresentante)) {
    return { status: 400, payload: { message: 'CPF do representante invalido.' } }
  }

  const duplicateCodeResult = await pool.query(
    `SELECT 1
     FROM credenciada
     WHERE codigo = $1
       AND ($2::int IS NULL OR codigo <> $2)
     LIMIT 1`,
    [normalizedCodigo, originalCodigo],
  )

  if (duplicateCodeResult.rowCount > 0) {
    return { status: 409, payload: { message: 'Codigo ja cadastrado.' } }
  }

  const duplicateDocumentResult = await pool.query(
    `SELECT codigo
     FROM credenciada
     WHERE cnpj_cpf = $1
       AND ($2::int IS NULL OR codigo <> $2)
     LIMIT 1`,
    [normalizedCnpjCpf, originalCodigo],
  )

  if (duplicateDocumentResult.rowCount > 0) {
    return { status: 409, payload: { message: 'CNPJ/CPF ja cadastrado para outra credenciada.' } }
  }

  return {
    status: 200,
    payload: {
      codigo: normalizedCodigo,
      credenciado: normalizedCredenciado,
      tipoPessoa: normalizedTipoPessoa,
      cnpjCpf: normalizedCnpjCpf,
      cep: normalizedCep,
      numero: normalizedNumero,
      complemento: normalizedComplemento,
      email: normalizedEmail,
      telefone1: normalizedTelefone1,
      telefone2: normalizedTelefone2,
      representante: normalizedRepresentante,
      cpfRepresentante: normalizedCpfRepresentante,
      status: normalizedStatus,
      ...buildCredenciadaLegacyFields({
        codigo: normalizedCodigo,
        credenciado: normalizedCredenciado,
        representante: normalizedRepresentante,
        cnpjCpf: normalizedCnpjCpf,
      }),
    },
  }
}

const validateCredenciamentoTermoPayload = async ({
  codigoXml,
  credenciadaCodigo,
  cep,
  termoAdesao,
  sei,
  aditivo,
  situacaoPublicacao,
  situacaoEmissao,
  inicioVigencia,
  terminoVigencia,
  compDataAditivo,
  statusAditivo,
  dataPubAditivo,
  checkAditivo,
  statusTermo,
  tipoTermo,
  logradouro,
  bairro,
  municipio,
  especificacaoSei,
  valorContrato,
  dataPublicacao,
  valorContratoAtualizado,
  vencimentoGeral,
  mesRenovacao,
  tpOptante,
}, executor = pool, options = {}) => {
  const errors = {}
  const requireAditivo = options.requireAditivo !== false
  const normalizedCodigoXml = normalizeCondutorCodigo(codigoXml)
  const normalizedCredenciadaCodigo = normalizeCondutorCodigo(credenciadaCodigo)
  const normalizedCep = normalizeCep(cep)
  const normalizedTermoAdesao = normalizeRequestValue(termoAdesao).toUpperCase().slice(0, 255)
  const normalizedSei = normalizeRequestValue(sei).toUpperCase().slice(0, 255)
  const normalizedAditivo = normalizeIntegerValue(aditivo)
  const normalizedSituacaoPublicacao = normalizeCredenciadaText(situacaoPublicacao, 100)
  const normalizedSituacaoEmissao = normalizeCredenciadaText(situacaoEmissao, 100)
  const normalizedInicioVigencia = normalizeXmlDateInput(inicioVigencia) || normalizeRequestValue(inicioVigencia)
  const normalizedTerminoVigencia = normalizeXmlDateInput(terminoVigencia) || normalizeRequestValue(terminoVigencia)
  const normalizedCompDataAditivo = normalizeXmlDateInput(compDataAditivo) || normalizeRequestValue(compDataAditivo)
  const normalizedStatusAditivo = normalizeCredenciadaText(statusAditivo, 100)
  const normalizedDataPubAditivo = normalizeXmlDateInput(dataPubAditivo) || normalizeRequestValue(dataPubAditivo)
  const normalizedCheckAditivo = normalizeIntegerValue(checkAditivo)
  const normalizedStatusTermo = normalizeCredenciadaText(statusTermo, 100)
  const normalizedTipoTermo = normalizeCredenciadaText(tipoTermo, 100)
  const normalizedLogradouro = normalizeCredenciadaText(logradouro, 255)
  const normalizedBairro = normalizeCredenciadaText(bairro, 120)
  const normalizedMunicipio = normalizeCredenciadaText(municipio, 120)
  const normalizedEspecificacaoSei = normalizeRequestValue(especificacaoSei).toUpperCase().slice(0, 255)
  const normalizedValorContrato = normalizeDecimalValue(valorContrato)
  const normalizedDataPublicacao = normalizeXmlDateInput(dataPublicacao) || normalizeRequestValue(dataPublicacao)
  const normalizedValorContratoAtualizado = normalizeDecimalValue(valorContratoAtualizado)
  const normalizedVencimentoGeral = normalizeXmlDateInput(vencimentoGeral) || normalizeRequestValue(vencimentoGeral)
  const normalizedMesRenovacao = normalizeRequestValue(mesRenovacao).toUpperCase().slice(0, 50)
  const normalizedTpOptante = normalizeRequestValue(tpOptante).toUpperCase().slice(0, 20)

  if (normalizedCodigoXml !== null && (!Number.isInteger(normalizedCodigoXml) || normalizedCodigoXml <= 0)) {
    errors.codigoXml = 'Codigo XML invalido.'
  }

  if (!normalizedTermoAdesao) {
    errors.termoAdesao = 'Termo de adesao e obrigatorio.'
  }

  if (requireAditivo && (!Number.isInteger(normalizedAditivo) || normalizedAditivo < 0)) {
    errors.aditivo = 'Aditivo deve ser um inteiro positivo.'
  }

  if (!Number.isInteger(normalizedCheckAditivo)) {
    errors.checkAditivo = 'Check do aditivo deve ser um inteiro.'
  }

  if (normalizedCep && !isCepValid(normalizedCep)) {
    errors.cep = 'CEP invalido.'
  }

  if (normalizedInicioVigencia && !isDateInputValid(normalizedInicioVigencia)) {
    errors.inicioVigencia = 'Inicio de vigencia invalido.'
  }

  if (normalizedTerminoVigencia && !isDateInputValid(normalizedTerminoVigencia)) {
    errors.terminoVigencia = 'Termino de vigencia invalido.'
  }

  if (normalizedCompDataAditivo && !isDateInputValid(normalizedCompDataAditivo)) {
    errors.compDataAditivo = 'Comp data do aditivo invalida.'
  }

  if (normalizedDataPubAditivo && !isDateInputValid(normalizedDataPubAditivo)) {
    errors.dataPubAditivo = 'Data de publicacao do aditivo invalida.'
  }

  if (normalizedDataPublicacao && !isDateInputValid(normalizedDataPublicacao)) {
    errors.dataPublicacao = 'Data de publicacao invalida.'
  }

  if (normalizedVencimentoGeral && !isDateInputValid(normalizedVencimentoGeral)) {
    errors.vencimentoGeral = 'Vencimento geral invalido.'
  }

  if (Number.isNaN(normalizedValorContrato)) {
    errors.valorContrato = 'Valor do contrato invalido.'
  }

  if (Number.isNaN(normalizedValorContratoAtualizado)) {
    errors.valorContratoAtualizado = 'Valor do contrato atualizado invalido.'
  }

  if (!normalizedTpOptante) {
    errors.tpOptante = 'Tipo optante e obrigatorio.'
  } else if (!['O', 'N', 'C'].includes(normalizedTpOptante)) {
    errors.tpOptante = 'Tipo optante invalido.'
  }

  let credenciadaItem = null

  if (Number.isInteger(normalizedCredenciadaCodigo) && normalizedCredenciadaCodigo > 0) {
    credenciadaItem = await findCredenciadaByCodigo(normalizedCredenciadaCodigo, executor)
  }

  if (!credenciadaItem) {
    errors.credenciado = 'Credenciado nao encontrado na tabela credenciada.'
  }

  if (Object.keys(errors).length > 0) {
    return {
      status: 400,
      payload: {
        message: 'Corrija os campos do credenciamento termo para continuar.',
        errors,
      },
    }
  }

  return {
    status: 200,
    payload: {
      codigoXml: normalizedCodigoXml,
      credenciadaCodigo: Number(credenciadaItem.codigo),
      cep: normalizedCep,
      termoAdesao: normalizedTermoAdesao,
      sei: normalizedSei,
      aditivo: normalizedAditivo,
      situacaoPublicacao: normalizedSituacaoPublicacao,
      situacaoEmissao: normalizedSituacaoEmissao,
      inicioVigencia: normalizedInicioVigencia,
      terminoVigencia: normalizedTerminoVigencia,
      compDataAditivo: normalizedCompDataAditivo,
      statusAditivo: normalizedStatusAditivo,
      dataPubAditivo: normalizedDataPubAditivo,
      checkAditivo: normalizedCheckAditivo,
      statusTermo: normalizedStatusTermo,
      tipoTermo: normalizedTipoTermo,
      logradouro: normalizedLogradouro,
      bairro: normalizedBairro,
      municipio: normalizedMunicipio,
      especificacaoSei: normalizedEspecificacaoSei,
      valorContrato: normalizedValorContrato,
      dataPublicacao: normalizedDataPublicacao,
      valorContratoAtualizado: normalizedValorContratoAtualizado,
      vencimentoGeral: normalizedVencimentoGeral,
      mesRenovacao: normalizedMesRenovacao,
      tpOptante: normalizedTpOptante,
    },
  }
}

const pickCredenciamentoTermoTextValue = (value, fallbackValue = '') => {
  const normalizedValue = normalizeRequestValue(value)

  if (normalizedValue) {
    return normalizedValue
  }

  return normalizeRequestValue(fallbackValue)
}

const pickCredenciamentoTermoIntegerValue = (value, fallbackValue = null) => {
  if (Number.isInteger(value)) {
    return value
  }

  const normalizedValue = normalizeIntegerValue(value)

  if (Number.isInteger(normalizedValue)) {
    return normalizedValue
  }

  return Number.isInteger(fallbackValue) ? fallbackValue : normalizeIntegerValue(fallbackValue)
}

const pickCredenciamentoTermoDecimalValue = (value, fallbackValue = null) => {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value
  }

  const normalizedValue = normalizeDecimalValue(value)

  if (typeof normalizedValue === 'number' && !Number.isNaN(normalizedValue)) {
    return normalizedValue
  }

  const normalizedFallbackValue = normalizeDecimalValue(fallbackValue)
  return typeof normalizedFallbackValue === 'number' && !Number.isNaN(normalizedFallbackValue)
    ? normalizedFallbackValue
    : null
}

const formatCredenciamentoTermoDateValue = (value) => {
  const year = value.getFullYear()
  const month = String(value.getMonth() + 1).padStart(2, '0')
  const day = String(value.getDate()).padStart(2, '0')
  return `${year}-${month}-${day}`
}

const addDaysToCredenciamentoTermoDate = (value, days) => {
  const result = new Date(value)
  result.setDate(result.getDate() + days)
  return result
}

const addYearsToCredenciamentoTermoDate = (value, years) => {
  const result = new Date(value)
  result.setFullYear(result.getFullYear() + years)
  return result
}

const buildCredenciamentoTermoCreatePayload = (payload, latestTermoItem) => {
  const today = new Date()
  const inicioVigencia = formatCredenciamentoTermoDateValue(today)
  const terminoVigencia = formatCredenciamentoTermoDateValue(addDaysToCredenciamentoTermoDate(today, 365))
  const vencimentoGeral = formatCredenciamentoTermoDateValue(addYearsToCredenciamentoTermoDate(today, 5))

  return {
    codigoXml: pickCredenciamentoTermoIntegerValue(payload.codigoXml, latestTermoItem?.codigo_xml),
    credenciadaCodigo: pickCredenciamentoTermoIntegerValue(payload.credenciadaCodigo, latestTermoItem?.credenciada_codigo),
    termoAdesao: pickCredenciamentoTermoTextValue(payload.termoAdesao, latestTermoItem?.termo_adesao),
    sei: pickCredenciamentoTermoTextValue(payload.sei, latestTermoItem?.sei),
    situacaoPublicacao: 'PUBLICAR',
    situacaoEmissao: 'EMITIR',
    inicioVigencia,
    terminoVigencia,
    compDataAditivo: pickCredenciamentoTermoTextValue(payload.compDataAditivo, latestTermoItem?.comp_data_aditivo),
    statusAditivo: 'PUBLICAR',
    dataPubAditivo: pickCredenciamentoTermoTextValue(payload.dataPubAditivo, latestTermoItem?.data_pub_aditivo),
    checkAditivo: pickCredenciamentoTermoIntegerValue(payload.checkAditivo, latestTermoItem?.check_aditivo),
    statusTermo: 'ATIVO',
    tipoTermo: pickCredenciamentoTermoTextValue(payload.tipoTermo, latestTermoItem?.tipo_termo),
    especificacaoSei: pickCredenciamentoTermoTextValue(payload.especificacaoSei, latestTermoItem?.especificacao_sei),
    valorContrato: pickCredenciamentoTermoDecimalValue(payload.valorContrato, latestTermoItem?.valor_contrato),
    folhas: pickCredenciamentoTermoTextValue(payload.folhas, latestTermoItem?.folhas),
    dataPublicacao: pickCredenciamentoTermoTextValue(payload.dataPublicacao, latestTermoItem?.data_publicacao),
    valorContratoAtualizado: pickCredenciamentoTermoDecimalValue(payload.valorContratoAtualizado, latestTermoItem?.valor_contrato_atualizado),
    vencimentoGeral,
    mesRenovacao: pickCredenciamentoTermoTextValue(payload.mesRenovacao, latestTermoItem?.mes_renovacao),
    tpOptante: pickCredenciamentoTermoTextValue(payload.tpOptante, latestTermoItem?.tp_optante),
  }
}

const getCredenciamentoTermoPersistenceError = (error, fallbackMessage) => {
  if (error && typeof error === 'object') {
    const databaseError = error
    const constraintName = typeof databaseError.constraint === 'string' ? databaseError.constraint : ''
    const errorCode = typeof databaseError.code === 'string' ? databaseError.code : ''
    const errorMessage = error instanceof Error ? error.message : ''
    const isUniqueViolation = errorCode === '23505' || /duplicate key|unicidade/i.test(errorMessage)

    if (constraintName === 'termo_codigo_xml_aditivo_uk' || isUniqueViolation) {
      return {
        status: 409,
        message: 'Codigo XML e aditivo ja cadastrados.',
      }
    }
  }

  return {
    status: 500,
    message: fallbackMessage,
  }
}

const findCredenciamentoTermoByTermoAdesao = async (termoAdesao, executor = pool) => {
  const normalizedTermo = normalizeOrdemServicoTermoAdesao(termoAdesao)

  if (!normalizedTermo) {
    return null
  }

  const result = await executor.query(
      `SELECT
        ${credenciamentoTermoSelectClause}
     FROM ${credenciamentoTermoTableName}
     WHERE UPPER(BTRIM(COALESCE(termo_adesao, ''))) = UPPER($1)
     ORDER BY aditivo DESC, CAST(codigo AS integer) DESC LIMIT 1`,
    [normalizedTermo],
  )

  return result.rows[0] ?? null
}

const findLatestCredenciamentoTermoByTermoAdesao = async (termoAdesao, executor = pool, options = {}) => {
  const normalizedTermo = normalizeOrdemServicoTermoAdesao(termoAdesao)

  if (!normalizedTermo) {
    return null
  }

  const lockClause = options.forUpdate ? ' FOR UPDATE' : ''
  const result = await executor.query(
    `SELECT
        t.codigo,
        t.termo_adesao,
        t.aditivo,
        COALESCE(BTRIM(t.status_termo), '') AS status_termo,
        TO_CHAR(t.termino_vigencia::date, 'YYYY-MM-DD') AS termino_vigencia
      FROM ${credenciamentoTermoTableName} t
      WHERE UPPER(BTRIM(COALESCE(t.termo_adesao, ''))) = UPPER($1)
      ORDER BY t.aditivo DESC, t.codigo DESC
      LIMIT 1${lockClause}`,
    [normalizedTermo],
  )

  return result.rows[0] ?? null
}

const findEmissaoDocumentoParametroByDate = async (dataReferencia, executor = pool) => {
  const normalizedDateKey = normalizeEmissaoDocumentoDateKey(dataReferencia) || buildCurrentEmissaoDocumentoDateKey()

  if (!isEmissaoDocumentoDateKeyValid(normalizedDateKey)) {
    return null
  }

  const result = await executor.query(
    `SELECT
       ${emissaoDocumentoParametroSelectClause}
     FROM ${emissaoDocumentoParametroTableName}
     WHERE TO_DATE(BTRIM(data_referencia), 'DD/MM/YYYY') <= TO_DATE($1, 'DD/MM/YYYY')
     ORDER BY TO_DATE(BTRIM(data_referencia), 'DD/MM/YYYY') DESC
     LIMIT 1`,
    [normalizedDateKey],
  )

  return result.rows[0] ?? null
}

const validateOrdemServicoPayload = async ({
  codigo,
  codigoAccess,
  termoAdesao,
  numOs,
  revisao,
  vigenciaOs,
  credenciado,
  cnpjCpf,
  dreCodigo,
  modalidadeDescricao,
  cpfCondutor,
  dataAdmissaoCondutor,
  cpfPreposto,
  prepostoInicio,
  prepostoDias,
  crm,
  cpfMonitor,
  dataAdmissaoMonitor,
  situacao,
  tipoTroca,
  conexao,
  dataEncerramento,
  anotacao,
  uniaoTermos,
  substitutionSourceCodigo = null,
  originalCodigo = null,
  importMode = false,
  skipVigenciaValidation = false,
  requireCodigo = !importMode,
}) => {
  const normalizedCodigo = normalizeCondutorCodigo(codigo)
  const normalizedCodigoAccess = normalizeRequestValue(codigoAccess).slice(0, 50)
  const normalizedTermoAdesao = normalizeOrdemServicoTermoAdesao(termoAdesao)
  const normalizedNumOs = normalizeOperationalCode(numOs, 10)
  const normalizedRevisao = normalizeOperationalCode(revisao, 30) || ordemServicoSemRevisaoLabel
  const normalizedVigenciaOs = normalizeRequestValue(vigenciaOs)
  const normalizedCnpjCpf = normalizeCnpjCpf(cnpjCpf)
  const normalizedDreCodigo = normalizeRequestValue(dreCodigo).toUpperCase().slice(0, 30)
  const normalizedModalidadeDescricao = normalizeModalidadeDescriptionKey(modalidadeDescricao)
  const normalizedCpfCondutor = normalizeCpf(cpfCondutor)
  const normalizedDataAdmissaoCondutor = normalizeRequestValue(dataAdmissaoCondutor)
  const normalizedCpfPreposto = normalizeCpf(cpfPreposto)
  const normalizedPrepostoInicio = normalizeRequestValue(prepostoInicio)
  const normalizedPrepostoDias = normalizeVehicleInteger(prepostoDias, 3)
  const normalizedCrm = normalizeVehicleCrm(crm)
  const normalizedCpfMonitor = normalizeCpf(cpfMonitor)
  const normalizedDataAdmissaoMonitor = normalizeRequestValue(dataAdmissaoMonitor)
  const normalizedSituacao = normalizeCredenciamentoSituacao(situacao)
  const normalizedTipoTroca = normalizeTrocaText(tipoTroca, 255)
  const normalizedConexao = normalizeOperationalCode(conexao, 50)
  const normalizedDataEncerramento = normalizeRequestValue(dataEncerramento)
  const normalizedAnotacao = normalizeCredenciamentoAnnotation(anotacao)
  const normalizedUniaoTermos = normalizeOperationalCode(uniaoTermos, 255)
  const normalizedSubstitutionSourceCodigo = normalizeCondutorCodigo(substitutionSourceCodigo)
  const hasValidCpfCondutor = normalizedCpfCondutor && isCpfValid(normalizedCpfCondutor)
  const hasValidCrm = normalizedCrm && isVehicleCrmValid(normalizedCrm)

  if (requireCodigo && normalizedCodigo === null) {
    return { status: 400, payload: { message: 'Codigo e obrigatorio.' } }
  }

  if (requireCodigo && Number.isNaN(normalizedCodigo)) {
    return { status: 400, payload: { message: 'Codigo deve ser um numero inteiro positivo.' } }
  }

  if (importMode && !normalizedCodigoAccess) {
    return { status: 400, payload: { message: 'Codigo access e obrigatorio no XML.' } }
  }

  if (!normalizedNumOs) {
    return { status: 400, payload: { message: 'Num OS e obrigatorio.' } }
  }

  if (!skipVigenciaValidation && normalizedVigenciaOs && !isDateInputValid(normalizedVigenciaOs)) {
    return { status: 400, payload: { message: 'Vigencia da OS invalida.' } }
  }

  if (!skipVigenciaValidation && !importMode && !normalizedVigenciaOs) {
    return { status: 400, payload: { message: 'Vigencia da OS e obrigatoria.' } }
  }

  if (!skipVigenciaValidation && !importMode && normalizedVigenciaOs && isDateBeforeToday(normalizedVigenciaOs)) {
    return { status: 400, payload: { message: 'Vigencia da OS nao pode ser anterior a hoje.' } }
  }

if (importMode && !normalizedCredenciado && !normalizedCnpjCpf) {
    return { status: 400, payload: { message: 'Credenciado e obrigatorio.' } }
  }

  if (!normalizedDreCodigo) {
    return { status: 400, payload: { message: 'DRE e obrigatoria.' } }
  }

  if (!importMode && !hasValidCpfCondutor) {
    return { status: 400, payload: { message: 'CPF do condutor deve conter 11 digitos.' } }
  }

  if (normalizedDataAdmissaoCondutor && !isDateInputValid(normalizedDataAdmissaoCondutor)) {
    return { status: 400, payload: { message: 'Data de admissao do condutor invalida.' } }
  }

  if (normalizedDataAdmissaoCondutor && isDateAfterToday(normalizedDataAdmissaoCondutor)) {
    return { status: 400, payload: { message: 'Data de admissao do condutor nao pode ser futura.' } }
  }

  if (normalizedCpfPreposto && !isCpfValid(normalizedCpfPreposto)) {
    return { status: 400, payload: { message: 'CPF do preposto invalido.' } }
  }

  if (normalizedPrepostoInicio && !isDateInputValid(normalizedPrepostoInicio)) {
    return { status: 400, payload: { message: 'Inicio do preposto invalido.' } }
  }

  if (normalizedPrepostoDias !== null && Number.isNaN(normalizedPrepostoDias)) {
    return { status: 400, payload: { message: 'Dias de preposto invalido.' } }
  }

  if (!importMode && !hasValidCrm) {
    return { status: 400, payload: { message: 'CRM do veiculo invalido.' } }
  }

  if (normalizedCpfMonitor && !isCpfValid(normalizedCpfMonitor)) {
    return { status: 400, payload: { message: 'CPF do monitor invalido.' } }
  }

  if (normalizedDataAdmissaoMonitor && !isDateInputValid(normalizedDataAdmissaoMonitor)) {
    return { status: 400, payload: { message: 'Data de admissao do monitor invalida.' } }
  }

  if (normalizedDataAdmissaoMonitor && isDateAfterToday(normalizedDataAdmissaoMonitor)) {
    return { status: 400, payload: { message: 'Data de admissao do monitor nao pode ser futura.' } }
  }

  if (!normalizedSituacao) {
    return { status: 400, payload: { message: 'Situacao da OS invalida.' } }
  }

  if (normalizedDataEncerramento && !isDateInputValid(normalizedDataEncerramento)) {
    return { status: 400, payload: { message: 'Data de encerramento invalida.' } }
  }

  if (!skipVigenciaValidation && !importMode && normalizedVigenciaOs && normalizedDataEncerramento && normalizedDataEncerramento < normalizedVigenciaOs) {
    return { status: 400, payload: { message: 'Data de encerramento deve ser maior ou igual a vigencia da OS.' } }
  }

  const veiculoItem = hasValidCrm ? await findVeiculoByCrm(normalizedCrm) : null

  if (normalizedSituacao === 'Ativo') {
    const normalizedOriginalCodigo = normalizeCondutorCodigo(originalCodigo)
    const activeCpfExcludeCodigo = Number.isInteger(normalizedSubstitutionSourceCodigo) && normalizedSubstitutionSourceCodigo > 0
      ? normalizedSubstitutionSourceCodigo
      : Number.isInteger(normalizedOriginalCodigo) && normalizedOriginalCodigo > 0
        ? normalizedOriginalCodigo
        : Number.isInteger(normalizedCodigo) && normalizedCodigo > 0
          ? normalizedCodigo
          : null

    const cpfChecks = [
      { cpf: normalizedCpfCondutor, label: 'condutor' },
      { cpf: normalizedCpfPreposto, label: 'preposto' },
      { cpf: normalizedCpfMonitor, label: 'monitor' },
    ]

    for (const cpfCheck of cpfChecks) {
      if (!cpfCheck.cpf) {
        continue
      }

      const activeOrdemServico = await findActiveOrdemServicoByCpf(
        cpfCheck.cpf,
        { excludeCodigo: activeCpfExcludeCodigo },
        conexao ?? pool,
      )

      if (activeOrdemServico) {
        return {
          status: 409,
          payload: {
            message: `CPF do ${cpfCheck.label} ja esta vinculado a OrdemServico ativa ${activeOrdemServico.codigo} como ${activeOrdemServico.papel}.`,
          },
        }
      }
    }

    const activeVehiclePlaca = veiculoItem ? normalizeOperationalCode(veiculoItem.placas, 20) : ''

    if (activeVehiclePlaca) {
      const activeOrdemServicoByPlaca = await findActiveOrdemServicoByPlaca(
        activeVehiclePlaca,
        { excludeCodigo: activeCpfExcludeCodigo },
        conexao ?? pool,
      )

      if (activeOrdemServicoByPlaca) {
        const activeOsLabel = [
          activeOrdemServicoByPlaca.termo_adesao,
          activeOrdemServicoByPlaca.num_os,
          activeOrdemServicoByPlaca.revisao,
        ].filter(Boolean).join('-')
        const activePlacaLabel = normalizeOperationalCode(activeOrdemServicoByPlaca.veiculo_placas, 20)
        return {
          status: 409,
          payload: {
            message: activeOsLabel
              ? `Placa ${activePlacaLabel} ja utilizada na OrdemServico ativa ${activeOrdemServicoByPlaca.codigo} (${activeOsLabel}).`
              : `Placa ${activePlacaLabel} ja utilizada na OrdemServico ativa ${activeOrdemServicoByPlaca.codigo}.`,
          },
        }
      }
    }
  }

  if (requireCodigo) {
    const duplicateCodeResult = await pool.query(
      `SELECT 1
      FROM ${ordemServicoTableName}
       WHERE codigo = $1
         AND ($2::int IS NULL OR codigo <> $2)
       LIMIT 1`,
      [normalizedCodigo, originalCodigo],
    )

    if (duplicateCodeResult.rowCount > 0) {
      return { status: 409, payload: { message: 'Codigo ja cadastrado.' } }
    }
  }

  let termoCodigo = null
  let termoCredenciado = ''
  let termoCnpjCpf = ''

  if (!importMode) {
    const credenciamentoTermoItem = await findCredenciamentoTermoByTermoAdesao(normalizedTermoAdesao)

    if (!credenciamentoTermoItem) {
      return { status: 400, payload: { message: 'Termo de adesao nao encontrado na tabela termo.' } }
    }

    termoCodigo = Number(credenciamentoTermoItem.codigo)
    termoCredenciado = normalizeCredenciadaText(credenciamentoTermoItem.credenciado, 255)
    termoCnpjCpf = normalizeCnpjCpf(credenciamentoTermoItem.cnpj_cpf)
  } else {
    const credenciamentoTermoItem = await findCredenciamentoTermoByTermoAdesao(normalizedTermoAdesao)
    if (credenciamentoTermoItem) {
      termoCodigo = Number(credenciamentoTermoItem.codigo)
      termoCredenciado = normalizeCredenciadaText(credenciamentoTermoItem.credenciado, 255)
      termoCnpjCpf = normalizeCnpjCpf(credenciamentoTermoItem.cnpj_cpf)
    }
  }

  const dreItem = await findDreByCodigo(normalizedDreCodigo)

  if (!dreItem) {
    return { status: 400, payload: { message: 'DRE nao encontrada.' } }
  }

  const canonicalDreItem = await resolveCanonicalOrdemServicoDre(dreItem)

  const derivedModalidadeDescricao = deriveModalidadeDescricaoFromDreDescricao(dreItem.descricao)

  const modalidadeItem = derivedModalidadeDescricao
    ? await findModalidadeByCodigoOrDescription({ descricao: derivedModalidadeDescricao })
    : null

  if (derivedModalidadeDescricao && !modalidadeItem) {
    return { status: 400, payload: { message: `Modalidade derivada da DRE nao encontrada: ${derivedModalidadeDescricao}.` } }
  }

  const condutorItem = hasValidCpfCondutor ? await findCondutorByCpf(normalizedCpfCondutor) : null

  if (!condutorItem && hasValidCpfCondutor && !importMode) {
    return { status: 400, payload: { message: 'CPF do condutor nao encontrado na tabela condutor.' } }
  }

  const prepostoItem = normalizedCpfPreposto ? await findCondutorByCpf(normalizedCpfPreposto) : null

  if (normalizedCpfPreposto && !prepostoItem) {
    return { status: 400, payload: { message: 'CPF do preposto nao encontrado na tabela condutor.' } }
  }

  if (!veiculoItem && hasValidCrm && !importMode) {
    return { status: 400, payload: { message: 'CRM nao encontrado na tabela veiculo.' } }
  }

  const monitorItem = normalizedCpfMonitor ? await findMonitorByCpf(normalizedCpfMonitor) : null

  if (normalizedCpfMonitor && !monitorItem) {
    return { status: 400, payload: { message: 'CPF do monitor nao encontrado na tabela monitor.' } }
  }

  const tipoTrocaItem = normalizedTipoTroca
    ? await findTrocaByCodigoOrDescricao({ descricao: normalizedTipoTroca })
    : null

  if (normalizedTipoTroca && !tipoTrocaItem) {
    return { status: 400, payload: { message: 'Tipo de troca nao encontrado na tabela tipo_troca.' } }
  }

  return {
    status: 200,
    payload: {
      codigo: normalizedCodigo,
      codigoAccess: normalizedCodigoAccess,
      termoAdesao: normalizedTermoAdesao,
      numOs: normalizedNumOs,
      revisao: normalizedRevisao,
      osConcat: buildOrdemServicoConcat({ termoAdesao: normalizedTermoAdesao, numOs: normalizedNumOs, revisao: normalizedRevisao }),
      vigenciaOs: normalizedVigenciaOs,
      termoCodigo,
      credenciado: termoCredenciado || normalizeCredenciadaText(normalizedCredenciado, 255),
      cnpjCpf: termoCnpjCpf || normalizedCnpjCpf,
      dreCodigo: normalizeDreOperationalCode(canonicalDreItem.codigo_operacional || canonicalDreItem.codigo),
      dreDescricao: normalizeOperationalCode(canonicalDreItem.descricao, 255),
      modalidadeCodigo: modalidadeItem ? Number(modalidadeItem.codigo) : null,
      modalidadeDescricao: modalidadeItem ? normalizeOperationalCode(derivedModalidadeDescricao, 255) : normalizedModalidadeDescricao,
      cpfCondutor: condutorItem ? normalizeCpf(condutorItem.cpf_condutor) : hasValidCpfCondutor ? normalizedCpfCondutor : '',
      condutor: condutorItem ? normalizeCondutorName(condutorItem.condutor) : '',
      dataAdmissaoCondutor: normalizedDataAdmissaoCondutor,
      cpfPreposto: prepostoItem ? normalizeCpf(prepostoItem.cpf_condutor) : '',
      prepostoCondutor: prepostoItem ? normalizeCondutorName(prepostoItem.condutor) : '',
      prepostoInicio: normalizedPrepostoInicio,
      prepostoDias: normalizedPrepostoDias,
      crm: veiculoItem ? normalizeVehicleCrm(veiculoItem.crm) : hasValidCrm ? normalizedCrm : '',
      veiculoPlacas: veiculoItem ? normalizeOperationalCode(veiculoItem.placas, 20) : '',
      cpfMonitor: monitorItem ? normalizeCpf(monitorItem.cpf_monitor) : '',
      monitor: monitorItem ? normalizeCondutorName(monitorItem.monitor) : '',
      dataAdmissaoMonitor: normalizedDataAdmissaoMonitor,
      situacao: normalizedSituacao,
      tipoTrocaCodigo: tipoTrocaItem ? Number(tipoTrocaItem.codigo) : null,
      tipoTrocaDescricao: tipoTrocaItem ? normalizeTrocaText(tipoTrocaItem.lista, 255) : '',
      conexao: normalizedConexao,
      dataEncerramento: normalizedDataEncerramento,
      anotacao: normalizedAnotacao,
      uniaoTermos: normalizedUniaoTermos,
    },
  }
}

const validateTrocaPayload = async ({ codigo, controle, lista, originalCodigo = null }) => {
  const normalizedCodigo = normalizeCondutorCodigo(codigo)
  const normalizedControle = normalizeCondutorCodigo(controle)
  const normalizedLista = normalizeTrocaText(lista, 255)

  if (normalizedCodigo !== null && Number.isNaN(normalizedCodigo)) {
    return { status: 400, payload: { message: 'Codigo deve ser um numero inteiro positivo.' } }
  }

  if (normalizedControle === null) {
    return { status: 400, payload: { message: 'Controle e obrigatorio.' } }
  }

  if (Number.isNaN(normalizedControle)) {
    return { status: 400, payload: { message: 'Controle deve ser um numero inteiro positivo.' } }
  }

  if (!normalizedLista) {
    return { status: 400, payload: { message: 'Lista e obrigatoria.' } }
  }

  if (normalizedCodigo !== null) {
    const duplicateCodeResult = await pool.query(
      `SELECT 1
       FROM tipo_troca
       WHERE codigo = $1
         AND ($2::int IS NULL OR codigo <> $2)
       LIMIT 1`,
      [normalizedCodigo, originalCodigo],
    )

    if (duplicateCodeResult.rowCount > 0) {
      return { status: 409, payload: { message: 'Codigo ja cadastrado.' } }
    }
  }

  const duplicateControleResult = await pool.query(
    `SELECT 1
     FROM tipo_troca
     WHERE controle = $1
       AND ($2::int IS NULL OR codigo <> $2)
     LIMIT 1`,
    [normalizedControle, originalCodigo],
  )

  if (duplicateControleResult.rowCount > 0) {
    return { status: 409, payload: { message: 'Controle ja cadastrado.' } }
  }

  const duplicateListaResult = await pool.query(
    `SELECT 1
     FROM tipo_troca
     WHERE BTRIM(lista) = $1
       AND ($2::int IS NULL OR codigo <> $2)
     LIMIT 1`,
    [normalizedLista, originalCodigo],
  )

  if (duplicateListaResult.rowCount > 0) {
    return { status: 409, payload: { message: 'Lista ja cadastrada.' } }
  }

  return {
    status: 200,
    payload: {
      codigo: normalizedCodigo,
      controle: normalizedControle,
      lista: normalizedLista,
    },
  }
}

const validateSeguradoraPayload = async ({ codigo, controle, descricao, originalCodigo = null }) => {
  const normalizedCodigo = normalizeCondutorCodigo(codigo)
  const normalizedControle = normalizeCondutorCodigo(controle)
  const normalizedDescricao = normalizeTrocaText(descricao, 255)

  if (normalizedCodigo !== null && Number.isNaN(normalizedCodigo)) {
    return { status: 400, payload: { message: 'Codigo deve ser um numero inteiro positivo.' } }
  }

  if (normalizedControle === null) {
    return { status: 400, payload: { message: 'Controle e obrigatorio.' } }
  }

  if (Number.isNaN(normalizedControle)) {
    return { status: 400, payload: { message: 'Controle deve ser um numero inteiro positivo.' } }
  }

  if (!normalizedDescricao) {
    return { status: 400, payload: { message: 'Descricao e obrigatoria.' } }
  }

  if (normalizedCodigo !== null) {
    const duplicateCodeResult = await pool.query(
      `SELECT 1
       FROM seguradora
       WHERE codigo = $1
         AND ($2::int IS NULL OR codigo <> $2)
       LIMIT 1`,
      [normalizedCodigo, originalCodigo],
    )

    if (duplicateCodeResult.rowCount > 0) {
      return { status: 409, payload: { message: 'Codigo ja cadastrado.' } }
    }
  }

  return {
    status: 200,
    payload: {
      codigo: normalizedCodigo,
      controle: normalizedControle,
      descricao: normalizedDescricao,
    },
  }
}

const validateLoginDrePayload = async ({ loginCodigo, dreCodigo, originalLoginCodigo = null, originalDreCodigo = null }) => {
  const normalizedLoginCodigo = Number(normalizeRequestValue(loginCodigo))
  const normalizedDreCodigo = Number(normalizeRequestValue(dreCodigo))

  if (!Number.isInteger(normalizedLoginCodigo) || normalizedLoginCodigo <= 0) {
    return { status: 400, payload: { message: 'Codigo do login e obrigatorio.' } }
  }

  if (!Number.isInteger(normalizedDreCodigo) || normalizedDreCodigo <= 0) {
    return { status: 400, payload: { message: 'Codigo da DRE e obrigatorio.' } }
  }

  const loginResult = await pool.query(
    'SELECT codigo::text AS codigo, BTRIM(nome) AS nome FROM login WHERE codigo = $1 LIMIT 1',
    [normalizedLoginCodigo],
  )

  if (loginResult.rowCount === 0) {
    return { status: 404, payload: { message: 'Login nao encontrado.' } }
  }

  const dreResult = await pool.query(
    "SELECT CAST(codigo AS text) AS codigo, COALESCE(BTRIM(sigla), '') AS sigla, BTRIM(CAST(descricao AS text)) AS descricao FROM dre WHERE codigo = $1 LIMIT 1",
    [normalizedDreCodigo],
  )

  if (dreResult.rowCount === 0) {
    return { status: 404, payload: { message: 'DRE nao encontrada.' } }
  }

  const duplicateResult = await pool.query(
    `SELECT 1
     FROM login_dre
     WHERE login_codigo = $1
       AND dre_codigo = $2
       AND NOT (login_codigo = COALESCE($3, -1) AND dre_codigo = COALESCE($4, -1))
     LIMIT 1`,
    [normalizedLoginCodigo, normalizedDreCodigo, originalLoginCodigo, originalDreCodigo],
  )

  if (duplicateResult.rowCount > 0) {
    return { status: 409, payload: { message: 'Relacionamento login x DRE ja cadastrado.' } }
  }

  return {
    status: 200,
    payload: {
      loginCodigo: normalizedLoginCodigo,
      dreCodigo: normalizedDreCodigo,
      loginNome: loginResult.rows[0].nome,
      dreDescricao: dreResult.rows[0].descricao,
    },
  }
}

const validateLoginCodigo = async (loginCodigo) => {
  const normalizedLoginCodigo = Number(normalizeRequestValue(loginCodigo))

  if (!Number.isInteger(normalizedLoginCodigo) || normalizedLoginCodigo <= 0) {
    return { status: 400, payload: { message: 'Codigo do login e obrigatorio.' } }
  }

  const loginResult = await pool.query(
    'SELECT codigo::text AS codigo, BTRIM(nome) AS nome FROM login WHERE codigo = $1 LIMIT 1',
    [normalizedLoginCodigo],
  )

  if (loginResult.rowCount === 0) {
    return { status: 404, payload: { message: 'Login nao encontrado.' } }
  }

  return {
    status: 200,
    payload: {
      loginCodigo: normalizedLoginCodigo,
      loginNome: loginResult.rows[0].nome,
    },
  }
}

const validateAccessPayload = async ({ nome, email, password, originalCodigo = null, requirePassword = true }) => {
  const normalizedNome = normalizeAccessName(nome)
  const normalizedEmail = normalizeRequestValue(email)
  const normalizedPassword = normalizeRequestValue(password)

  if (!normalizedNome) {
    return { status: 400, payload: { message: 'Nome e obrigatorio.' } }
  }

  if (!isAccessNameValid(normalizedNome)) {
    return { status: 400, payload: { message: 'Nome deve conter apenas letras maiusculas e no maximo 50 caracteres.' } }
  }

  if (!normalizedEmail) {
    return { status: 400, payload: { message: 'Email e obrigatorio.' } }
  }

  if (requirePassword && !normalizedPassword) {
    return { status: 400, payload: { message: 'Senha e obrigatoria.' } }
  }

  const duplicateNameResult = await pool.query(
    `SELECT 1
     FROM login
     WHERE UPPER(BTRIM(nome)) = UPPER($1)
       AND ($2::int IS NULL OR codigo <> $2)
     LIMIT 1`,
    [normalizedNome, originalCodigo],
  )

  if (duplicateNameResult.rowCount > 0) {
    return { status: 409, payload: { message: 'Nome ja cadastrado.' } }
  }

  const duplicateEmailResult = await pool.query(
    `SELECT 1
     FROM login
     WHERE LOWER(TRIM(email)) = LOWER($1)
       AND ($2::int IS NULL OR codigo <> $2)
     LIMIT 1`,
    [normalizedEmail, originalCodigo],
  )

  if (duplicateEmailResult.rowCount > 0) {
    return { status: 409, payload: { message: 'Email ja cadastrado.' } }
  }

  return {
    status: 200,
    payload: {
      nome: normalizedNome,
      email: normalizedEmail,
      password: normalizedPassword,
    },
  }
}

const createAccess = async (nome, email, password) => {
  const validationResult = await validateAccessPayload({
    nome,
    email,
    password,
    requirePassword: true,
  })

  if (validationResult.status !== 200) {
    return validationResult
  }

  const { nome: normalizedNome, email: normalizedEmail, password: normalizedPassword } = validationResult.payload
  const existingUser = await pool.query(
    'SELECT 1 FROM login WHERE LOWER(TRIM(email)) = LOWER($1) LIMIT 1',
    [normalizedEmail],
  )

  if (existingUser.rowCount > 0) {
    return { status: 409, payload: { message: 'Email ja cadastrado.' } }
  }

  const passwordPayload = createAccessHashPayload(normalizedPassword)
  const insertResult = await pool.query(
    `INSERT INTO login (nome, email, password, descricao)
     VALUES ($1, $2, $3, $4)
     RETURNING codigo::text AS codigo, BTRIM(nome) AS nome, TRIM(email) AS email`,
    [normalizedNome, normalizedEmail, passwordPayload.password, passwordPayload.descricao],
  )

  return {
    status: 201,
    payload: {
      message: 'Acesso cadastrado com sucesso.',
      item: insertResult.rows[0],
      user: insertResult.rows[0],
    },
  }
}

const ensureDatabaseSchema = async () => {
  await pool.query('CREATE SEQUENCE IF NOT EXISTS login_codigo_seq START WITH 1 INCREMENT BY 1')
  await pool.query('ALTER TABLE login ADD COLUMN IF NOT EXISTS codigo integer')
  await pool.query('ALTER TABLE login ALTER COLUMN codigo SET DEFAULT nextval(\'login_codigo_seq\')')
  await pool.query('ALTER SEQUENCE login_codigo_seq OWNED BY login.codigo')
  await pool.query('UPDATE login SET codigo = nextval(\'login_codigo_seq\') WHERE codigo IS NULL')
  await pool.query('SELECT setval(\'login_codigo_seq\', GREATEST(COALESCE((SELECT MAX(codigo) FROM login), 0), 1), true)')
  await pool.query('ALTER TABLE login ADD COLUMN IF NOT EXISTS nome varchar(50)')
  await pool.query('ALTER TABLE login ADD COLUMN IF NOT EXISTS descricao text')
  await pool.query(`
    WITH pending_names AS (
      SELECT
        ctid,
        ROW_NUMBER() OVER (ORDER BY codigo, LOWER(TRIM(email))) AS sequence_number,
        TRIM(email) AS email
      FROM login
      WHERE nome IS NULL OR BTRIM(nome) = ''
    )
    UPDATE login AS target
    SET nome = generated.nome
    FROM (
      SELECT
        ctid,
        LEFT(
          CONCAT(
            COALESCE(NULLIF(REGEXP_REPLACE(UPPER(SPLIT_PART(email, '@', 1)), '[^A-Z ]', '', 'g'), ''), 'USUARIO'),
            CASE WHEN sequence_number > 1 THEN CONCAT(' ', sequence_number::text) ELSE '' END
          ),
          50
        ) AS nome
      FROM pending_names
    ) AS generated
    WHERE target.ctid = generated.ctid
  `)
  await pool.query('UPDATE login SET nome = LEFT(UPPER(BTRIM(nome)), 50) WHERE nome IS NOT NULL')
  await pool.query('ALTER TABLE login ALTER COLUMN codigo SET NOT NULL')
  await pool.query('ALTER TABLE login ALTER COLUMN nome SET NOT NULL')
  await pool.query('CREATE SEQUENCE IF NOT EXISTS dre_codigo_seq START WITH 1 INCREMENT BY 1')
  await pool.query(`
    CREATE TABLE IF NOT EXISTS dre (
      codigo integer PRIMARY KEY DEFAULT nextval('dre_codigo_seq'),
      sigla varchar(2),
      descricao varchar(255) NOT NULL,
      codigo_operacional varchar(30)
    )
  `)
  await pool.query('ALTER TABLE dre ADD COLUMN IF NOT EXISTS sigla varchar(2)')
  await pool.query('ALTER TABLE dre ADD COLUMN IF NOT EXISTS descricao varchar(255)')
  await pool.query('ALTER TABLE dre ADD COLUMN IF NOT EXISTS codigo_operacional varchar(30)')
  await pool.query('ALTER TABLE dre ALTER COLUMN codigo SET DEFAULT nextval(\'dre_codigo_seq\')')
  await pool.query('ALTER SEQUENCE dre_codigo_seq OWNED BY dre.codigo')
  await pool.query('ALTER TABLE dre ALTER COLUMN sigla TYPE varchar(2)')
  await pool.query('ALTER TABLE dre ALTER COLUMN descricao TYPE varchar(255)')
  await pool.query(`UPDATE dre
    SET sigla = LEFT(UPPER(BTRIM(COALESCE(NULLIF(sigla, ''), NULLIF(codigo_operacional, ''), CAST(descricao AS text)))), 2)
    WHERE sigla IS NULL OR BTRIM(sigla) = ''`)
  await pool.query('UPDATE dre SET sigla = UPPER(BTRIM(sigla)) WHERE sigla IS NOT NULL')
  await pool.query('UPDATE dre SET descricao = UPPER(BTRIM(CAST(descricao AS text))) WHERE descricao IS NOT NULL')
  await pool.query('UPDATE dre SET codigo_operacional = UPPER(BTRIM(codigo_operacional)) WHERE codigo_operacional IS NOT NULL')
  await pool.query('ALTER TABLE dre ALTER COLUMN sigla SET NOT NULL')
  await pool.query('ALTER TABLE dre ALTER COLUMN descricao SET NOT NULL')
  await pool.query('SELECT setval(\'dre_codigo_seq\', GREATEST(COALESCE((SELECT MAX(codigo) FROM dre), 0), 1), true)')
  await pool.query('CREATE UNIQUE INDEX IF NOT EXISTS dre_codigo_unique_idx ON dre (codigo)')
  await pool.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS dre_codigo_operacional_unique_idx
    ON dre (UPPER(BTRIM(codigo_operacional)))
    WHERE codigo_operacional IS NOT NULL AND BTRIM(codigo_operacional) <> ''
  `)
  await pool.query('CREATE SEQUENCE IF NOT EXISTS modalidade_codigo_seq START WITH 1 INCREMENT BY 1')
  await pool.query(`
    CREATE TABLE IF NOT EXISTS modalidade (
      codigo integer PRIMARY KEY DEFAULT nextval('modalidade_codigo_seq'),
      descricao varchar(255) NOT NULL
    )
  `)
  await pool.query('ALTER TABLE modalidade ADD COLUMN IF NOT EXISTS descricao varchar(255)')
  await pool.query('ALTER TABLE modalidade ALTER COLUMN codigo SET DEFAULT nextval(\'modalidade_codigo_seq\')')
  await pool.query('ALTER SEQUENCE modalidade_codigo_seq OWNED BY modalidade.codigo')
  await pool.query('ALTER TABLE modalidade ALTER COLUMN descricao TYPE varchar(255)')
  await pool.query('UPDATE modalidade SET descricao = UPPER(BTRIM(CAST(descricao AS text))) WHERE descricao IS NOT NULL')
  await pool.query('ALTER TABLE modalidade ALTER COLUMN descricao SET NOT NULL')
  await ensureDefaultModalidadeEntries()
  await pool.query('SELECT setval(\'modalidade_codigo_seq\', GREATEST(COALESCE((SELECT MAX(codigo) FROM modalidade), 0), 1), true)')
  await pool.query('CREATE UNIQUE INDEX IF NOT EXISTS modalidade_codigo_unique_idx ON modalidade (codigo)')
  await pool.query('CREATE UNIQUE INDEX IF NOT EXISTS modalidade_descricao_unique_idx ON modalidade (UPPER(BTRIM(descricao)))')
  await pool.query('CREATE SEQUENCE IF NOT EXISTS condutor_codigo_seq START WITH 1 INCREMENT BY 1')
  await pool.query('ALTER TABLE condutor ADD COLUMN IF NOT EXISTS data_inclusao timestamp without time zone')
  await pool.query('ALTER TABLE condutor ADD COLUMN IF NOT EXISTS data_modificacao timestamp without time zone')
  await pool.query('ALTER TABLE condutor ALTER COLUMN codigo SET DEFAULT nextval(\'condutor_codigo_seq\')')
  await pool.query('ALTER TABLE condutor ALTER COLUMN data_inclusao SET DEFAULT NOW()')
  await pool.query('ALTER TABLE condutor ALTER COLUMN data_modificacao SET DEFAULT NOW()')
  await pool.query('ALTER TABLE condutor ALTER COLUMN validade_crmc DROP NOT NULL')
  await pool.query('ALTER TABLE condutor ALTER COLUMN validade_curso DROP NOT NULL')
  await pool.query('ALTER SEQUENCE condutor_codigo_seq OWNED BY condutor.codigo')
  await pool.query(`
    CREATE TABLE IF NOT EXISTS condutor_import_recusa (
      id bigserial PRIMARY KEY,
      arquivo_xml varchar(255) NOT NULL,
      linha_xml integer NOT NULL,
      codigo_xml varchar(50),
      condutor_xml varchar(150),
      cpf_condutor_xml varchar(20),
      crmc_xml varchar(20),
      tipo_vinculo_xml varchar(50),
      motivo_recusa text NOT NULL,
      data_importacao timestamp without time zone NOT NULL DEFAULT NOW()
    )
  `)
  await pool.query(`
    UPDATE condutor
    SET data_inclusao = COALESCE(data_inclusao, NOW()),
        data_modificacao = COALESCE(data_modificacao, COALESCE(data_inclusao, NOW()))
    WHERE data_inclusao IS NULL OR data_modificacao IS NULL
  `)
  await pool.query('SELECT setval(\'condutor_codigo_seq\', GREATEST(COALESCE((SELECT MAX(codigo) FROM condutor), 0), 1), true)')
  await pool.query('CREATE UNIQUE INDEX IF NOT EXISTS login_codigo_unique_idx ON login (codigo)')
  await pool.query('CREATE UNIQUE INDEX IF NOT EXISTS condutor_codigo_unique_idx ON condutor (codigo)')
  await pool.query('CREATE INDEX IF NOT EXISTS condutor_import_recusa_data_idx ON condutor_import_recusa (data_importacao DESC)')
  await pool.query('CREATE INDEX IF NOT EXISTS condutor_import_recusa_arquivo_idx ON condutor_import_recusa (arquivo_xml)')
  await pool.query('CREATE SEQUENCE IF NOT EXISTS monitor_codigo_seq START WITH 1 INCREMENT BY 1')
  await pool.query(`
    CREATE TABLE IF NOT EXISTS monitor (
      codigo integer PRIMARY KEY DEFAULT nextval('monitor_codigo_seq'),
      monitor varchar(255) NOT NULL,
      rg_monitor varchar(20),
      cpf_monitor varchar(14) NOT NULL,
      curso_monitor date,
      validade_curso date,
      tipo_vinculo varchar(50),
      nascimento date,
      data_inclusao timestamp without time zone NOT NULL DEFAULT NOW(),
      data_modificacao timestamp without time zone NOT NULL DEFAULT NOW()
    )
  `)
  await pool.query('ALTER SEQUENCE monitor_codigo_seq OWNED BY monitor.codigo')
  await pool.query('ALTER TABLE monitor ALTER COLUMN codigo SET DEFAULT nextval(\'monitor_codigo_seq\')')
  await pool.query('ALTER TABLE monitor ALTER COLUMN monitor TYPE varchar(255)')
  await pool.query('ALTER TABLE monitor ALTER COLUMN data_inclusao SET DEFAULT NOW()')
  await pool.query('ALTER TABLE monitor ALTER COLUMN data_modificacao SET DEFAULT NOW()')
  await pool.query(`
    CREATE TABLE IF NOT EXISTS monitor_import_recusa (
      id bigserial PRIMARY KEY,
      arquivo_xml varchar(255) NOT NULL,
      linha_xml integer NOT NULL,
      codigo_xml varchar(50),
      monitor_xml varchar(150),
      cpf_monitor_xml varchar(20),
      rg_monitor_xml varchar(20),
      tipo_vinculo_xml varchar(50),
      motivo_recusa text NOT NULL,
      data_importacao timestamp without time zone NOT NULL DEFAULT NOW()
    )
  `)
  await pool.query(`
    UPDATE monitor
    SET data_inclusao = COALESCE(data_inclusao, NOW()),
        data_modificacao = COALESCE(data_modificacao, COALESCE(data_inclusao, NOW()))
    WHERE data_inclusao IS NULL OR data_modificacao IS NULL
  `)
  await pool.query('SELECT setval(\'monitor_codigo_seq\', GREATEST(COALESCE((SELECT MAX(codigo) FROM monitor), 0), 1), true)')
  await pool.query('CREATE UNIQUE INDEX IF NOT EXISTS monitor_codigo_unique_idx ON monitor (codigo)')
  await pool.query('CREATE INDEX IF NOT EXISTS monitor_import_recusa_data_idx ON monitor_import_recusa (data_importacao DESC)')
  await pool.query('CREATE INDEX IF NOT EXISTS monitor_import_recusa_arquivo_idx ON monitor_import_recusa (arquivo_xml)')
  await pool.query('CREATE SEQUENCE IF NOT EXISTS veiculo_codigo_seq START WITH 1 INCREMENT BY 1')
  await pool.query(`
    CREATE TABLE IF NOT EXISTS veiculo (
      codigo integer PRIMARY KEY DEFAULT nextval('veiculo_codigo_seq'),
      crm varchar(20),
      placas varchar(7),
      ano integer,
      cap_detran integer,
      cap_teg integer,
      cap_teg_creche integer,
      cap_acessivel integer,
      val_crm date,
      seguradora varchar(255),
      seguro_inicio date,
      seguro_termino date,
      tipo_de_bancada varchar(20),
      tipo_de_veiculo varchar(20),
      marca_modelo varchar(255),
      titular varchar(255),
      cnpj_cpf varchar(18),
      valor_veiculo numeric(14, 2),
      os_especial varchar(3),
      data_inclusao timestamp without time zone NOT NULL DEFAULT NOW(),
      data_modificacao timestamp without time zone NOT NULL DEFAULT NOW()
    )
  `)
  await pool.query('ALTER SEQUENCE veiculo_codigo_seq OWNED BY veiculo.codigo')
  await pool.query('ALTER TABLE veiculo ALTER COLUMN codigo SET DEFAULT nextval(\'veiculo_codigo_seq\')')
  await pool.query('ALTER TABLE veiculo ALTER COLUMN data_inclusao SET DEFAULT NOW()')
  await pool.query('ALTER TABLE veiculo ALTER COLUMN data_modificacao SET DEFAULT NOW()')
  await pool.query(`
    CREATE TABLE IF NOT EXISTS veiculo_import_recusa (
      id bigserial PRIMARY KEY,
      arquivo_xml varchar(255) NOT NULL,
      linha_xml integer NOT NULL,
      codigo_xml varchar(50),
      crm_xml varchar(20),
      placas_xml varchar(20),
      tipo_de_veiculo_xml varchar(50),
      motivo_recusa text NOT NULL,
      data_importacao timestamp without time zone NOT NULL DEFAULT NOW()
    )
  `)
  await pool.query(`
    UPDATE veiculo
    SET data_inclusao = COALESCE(data_inclusao, NOW()),
        data_modificacao = COALESCE(data_modificacao, COALESCE(data_inclusao, NOW()))
    WHERE data_inclusao IS NULL OR data_modificacao IS NULL
  `)
  await pool.query('SELECT setval(\'veiculo_codigo_seq\', GREATEST(COALESCE((SELECT MAX(codigo) FROM veiculo), 0), 1), true)')
  await pool.query('CREATE UNIQUE INDEX IF NOT EXISTS veiculo_codigo_unique_idx ON veiculo (codigo)')
  await pool.query('CREATE INDEX IF NOT EXISTS veiculo_placas_idx ON veiculo (placas)')
  await pool.query('CREATE INDEX IF NOT EXISTS veiculo_crm_idx ON veiculo (crm)')
  await pool.query('CREATE INDEX IF NOT EXISTS veiculo_import_recusa_data_idx ON veiculo_import_recusa (data_importacao DESC)')
  await pool.query('CREATE INDEX IF NOT EXISTS veiculo_import_recusa_arquivo_idx ON veiculo_import_recusa (arquivo_xml)')
  await pool.query(`
    DO $$
    BEGIN
      IF EXISTS (
        SELECT 1
        FROM pg_class
        WHERE relkind = 'r'
          AND relname = 'titular'
      ) AND NOT EXISTS (
        SELECT 1
        FROM pg_class
        WHERE relkind = 'r'
          AND relname = 'titularCrm'
      ) THEN
        ALTER TABLE titular RENAME TO "titularCrm";
      END IF;
    END $$;
  `)
  await pool.query(`
    DO $$
    BEGIN
      IF EXISTS (
        SELECT 1
        FROM pg_class
        WHERE relkind = 'S'
          AND relname = 'titular_codigo_seq'
      ) AND NOT EXISTS (
        SELECT 1
        FROM pg_class
        WHERE relkind = 'S'
          AND relname = 'titularCrm_codigo_seq'
      ) THEN
        ALTER SEQUENCE titular_codigo_seq RENAME TO "titularCrm_codigo_seq";
      END IF;
    END $$;
  `)
  await pool.query(`
    DO $$
    BEGIN
      IF EXISTS (
        SELECT 1
        FROM pg_class
        WHERE relkind = 'i'
          AND relname = 'titular_codigo_unique_idx'
      ) AND NOT EXISTS (
        SELECT 1
        FROM pg_class
        WHERE relkind = 'i'
          AND relname = 'titularCrm_codigo_unique_idx'
      ) THEN
        ALTER INDEX titular_codigo_unique_idx RENAME TO "titularCrm_codigo_unique_idx";
      END IF;
    END $$;
  `)
  await pool.query(`CREATE SEQUENCE IF NOT EXISTS ${titularSequenceName} START WITH 1 INCREMENT BY 1`)
  await pool.query(`
    CREATE TABLE IF NOT EXISTS ${titularTableName} (
      codigo integer PRIMARY KEY DEFAULT nextval('${titularSequenceName}'),
      cnpj_cpf varchar(18) NOT NULL,
      titular varchar(255) NOT NULL,
      data_inclusao timestamp without time zone NOT NULL DEFAULT NOW(),
      data_modificacao timestamp without time zone NOT NULL DEFAULT NOW()
    )
  `)
  await pool.query(`ALTER TABLE ${titularTableName} ADD COLUMN IF NOT EXISTS cnpj_cpf varchar(18)`)
  await pool.query(`ALTER TABLE ${titularTableName} ADD COLUMN IF NOT EXISTS titular varchar(255)`)
  await pool.query(`ALTER TABLE ${titularTableName} ADD COLUMN IF NOT EXISTS data_inclusao timestamp without time zone`)
  await pool.query(`ALTER TABLE ${titularTableName} ADD COLUMN IF NOT EXISTS data_modificacao timestamp without time zone`)
  await pool.query(`ALTER TABLE ${titularTableName} ALTER COLUMN codigo SET DEFAULT nextval('${titularSequenceName}')`)
  await pool.query(`ALTER TABLE ${titularTableName} ALTER COLUMN data_inclusao SET DEFAULT NOW()`)
  await pool.query(`ALTER TABLE ${titularTableName} ALTER COLUMN data_modificacao SET DEFAULT NOW()`)
  await pool.query(`ALTER SEQUENCE ${titularSequenceName} OWNED BY ${titularTableName}.codigo`)
  await pool.query(`
    UPDATE ${titularTableName}
    SET data_inclusao = COALESCE(data_inclusao, NOW()),
        data_modificacao = COALESCE(data_modificacao, COALESCE(data_inclusao, NOW()))
    WHERE data_inclusao IS NULL OR data_modificacao IS NULL
  `)
  await pool.query(`ALTER TABLE ${titularTableName} ALTER COLUMN cnpj_cpf SET NOT NULL`)
  await pool.query(`ALTER TABLE ${titularTableName} ALTER COLUMN titular SET NOT NULL`)
  await pool.query(`SELECT setval('${titularSequenceName}', GREATEST(COALESCE((SELECT MAX(codigo) FROM ${titularTableName}), 0), 1), true)`)
  await pool.query(`CREATE UNIQUE INDEX IF NOT EXISTS ${titularUniqueIndexName} ON ${titularTableName} (codigo)`)
  await pool.query(`
    DO $$
    BEGIN
      IF EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name = 'troca'
      )
      AND NOT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name = 'tipo_troca'
      ) THEN
        ALTER TABLE troca RENAME TO tipo_troca;
      END IF;
    END $$;
  `)
  await pool.query('CREATE SEQUENCE IF NOT EXISTS tipo_troca_codigo_seq START WITH 1 INCREMENT BY 1')
  await pool.query(`
    CREATE TABLE IF NOT EXISTS tipo_troca (
      codigo integer PRIMARY KEY DEFAULT nextval('tipo_troca_codigo_seq'),
      controle integer NOT NULL,
      lista varchar(255) NOT NULL,
      data_inclusao timestamp without time zone NOT NULL DEFAULT NOW(),
      data_modificacao timestamp without time zone NOT NULL DEFAULT NOW()
    )
  `)
  await pool.query('ALTER TABLE tipo_troca ADD COLUMN IF NOT EXISTS controle integer')
  await pool.query('ALTER TABLE tipo_troca ADD COLUMN IF NOT EXISTS lista varchar(255)')
  await pool.query('ALTER TABLE tipo_troca ADD COLUMN IF NOT EXISTS data_inclusao timestamp without time zone')
  await pool.query('ALTER TABLE tipo_troca ADD COLUMN IF NOT EXISTS data_modificacao timestamp without time zone')
  await pool.query('ALTER TABLE tipo_troca ALTER COLUMN codigo SET DEFAULT nextval(\'tipo_troca_codigo_seq\')')
  await pool.query('ALTER SEQUENCE tipo_troca_codigo_seq OWNED BY tipo_troca.codigo')
  await pool.query(`
    UPDATE tipo_troca
    SET data_inclusao = COALESCE(data_inclusao, NOW()),
        data_modificacao = COALESCE(data_modificacao, COALESCE(data_inclusao, NOW()))
    WHERE data_inclusao IS NULL OR data_modificacao IS NULL
  `)
  await pool.query('ALTER TABLE tipo_troca ALTER COLUMN controle SET NOT NULL')
  await pool.query('ALTER TABLE tipo_troca ALTER COLUMN lista SET NOT NULL')
  await pool.query('ALTER TABLE tipo_troca ALTER COLUMN data_inclusao SET DEFAULT NOW()')
  await pool.query('ALTER TABLE tipo_troca ALTER COLUMN data_modificacao SET DEFAULT NOW()')
  await pool.query('SELECT setval(\'tipo_troca_codigo_seq\', GREATEST(COALESCE((SELECT MAX(codigo) FROM tipo_troca), 0), 1), true)')
  await pool.query('CREATE UNIQUE INDEX IF NOT EXISTS tipo_troca_controle_unique_idx ON tipo_troca (controle)')
  await pool.query('CREATE UNIQUE INDEX IF NOT EXISTS tipo_troca_lista_unique_idx ON tipo_troca (lista)')
  await pool.query('CREATE SEQUENCE IF NOT EXISTS seguradora_codigo_seq START WITH 1 INCREMENT BY 1')
  await pool.query(`
    CREATE TABLE IF NOT EXISTS seguradora (
      codigo integer PRIMARY KEY DEFAULT nextval('seguradora_codigo_seq'),
      controle integer NOT NULL,
      lista varchar(255) NOT NULL,
      data_inclusao timestamp without time zone NOT NULL DEFAULT NOW(),
      data_modificacao timestamp without time zone NOT NULL DEFAULT NOW()
    )
  `)
  await pool.query('ALTER TABLE seguradora ADD COLUMN IF NOT EXISTS controle integer')
  await pool.query('ALTER TABLE seguradora ADD COLUMN IF NOT EXISTS lista varchar(255)')
  await pool.query('ALTER TABLE seguradora ADD COLUMN IF NOT EXISTS data_inclusao timestamp without time zone')
  await pool.query('ALTER TABLE seguradora ADD COLUMN IF NOT EXISTS data_modificacao timestamp without time zone')
  await pool.query('ALTER TABLE seguradora ALTER COLUMN codigo SET DEFAULT nextval(\'seguradora_codigo_seq\')')
  await pool.query('ALTER SEQUENCE seguradora_codigo_seq OWNED BY seguradora.codigo')
  await pool.query(`
    UPDATE seguradora
    SET data_inclusao = COALESCE(data_inclusao, NOW()),
        data_modificacao = COALESCE(data_modificacao, COALESCE(data_inclusao, NOW()))
    WHERE data_inclusao IS NULL OR data_modificacao IS NULL
  `)
  await pool.query('ALTER TABLE seguradora ALTER COLUMN controle SET NOT NULL')
  await pool.query('ALTER TABLE seguradora ALTER COLUMN lista SET NOT NULL')
  await pool.query('ALTER TABLE seguradora ALTER COLUMN data_inclusao SET DEFAULT NOW()')
  await pool.query('ALTER TABLE seguradora ALTER COLUMN data_modificacao SET DEFAULT NOW()')
  await pool.query('SELECT setval(\'seguradora_codigo_seq\', GREATEST(COALESCE((SELECT MAX(codigo) FROM seguradora), 0), 1), true)')
  await pool.query('DROP INDEX IF EXISTS seguradora_controle_unique_idx')
  await pool.query('DROP INDEX IF EXISTS seguradora_lista_unique_idx')
  await pool.query('CREATE SEQUENCE IF NOT EXISTS marca_modelo_codigo_seq START WITH 1 INCREMENT BY 1')
  await pool.query(`
    CREATE TABLE IF NOT EXISTS marca_modelo (
      codigo varchar(50) PRIMARY KEY DEFAULT nextval('marca_modelo_codigo_seq')::text,
      descricao varchar(255) NOT NULL,
      data_inclusao timestamp without time zone NOT NULL DEFAULT NOW(),
      data_modificacao timestamp without time zone NOT NULL DEFAULT NOW()
    )
  `)
  await pool.query('ALTER TABLE marca_modelo ADD COLUMN IF NOT EXISTS descricao varchar(255)')
  await pool.query('ALTER TABLE marca_modelo ADD COLUMN IF NOT EXISTS data_inclusao timestamp without time zone')
  await pool.query('ALTER TABLE marca_modelo ADD COLUMN IF NOT EXISTS data_modificacao timestamp without time zone')
  await pool.query("ALTER TABLE marca_modelo ALTER COLUMN codigo SET DEFAULT nextval('marca_modelo_codigo_seq')::text")
  await pool.query('ALTER SEQUENCE marca_modelo_codigo_seq OWNED BY marca_modelo.codigo')
  await pool.query(`
    UPDATE marca_modelo
    SET data_inclusao = COALESCE(data_inclusao, NOW()),
        data_modificacao = COALESCE(data_modificacao, COALESCE(data_inclusao, NOW()))
    WHERE data_inclusao IS NULL OR data_modificacao IS NULL
  `)
  await pool.query('ALTER TABLE marca_modelo ALTER COLUMN descricao SET NOT NULL')
  await pool.query('ALTER TABLE marca_modelo ALTER COLUMN data_inclusao SET DEFAULT NOW()')
  await pool.query('ALTER TABLE marca_modelo ALTER COLUMN data_modificacao SET DEFAULT NOW()')
  await pool.query(`
    SELECT setval(
      'marca_modelo_codigo_seq',
      GREATEST(
        COALESCE(
          (
            SELECT MAX(CAST(BTRIM(codigo) AS bigint))
            FROM marca_modelo
            WHERE BTRIM(CAST(codigo AS text)) ~ '^[0-9]+$'
          ),
          0
        ),
        1
      ),
      true
    )
  `)
  await pool.query('CREATE UNIQUE INDEX IF NOT EXISTS marca_modelo_descricao_unique_idx ON marca_modelo (descricao)')
  await pool.query('CREATE SEQUENCE IF NOT EXISTS credenciada_codigo_seq START WITH 1 INCREMENT BY 1')
  await pool.query(`
    CREATE TABLE IF NOT EXISTS credenciada (
      codigo integer PRIMARY KEY DEFAULT nextval('credenciada_codigo_seq'),
      placa varchar(20) NOT NULL,
      empresa varchar(255) NOT NULL,
      condutor varchar(255) NOT NULL,
      tipo_pessoa varchar(20) NOT NULL,
      credenciado varchar(255) NOT NULL,
      cnpj_cpf varchar(20) NOT NULL,
      logradouro varchar(255),
      bairro varchar(120),
      cep varchar(10),
      municipio varchar(120),
      email varchar(255),
      telefone_01 varchar(20),
      telefone_02 varchar(20),
      representante varchar(255),
      cpf_representante varchar(20),
      status varchar(50),
      data_inclusao timestamp without time zone NOT NULL DEFAULT NOW(),
      data_modificacao timestamp without time zone NOT NULL DEFAULT NOW()
    )
  `)
  await pool.query('ALTER TABLE credenciada ADD COLUMN IF NOT EXISTS placa varchar(20)')
  await pool.query('ALTER TABLE credenciada ADD COLUMN IF NOT EXISTS empresa varchar(255)')
  await pool.query('ALTER TABLE credenciada ADD COLUMN IF NOT EXISTS condutor varchar(255)')
  await pool.query('ALTER TABLE credenciada ADD COLUMN IF NOT EXISTS tipo_pessoa varchar(20)')
  await pool.query('ALTER TABLE credenciada ADD COLUMN IF NOT EXISTS credenciado varchar(255)')
  await pool.query('ALTER TABLE credenciada ADD COLUMN IF NOT EXISTS cnpj_cpf varchar(20)')
  await pool.query('ALTER TABLE credenciada ADD COLUMN IF NOT EXISTS logradouro varchar(255)')
  await pool.query('ALTER TABLE credenciada ADD COLUMN IF NOT EXISTS bairro varchar(120)')
  await pool.query('ALTER TABLE credenciada ADD COLUMN IF NOT EXISTS cep varchar(10)')
  await pool.query('ALTER TABLE credenciada ADD COLUMN IF NOT EXISTS municipio varchar(120)')
  await pool.query('ALTER TABLE credenciada ADD COLUMN IF NOT EXISTS email varchar(255)')
  await pool.query('ALTER TABLE credenciada ADD COLUMN IF NOT EXISTS telefone_01 varchar(20)')
  await pool.query('ALTER TABLE credenciada ADD COLUMN IF NOT EXISTS telefone_02 varchar(20)')
  await pool.query('ALTER TABLE credenciada ADD COLUMN IF NOT EXISTS representante varchar(255)')
  await pool.query('ALTER TABLE credenciada ADD COLUMN IF NOT EXISTS cpf_representante varchar(20)')
  await pool.query('ALTER TABLE credenciada ADD COLUMN IF NOT EXISTS status varchar(50)')
  await pool.query('ALTER TABLE credenciada ADD COLUMN IF NOT EXISTS data_inclusao timestamp without time zone')
  await pool.query('ALTER TABLE credenciada ADD COLUMN IF NOT EXISTS data_modificacao timestamp without time zone')
  await pool.query('ALTER TABLE credenciada DROP COLUMN IF EXISTS rg_representante')
  await pool.query(`
    UPDATE credenciada
       SET status = CASE
         WHEN UPPER(BTRIM(COALESCE(status, ''))) = 'CANCELADO' THEN 'CANCELADO'
         ELSE 'ATIVO'
       END
     WHERE status IS NULL
        OR BTRIM(COALESCE(status, '')) = ''
        OR UPPER(BTRIM(COALESCE(status, ''))) <> 'ATIVO'
           AND UPPER(BTRIM(COALESCE(status, ''))) <> 'CANCELADO'
  `)
  await pool.query('ALTER TABLE credenciada ALTER COLUMN codigo SET DEFAULT nextval(\'credenciada_codigo_seq\')')
  await pool.query('ALTER TABLE credenciada ALTER COLUMN data_inclusao SET DEFAULT NOW()')
  await pool.query('ALTER TABLE credenciada ALTER COLUMN data_modificacao SET DEFAULT NOW()')
  await pool.query('ALTER SEQUENCE credenciada_codigo_seq OWNED BY credenciada.codigo')
  await pool.query(`
    CREATE TABLE IF NOT EXISTS credenciada_import_recusa (
      id bigserial PRIMARY KEY,
      arquivo_xml varchar(255) NOT NULL,
      linha_xml integer NOT NULL,
      codigo_xml varchar(50),
      credenciado_xml varchar(255),
      cnpj_cpf_xml varchar(20),
      representante_xml varchar(255),
      status_xml varchar(50),
      motivo_recusa text NOT NULL,
      data_importacao timestamp without time zone NOT NULL DEFAULT NOW()
    )
  `)
  await pool.query(`ALTER TABLE IF EXISTS credenciamento_termo RENAME TO ${credenciamentoTermoTableName}`)
  await pool.query(`ALTER TABLE IF EXISTS credenciamento_termo_import_recusa RENAME TO ${credenciamentoTermoImportRecusaTableName}`)
  await pool.query(`DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_class WHERE relname = 'credenciamento_termo_codigo_seq' AND relkind = 'S') THEN ALTER SEQUENCE credenciamento_termo_codigo_seq RENAME TO ${credenciamentoTermoCodigoSequenceName}; END IF; END $$`)
  await pool.query(`CREATE SEQUENCE IF NOT EXISTS ${credenciamentoTermoCodigoSequenceName} START WITH 1 INCREMENT BY 1`)
  await pool.query(`
    CREATE TABLE IF NOT EXISTS ${credenciamentoTermoTableName} (
      codigo integer PRIMARY KEY DEFAULT nextval('${credenciamentoTermoCodigoSequenceName}'),
      codigo_xml integer NOT NULL,
      credenciada_codigo integer NOT NULL REFERENCES credenciada(codigo),
      credenciado varchar(255) NOT NULL,
      cnpj_cpf varchar(20),
      termo_adesao varchar(255) NOT NULL,
      sei varchar(255),
      aditivo integer NOT NULL,
      situacao_publicacao varchar(100),
      situacao_emissao varchar(100),
      inicio_vigencia date,
      termino_vigencia date,
      comp_data_aditivo date,
      status_aditivo varchar(100),
      data_pub_aditivo date,
      check_aditivo integer NOT NULL,
      status_termo varchar(100),
      tipo_termo varchar(100),
      representante varchar(255),
      cpf_representante varchar(20),
      rg_representante varchar(30),
      logradouro varchar(255),
      bairro varchar(120),
      municipio varchar(120),
      especificacao_sei varchar(255),
      valor_contrato numeric(14, 2),
      objeto text,
      data_publicacao date,
      info_sei varchar(100),
      valor_contrato_atualizado numeric(14, 2),
      vencimento_geral date,
      mes_renovacao varchar(50),
      tp_optante varchar(20),
      data_inclusao timestamp without time zone NOT NULL DEFAULT NOW(),
      data_modificacao timestamp without time zone NOT NULL DEFAULT NOW()
    )
  `)
  await pool.query(`ALTER TABLE ${credenciamentoTermoTableName} ADD COLUMN IF NOT EXISTS codigo_xml integer`)
  await pool.query(`ALTER TABLE ${credenciamentoTermoTableName} ADD COLUMN IF NOT EXISTS credenciada_codigo integer`)
  await pool.query(`ALTER TABLE ${credenciamentoTermoTableName} ADD COLUMN IF NOT EXISTS credenciado varchar(255)`)
  await pool.query(`ALTER TABLE ${credenciamentoTermoTableName} ADD COLUMN IF NOT EXISTS cnpj_cpf varchar(20)`)
  await pool.query(`ALTER TABLE ${credenciamentoTermoTableName} ADD COLUMN IF NOT EXISTS termo_adesao varchar(255)`)
  await pool.query(`ALTER TABLE ${credenciamentoTermoTableName} ADD COLUMN IF NOT EXISTS sei varchar(255)`)
  await pool.query(`ALTER TABLE ${credenciamentoTermoTableName} ADD COLUMN IF NOT EXISTS aditivo integer`)
  await pool.query(`ALTER TABLE ${credenciamentoTermoTableName} ADD COLUMN IF NOT EXISTS situacao_publicacao varchar(100)`)
  await pool.query(`ALTER TABLE ${credenciamentoTermoTableName} ADD COLUMN IF NOT EXISTS situacao_emissao varchar(100)`)
  await pool.query(`ALTER TABLE ${credenciamentoTermoTableName} ADD COLUMN IF NOT EXISTS inicio_vigencia date`)
  await pool.query(`ALTER TABLE ${credenciamentoTermoTableName} ADD COLUMN IF NOT EXISTS termino_vigencia date`)
  await pool.query(`ALTER TABLE ${credenciamentoTermoTableName} ADD COLUMN IF NOT EXISTS comp_data_aditivo date`)
  await pool.query(`ALTER TABLE ${credenciamentoTermoTableName} ADD COLUMN IF NOT EXISTS status_aditivo varchar(100)`)
  await pool.query(`ALTER TABLE ${credenciamentoTermoTableName} ADD COLUMN IF NOT EXISTS data_pub_aditivo date`)
  await pool.query(`ALTER TABLE ${credenciamentoTermoTableName} ADD COLUMN IF NOT EXISTS check_aditivo integer`)
  await pool.query(`ALTER TABLE ${credenciamentoTermoTableName} ADD COLUMN IF NOT EXISTS status_termo varchar(100)`)
  await pool.query(`ALTER TABLE ${credenciamentoTermoTableName} ADD COLUMN IF NOT EXISTS tipo_termo varchar(100)`)
  await pool.query(`ALTER TABLE ${credenciamentoTermoTableName} ADD COLUMN IF NOT EXISTS representante varchar(255)`)
  await pool.query(`ALTER TABLE ${credenciamentoTermoTableName} ADD COLUMN IF NOT EXISTS cpf_representante varchar(20)`)
  await pool.query(`ALTER TABLE ${credenciamentoTermoTableName} ADD COLUMN IF NOT EXISTS rg_representante varchar(30)`)
  await pool.query(`ALTER TABLE ${credenciamentoTermoTableName} ADD COLUMN IF NOT EXISTS logradouro varchar(255)`)
  await pool.query(`ALTER TABLE ${credenciamentoTermoTableName} ADD COLUMN IF NOT EXISTS bairro varchar(120)`)
  await pool.query(`ALTER TABLE ${credenciamentoTermoTableName} ADD COLUMN IF NOT EXISTS municipio varchar(120)`)
  await pool.query(`ALTER TABLE ${credenciamentoTermoTableName} ADD COLUMN IF NOT EXISTS especificacao_sei varchar(255)`)
  await pool.query(`ALTER TABLE ${credenciamentoTermoTableName} ADD COLUMN IF NOT EXISTS valor_contrato numeric(14, 2)`)
  await pool.query(`ALTER TABLE ${credenciamentoTermoTableName} ADD COLUMN IF NOT EXISTS objeto text`)
  await pool.query(`ALTER TABLE ${credenciamentoTermoTableName} ADD COLUMN IF NOT EXISTS data_publicacao date`)
  await pool.query(`ALTER TABLE ${credenciamentoTermoTableName} ADD COLUMN IF NOT EXISTS info_sei varchar(100)`)
  await pool.query(`ALTER TABLE ${credenciamentoTermoTableName} ADD COLUMN IF NOT EXISTS valor_contrato_atualizado numeric(14, 2)`)
  await pool.query(`ALTER TABLE ${credenciamentoTermoTableName} ADD COLUMN IF NOT EXISTS vencimento_geral date`)
  await pool.query(`ALTER TABLE ${credenciamentoTermoTableName} ADD COLUMN IF NOT EXISTS mes_renovacao varchar(50)`)
  await pool.query(`ALTER TABLE ${credenciamentoTermoTableName} ADD COLUMN IF NOT EXISTS tp_optante varchar(20)`)
  await pool.query(`ALTER TABLE ${credenciamentoTermoTableName} DROP COLUMN IF EXISTS comparecimento_ext`)
  await pool.query(`ALTER TABLE ${credenciamentoTermoTableName} ADD COLUMN IF NOT EXISTS data_inclusao timestamp without time zone`)
  await pool.query(`ALTER TABLE ${credenciamentoTermoTableName} ADD COLUMN IF NOT EXISTS data_modificacao timestamp without time zone`)
  await pool.query(`ALTER TABLE ${credenciamentoTermoTableName} ALTER COLUMN codigo SET DEFAULT nextval('${credenciamentoTermoCodigoSequenceName}')`)
  await pool.query(`ALTER TABLE ${credenciamentoTermoTableName} ALTER COLUMN check_aditivo SET DEFAULT 0`)
  await pool.query(`ALTER TABLE ${credenciamentoTermoTableName} ALTER COLUMN data_inclusao SET DEFAULT NOW()`)
  await pool.query(`ALTER TABLE ${credenciamentoTermoTableName} ALTER COLUMN data_modificacao SET DEFAULT NOW()`)
  await pool.query(`ALTER TABLE ${credenciamentoTermoTableName} ALTER COLUMN check_aditivo SET NOT NULL`)
  await pool.query(`ALTER TABLE ${credenciamentoTermoTableName} ALTER COLUMN aditivo SET NOT NULL`)
  await pool.query(`ALTER SEQUENCE ${credenciamentoTermoCodigoSequenceName} OWNED BY ${credenciamentoTermoTableName}.codigo`)
  await pool.query(`ALTER TABLE ${credenciamentoTermoTableName} DROP COLUMN IF EXISTS folhas`)
  await pool.query(`CREATE UNIQUE INDEX IF NOT EXISTS termo_codigo_xml_aditivo_uk ON ${credenciamentoTermoTableName} (codigo_xml, aditivo)`)
  await pool.query(`
    CREATE TABLE IF NOT EXISTS ${credenciamentoTermoImportRecusaTableName} (
      id bigserial PRIMARY KEY,
      arquivo_xml varchar(255) NOT NULL,
      linha_xml integer NOT NULL,
      codigo_xml varchar(50),
      credenciado_xml varchar(255),
      aditivo_xml varchar(20),
      motivo_recusa text NOT NULL,
      data_importacao timestamp without time zone NOT NULL DEFAULT NOW()
    )
  `)
  await pool.query(`
    UPDATE credenciada
    SET placa = COALESCE(NULLIF(BTRIM(placa), ''), LEFT(CONCAT('CRED-', codigo::text), 20)),
        empresa = COALESCE(NULLIF(BTRIM(empresa), ''), COALESCE(NULLIF(BTRIM(credenciado), ''), CONCAT('CREDENCIADA ', codigo::text))),
        condutor = COALESCE(NULLIF(BTRIM(condutor), ''), COALESCE(NULLIF(BTRIM(representante), ''), NULLIF(BTRIM(empresa), ''), CONCAT('CREDENCIADA ', codigo::text))),
        tipo_pessoa = COALESCE(NULLIF(BTRIM(tipo_pessoa), ''), CASE WHEN LENGTH(REGEXP_REPLACE(COALESCE(cnpj_cpf, ''), '\\D', '', 'g')) = 14 THEN 'JURIDICA' ELSE 'FISICA' END),
        credenciado = COALESCE(NULLIF(BTRIM(credenciado), ''), NULLIF(BTRIM(empresa), '')),
        cnpj_cpf = COALESCE(NULLIF(BTRIM(cnpj_cpf), ''), NULL),
        data_inclusao = COALESCE(data_inclusao, NOW()),
        data_modificacao = COALESCE(data_modificacao, COALESCE(data_inclusao, NOW()))
    WHERE placa IS NULL
       OR empresa IS NULL
       OR condutor IS NULL
       OR tipo_pessoa IS NULL
       OR data_inclusao IS NULL
       OR data_modificacao IS NULL
  `)
  await pool.query('ALTER TABLE credenciada ALTER COLUMN placa SET NOT NULL')
  await pool.query('ALTER TABLE credenciada ALTER COLUMN empresa SET NOT NULL')
  await pool.query('ALTER TABLE credenciada ALTER COLUMN condutor SET NOT NULL')
  await pool.query('ALTER TABLE credenciada ALTER COLUMN tipo_pessoa SET NOT NULL')
  await pool.query(`
    UPDATE credenciada
    SET data_inclusao = COALESCE(data_inclusao, NOW()),
        data_modificacao = COALESCE(data_modificacao, COALESCE(data_inclusao, NOW()))
    WHERE data_inclusao IS NULL OR data_modificacao IS NULL
  `)
  await pool.query('SELECT setval(\'credenciada_codigo_seq\', GREATEST(COALESCE((SELECT MAX(codigo) FROM credenciada), 0), 1), true)')
  await pool.query('CREATE UNIQUE INDEX IF NOT EXISTS credenciada_codigo_unique_idx ON credenciada (codigo)')
  await pool.query('CREATE INDEX IF NOT EXISTS credenciada_import_recusa_data_idx ON credenciada_import_recusa (data_importacao DESC)')
  await pool.query('CREATE INDEX IF NOT EXISTS credenciada_import_recusa_arquivo_idx ON credenciada_import_recusa (arquivo_xml)')
  await pool.query('ALTER TABLE credenciada DROP COLUMN IF EXISTS logradouro')
  await pool.query('ALTER TABLE credenciada DROP COLUMN IF EXISTS bairro')
  await pool.query('ALTER TABLE credenciada DROP COLUMN IF EXISTS municipio')
  await pool.query(`ALTER TABLE ${credenciamentoTermoTableName} DROP COLUMN IF EXISTS logradouro`)
  await pool.query(`ALTER TABLE ${credenciamentoTermoTableName} DROP COLUMN IF EXISTS bairro`)
  await pool.query(`ALTER TABLE ${credenciamentoTermoTableName} DROP COLUMN IF EXISTS municipio`)
  await pool.query('ALTER TABLE credenciada ADD COLUMN IF NOT EXISTS numero VARCHAR(30)')
  await pool.query('ALTER TABLE credenciada ADD COLUMN IF NOT EXISTS complemento VARCHAR(30)')
  await pool.query(`
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'credenciada_cep_fkey'
      ) THEN
        ALTER TABLE credenciada ADD CONSTRAINT credenciada_cep_fkey
          FOREIGN KEY (cep) REFERENCES ceps(cep) ON DELETE SET NULL ON UPDATE CASCADE;
      END IF;
    END $$
  `)
  await pool.query(`
    DO $$
    BEGIN
      IF to_regclass('public.${ordemServicoTableName}') IS NULL AND to_regclass('public.${legacyCredenciamentoOsTableName}') IS NOT NULL THEN
        ALTER TABLE ${legacyCredenciamentoOsTableName} RENAME TO ${ordemServicoTableName};
      END IF;
    END $$
  `)
  await pool.query(`
    DO $$
    BEGIN
      IF to_regclass('public.${ordemServicoImportRecusaTableName}') IS NULL AND to_regclass('public.${legacyCredenciamentoOsImportRecusaTableName}') IS NOT NULL THEN
        ALTER TABLE ${legacyCredenciamentoOsImportRecusaTableName} RENAME TO ${ordemServicoImportRecusaTableName};
      END IF;
    END $$
  `)
  await pool.query(`
    DO $$
    BEGIN
      IF to_regclass('public.${ordemServicoCodigoSequenceName}') IS NULL AND to_regclass('public.${legacyCredenciamentoOsCodigoSequenceName}') IS NOT NULL THEN
        ALTER SEQUENCE ${legacyCredenciamentoOsCodigoSequenceName} RENAME TO ${ordemServicoCodigoSequenceName};
      END IF;
    END $$
  `)
  await pool.query(`CREATE SEQUENCE IF NOT EXISTS ${ordemServicoCodigoSequenceName} START WITH 1 INCREMENT BY 1`)
  await pool.query(`
    CREATE TABLE IF NOT EXISTS ${ordemServicoTableName} (
      codigo integer PRIMARY KEY DEFAULT nextval('${ordemServicoCodigoSequenceName}'),
      codigo_access varchar(50),
      termo_adesao varchar(255),
      num_os varchar(10),
      revisao varchar(30),
      os_concat varchar(295),
      vigencia_os date,
      credenciada_codigo integer NOT NULL,
      credenciado varchar(255) NOT NULL,
      cnpj_cpf varchar(18) NOT NULL,
      dre_codigo varchar(30) NOT NULL,
      dre_descricao varchar(255) NOT NULL,
      modalidade_codigo integer,
      modalidade_descricao varchar(255),
      cpf_condutor varchar(14) NOT NULL,
      condutor varchar(255) NOT NULL,
      data_admissao_condutor date,
      cpf_preposto varchar(14),
      preposto_condutor varchar(255),
      preposto_inicio date,
      preposto_dias integer,
      crm varchar(20) NOT NULL,
      veiculo_placas varchar(20),
      cpf_monitor varchar(14),
      monitor varchar(255),
      data_admissao_monitor date,
      situacao varchar(20) NOT NULL,
      tipo_troca_codigo integer,
      tipo_troca_descricao varchar(255),
      conexao varchar(50),
      data_encerramento date,
      anotacao text,
      uniao_termos varchar(255),
      data_inclusao timestamp without time zone NOT NULL DEFAULT NOW(),
      data_modificacao timestamp without time zone NOT NULL DEFAULT NOW()
    )
  `)
  await pool.query(`ALTER SEQUENCE ${ordemServicoCodigoSequenceName} OWNED BY ${ordemServicoTableName}.codigo`)
  await pool.query(`ALTER TABLE ${ordemServicoTableName} ALTER COLUMN codigo SET DEFAULT nextval('${ordemServicoCodigoSequenceName}')`)
  await pool.query(`ALTER TABLE ${ordemServicoTableName} ADD COLUMN IF NOT EXISTS codigo_access varchar(50)`)
  await pool.query(`ALTER TABLE ${ordemServicoTableName} ADD COLUMN IF NOT EXISTS termo_adesao varchar(255)`)
  await pool.query(`ALTER TABLE ${ordemServicoTableName} ADD COLUMN IF NOT EXISTS num_os varchar(10)`)
  await pool.query(`ALTER TABLE ${ordemServicoTableName} ADD COLUMN IF NOT EXISTS revisao varchar(30)`)
  await pool.query(`ALTER TABLE ${ordemServicoTableName} ADD COLUMN IF NOT EXISTS os_concat varchar(295)`)
  await pool.query(`ALTER TABLE ${ordemServicoTableName} ADD COLUMN IF NOT EXISTS vigencia_os date`)
  await pool.query(`ALTER TABLE ${ordemServicoTableName} ADD COLUMN IF NOT EXISTS credenciada_codigo integer`)
  await pool.query(`ALTER TABLE ${ordemServicoTableName} ADD COLUMN IF NOT EXISTS credenciado varchar(255)`)
  await pool.query(`ALTER TABLE ${ordemServicoTableName} ADD COLUMN IF NOT EXISTS cnpj_cpf varchar(18)`)
  await pool.query(`ALTER TABLE ${ordemServicoTableName} ADD COLUMN IF NOT EXISTS dre_codigo varchar(30)`)
  await pool.query(`ALTER TABLE ${ordemServicoTableName} ADD COLUMN IF NOT EXISTS dre_descricao varchar(255)`)
  await pool.query(`ALTER TABLE ${ordemServicoTableName} ADD COLUMN IF NOT EXISTS modalidade_codigo integer`)
  await pool.query(`ALTER TABLE ${ordemServicoTableName} ADD COLUMN IF NOT EXISTS modalidade_descricao varchar(255)`)
  await pool.query(`ALTER TABLE ${ordemServicoTableName} ADD COLUMN IF NOT EXISTS cpf_condutor varchar(14)`)
  await pool.query(`ALTER TABLE ${ordemServicoTableName} ADD COLUMN IF NOT EXISTS condutor varchar(255)`)
  await pool.query(`ALTER TABLE ${ordemServicoTableName} ADD COLUMN IF NOT EXISTS data_admissao_condutor date`)
  await pool.query(`ALTER TABLE ${ordemServicoTableName} ADD COLUMN IF NOT EXISTS cpf_preposto varchar(14)`)
  await pool.query(`ALTER TABLE ${ordemServicoTableName} ADD COLUMN IF NOT EXISTS preposto_condutor varchar(255)`)
  await pool.query(`ALTER TABLE ${ordemServicoTableName} ADD COLUMN IF NOT EXISTS preposto_inicio date`)
  await pool.query(`ALTER TABLE ${ordemServicoTableName} ADD COLUMN IF NOT EXISTS preposto_dias integer`)
  await pool.query(`ALTER TABLE ${ordemServicoTableName} ADD COLUMN IF NOT EXISTS crm varchar(20)`)
  await pool.query(`ALTER TABLE ${ordemServicoTableName} ADD COLUMN IF NOT EXISTS veiculo_placas varchar(20)`)
  await pool.query(`ALTER TABLE ${ordemServicoTableName} ADD COLUMN IF NOT EXISTS cpf_monitor varchar(14)`)
  await pool.query(`ALTER TABLE ${ordemServicoTableName} ADD COLUMN IF NOT EXISTS monitor varchar(255)`)
  await pool.query(`ALTER TABLE ${ordemServicoTableName} ADD COLUMN IF NOT EXISTS data_admissao_monitor date`)
  await pool.query(`ALTER TABLE ${ordemServicoTableName} ADD COLUMN IF NOT EXISTS situacao varchar(20)`)
  await pool.query(`ALTER TABLE ${ordemServicoTableName} ADD COLUMN IF NOT EXISTS tipo_troca_codigo integer`)
  await pool.query(`ALTER TABLE ${ordemServicoTableName} ADD COLUMN IF NOT EXISTS tipo_troca_descricao varchar(255)`)
  await pool.query(`ALTER TABLE ${ordemServicoTableName} ADD COLUMN IF NOT EXISTS conexao varchar(50)`)
  await pool.query(`ALTER TABLE ${ordemServicoTableName} ADD COLUMN IF NOT EXISTS data_encerramento date`)
  await pool.query(`ALTER TABLE ${ordemServicoTableName} ADD COLUMN IF NOT EXISTS anotacao text`)
  await pool.query(`ALTER TABLE ${ordemServicoTableName} ADD COLUMN IF NOT EXISTS uniao_termos varchar(255)`)
  await pool.query(`ALTER TABLE ${ordemServicoTableName} ADD COLUMN IF NOT EXISTS data_inclusao timestamp without time zone`)
  await pool.query(`ALTER TABLE ${ordemServicoTableName} ADD COLUMN IF NOT EXISTS data_modificacao timestamp without time zone`)
  await pool.query(`ALTER TABLE ${ordemServicoTableName} ALTER COLUMN data_inclusao SET DEFAULT NOW()`)
  await pool.query(`ALTER TABLE ${ordemServicoTableName} ALTER COLUMN data_modificacao SET DEFAULT NOW()`)
  await pool.query(`ALTER TABLE ${ordemServicoTableName} DROP COLUMN IF EXISTS os_origem`)
  await pool.query(`
    CREATE TABLE IF NOT EXISTS ${vinculoCondutorTableName} (
      id bigserial PRIMARY KEY,
      termo_adesao varchar(255),
      num_os varchar(10),
      revisao varchar(30),
      credenciada_codigo integer NOT NULL,
      data_os date,
      data_admissao_condutor date,
      condutor_codigo integer,
      data_inclusao timestamp without time zone NOT NULL DEFAULT NOW()
    )
  `)
  await pool.query(`ALTER TABLE ${vinculoCondutorTableName} ADD COLUMN IF NOT EXISTS id bigserial`)
  await pool.query(`ALTER TABLE ${vinculoCondutorTableName} ADD COLUMN IF NOT EXISTS termo_adesao varchar(255)`)
  await pool.query(`ALTER TABLE ${vinculoCondutorTableName} ADD COLUMN IF NOT EXISTS num_os varchar(10)`)
  await pool.query(`ALTER TABLE ${vinculoCondutorTableName} ADD COLUMN IF NOT EXISTS revisao varchar(30)`)
  await pool.query(`ALTER TABLE ${vinculoCondutorTableName} ADD COLUMN IF NOT EXISTS credenciada_codigo integer`)
  await pool.query(`ALTER TABLE ${vinculoCondutorTableName} ADD COLUMN IF NOT EXISTS data_os date`)
  await pool.query(`ALTER TABLE ${vinculoCondutorTableName} ADD COLUMN IF NOT EXISTS data_admissao_condutor date`)
  await pool.query(`ALTER TABLE ${vinculoCondutorTableName} ADD COLUMN IF NOT EXISTS condutor_codigo integer`)
  await pool.query(`ALTER TABLE ${vinculoCondutorTableName} ADD COLUMN IF NOT EXISTS data_inclusao timestamp without time zone`)
  await pool.query(`ALTER TABLE ${vinculoCondutorTableName} ALTER COLUMN termo_adesao DROP NOT NULL`)
  await pool.query(`ALTER TABLE ${vinculoCondutorTableName} ALTER COLUMN num_os DROP NOT NULL`)
  await pool.query(`ALTER TABLE ${vinculoCondutorTableName} ALTER COLUMN revisao DROP NOT NULL`)
  await pool.query(`ALTER TABLE ${vinculoCondutorTableName} ALTER COLUMN data_inclusao SET DEFAULT NOW()`)
  await pool.query(`
    CREATE TABLE IF NOT EXISTS ${vinculoCondutorImportRecusaTableName} (
      id bigserial PRIMARY KEY,
      arquivo_xml varchar(255) NOT NULL,
      linha_xml integer NOT NULL,
      codigo_xml varchar(255),
      empregador_xml varchar(255),
      cpf_condutor_xml varchar(30),
      data_os_xml varchar(30),
      admissao_xml varchar(30),
      motivo_recusa text NOT NULL,
      data_importacao timestamp without time zone NOT NULL DEFAULT NOW()
    )
  `)
  await pool.query(`ALTER TABLE ${vinculoCondutorImportRecusaTableName} ADD COLUMN IF NOT EXISTS arquivo_xml varchar(255)`)
  await pool.query(`ALTER TABLE ${vinculoCondutorImportRecusaTableName} ADD COLUMN IF NOT EXISTS linha_xml integer`)
  await pool.query(`ALTER TABLE ${vinculoCondutorImportRecusaTableName} ADD COLUMN IF NOT EXISTS codigo_xml varchar(255)`)
  await pool.query(`ALTER TABLE ${vinculoCondutorImportRecusaTableName} ADD COLUMN IF NOT EXISTS empregador_xml varchar(255)`)
  await pool.query(`ALTER TABLE ${vinculoCondutorImportRecusaTableName} ADD COLUMN IF NOT EXISTS cpf_condutor_xml varchar(30)`)
  await pool.query(`ALTER TABLE ${vinculoCondutorImportRecusaTableName} ADD COLUMN IF NOT EXISTS data_os_xml varchar(30)`)
  await pool.query(`ALTER TABLE ${vinculoCondutorImportRecusaTableName} ADD COLUMN IF NOT EXISTS admissao_xml varchar(30)`)
  await pool.query(`ALTER TABLE ${vinculoCondutorImportRecusaTableName} ADD COLUMN IF NOT EXISTS motivo_recusa text`)
  await pool.query(`ALTER TABLE ${vinculoCondutorImportRecusaTableName} ADD COLUMN IF NOT EXISTS data_importacao timestamp without time zone`)
  await pool.query(`ALTER TABLE ${vinculoCondutorImportRecusaTableName} ALTER COLUMN data_importacao SET DEFAULT NOW()`)
  await pool.query(`CREATE UNIQUE INDEX IF NOT EXISTS vinculo_condutor_id_uk ON ${vinculoCondutorTableName} (id)`)
  await pool.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS vinculo_condutor_chave_unique_idx
    ON ${vinculoCondutorTableName} (
      UPPER(BTRIM(termo_adesao)),
      UPPER(BTRIM(num_os)),
      UPPER(BTRIM(revisao)),
      credenciada_codigo,
      condutor_codigo
    )
  `)
  await pool.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS vinculo_condutor_importado_uk
    ON ${vinculoCondutorTableName} (credenciada_codigo, condutor_codigo)
    WHERE COALESCE(BTRIM(termo_adesao), '') = ''
      AND COALESCE(BTRIM(num_os), '') = ''
      AND COALESCE(BTRIM(revisao), '') = ''
  `)
  await pool.query(`ALTER TABLE ${vinculoCondutorTableName} ADD COLUMN IF NOT EXISTS codigo_xml varchar(255)`)
  await pool.query(`CREATE UNIQUE INDEX IF NOT EXISTS vinculo_condutor_codigo_xml_uk ON ${vinculoCondutorTableName} (codigo_xml) WHERE codigo_xml IS NOT NULL AND BTRIM(codigo_xml) <> ''`)
  await pool.query(`DROP INDEX IF EXISTS vinculo_condutor_importado_uk`)
  await pool.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS vinculo_condutor_importado_uk
    ON ${vinculoCondutorTableName} (credenciada_codigo, condutor_codigo)
    WHERE COALESCE(BTRIM(termo_adesao), '') = ''
      AND COALESCE(BTRIM(num_os), '') = ''
      AND COALESCE(BTRIM(revisao), '') = ''
      AND codigo_xml IS NULL
  `)
  await pool.query(`
    CREATE TABLE IF NOT EXISTS ${vinculoMonitorTableName} (
      id bigserial PRIMARY KEY,
      termo_adesao varchar(255),
      num_os varchar(10),
      revisao varchar(30),
      credenciada_codigo integer NOT NULL,
      data_os date,
      data_admissao_monitor date,
      monitor_codigo integer,
      data_inclusao timestamp without time zone NOT NULL DEFAULT NOW()
    )
  `)
  await pool.query(`ALTER TABLE ${vinculoMonitorTableName} ADD COLUMN IF NOT EXISTS id bigserial`)
  await pool.query(`ALTER TABLE ${vinculoMonitorTableName} ADD COLUMN IF NOT EXISTS termo_adesao varchar(255)`)
  await pool.query(`ALTER TABLE ${vinculoMonitorTableName} ADD COLUMN IF NOT EXISTS num_os varchar(10)`)
  await pool.query(`ALTER TABLE ${vinculoMonitorTableName} ADD COLUMN IF NOT EXISTS revisao varchar(30)`)
  await pool.query(`ALTER TABLE ${vinculoMonitorTableName} ADD COLUMN IF NOT EXISTS credenciada_codigo integer`)
  await pool.query(`ALTER TABLE ${vinculoMonitorTableName} ADD COLUMN IF NOT EXISTS data_os date`)
  await pool.query(`ALTER TABLE ${vinculoMonitorTableName} ADD COLUMN IF NOT EXISTS data_admissao_monitor date`)
  await pool.query(`ALTER TABLE ${vinculoMonitorTableName} ADD COLUMN IF NOT EXISTS monitor_codigo integer`)
  await pool.query(`ALTER TABLE ${vinculoMonitorTableName} ADD COLUMN IF NOT EXISTS data_inclusao timestamp without time zone`)
  await pool.query(`ALTER TABLE ${vinculoMonitorTableName} ALTER COLUMN termo_adesao DROP NOT NULL`)
  await pool.query(`ALTER TABLE ${vinculoMonitorTableName} ALTER COLUMN num_os DROP NOT NULL`)
  await pool.query(`ALTER TABLE ${vinculoMonitorTableName} ALTER COLUMN revisao DROP NOT NULL`)
  await pool.query(`ALTER TABLE ${vinculoMonitorTableName} ALTER COLUMN data_inclusao SET DEFAULT NOW()`)
  await pool.query(`
    CREATE TABLE IF NOT EXISTS ${vinculoMonitorImportRecusaTableName} (
      id bigserial PRIMARY KEY,
      arquivo_xml varchar(255) NOT NULL,
      linha_xml integer NOT NULL,
      codigo_xml varchar(255),
      empregador_xml varchar(255),
      cpf_monitor_xml varchar(30),
      data_os_xml varchar(30),
      admissao_xml varchar(30),
      motivo_recusa text NOT NULL,
      data_importacao timestamp without time zone NOT NULL DEFAULT NOW()
    )
  `)
  await pool.query(`ALTER TABLE ${vinculoMonitorImportRecusaTableName} ADD COLUMN IF NOT EXISTS arquivo_xml varchar(255)`)
  await pool.query(`ALTER TABLE ${vinculoMonitorImportRecusaTableName} ADD COLUMN IF NOT EXISTS linha_xml integer`)
  await pool.query(`ALTER TABLE ${vinculoMonitorImportRecusaTableName} ADD COLUMN IF NOT EXISTS codigo_xml varchar(255)`)
  await pool.query(`ALTER TABLE ${vinculoMonitorImportRecusaTableName} ADD COLUMN IF NOT EXISTS empregador_xml varchar(255)`)
  await pool.query(`ALTER TABLE ${vinculoMonitorImportRecusaTableName} ADD COLUMN IF NOT EXISTS cpf_monitor_xml varchar(30)`)
  await pool.query(`ALTER TABLE ${vinculoMonitorImportRecusaTableName} ADD COLUMN IF NOT EXISTS data_os_xml varchar(30)`)
  await pool.query(`ALTER TABLE ${vinculoMonitorImportRecusaTableName} ADD COLUMN IF NOT EXISTS admissao_xml varchar(30)`)
  await pool.query(`ALTER TABLE ${vinculoMonitorImportRecusaTableName} ADD COLUMN IF NOT EXISTS motivo_recusa text`)
  await pool.query(`ALTER TABLE ${vinculoMonitorImportRecusaTableName} ADD COLUMN IF NOT EXISTS data_importacao timestamp without time zone`)
  await pool.query(`ALTER TABLE ${vinculoMonitorImportRecusaTableName} ALTER COLUMN data_importacao SET DEFAULT NOW()`)
  await pool.query(`CREATE UNIQUE INDEX IF NOT EXISTS vinculo_monitor_id_uk ON ${vinculoMonitorTableName} (id)`)
  await pool.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS vinculo_monitor_chave_unique_idx
    ON ${vinculoMonitorTableName} (
      UPPER(BTRIM(termo_adesao)),
      UPPER(BTRIM(num_os)),
      UPPER(BTRIM(revisao)),
      credenciada_codigo,
      monitor_codigo
    )
  `)
  await pool.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS vinculo_monitor_importado_uk
    ON ${vinculoMonitorTableName} (credenciada_codigo, monitor_codigo)
    WHERE COALESCE(BTRIM(termo_adesao), '') = ''
      AND COALESCE(BTRIM(num_os), '') = ''
      AND COALESCE(BTRIM(revisao), '') = ''
  `)
  await pool.query(`ALTER TABLE ${vinculoMonitorTableName} ADD COLUMN IF NOT EXISTS codigo_xml varchar(255)`)
  await pool.query(`CREATE UNIQUE INDEX IF NOT EXISTS vinculo_monitor_codigo_xml_uk ON ${vinculoMonitorTableName} (codigo_xml) WHERE codigo_xml IS NOT NULL AND BTRIM(codigo_xml) <> ''`)
  await pool.query(`DROP INDEX IF EXISTS vinculo_monitor_importado_uk`)
  await pool.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS vinculo_monitor_importado_uk
    ON ${vinculoMonitorTableName} (credenciada_codigo, monitor_codigo)
    WHERE COALESCE(BTRIM(termo_adesao), '') = ''
      AND COALESCE(BTRIM(num_os), '') = ''
      AND COALESCE(BTRIM(revisao), '') = ''
      AND codigo_xml IS NULL
  `)
  await pool.query(`ALTER TABLE ${ordemServicoTableName} ADD COLUMN IF NOT EXISTS termo_codigo integer`)
  await pool.query(`
    UPDATE ${ordemServicoTableName} os
    SET termo_codigo = t.codigo
    FROM (
      SELECT DISTINCT ON (UPPER(BTRIM(COALESCE(termo_adesao, '')))) codigo, UPPER(BTRIM(COALESCE(termo_adesao, ''))) AS termo_key
      FROM ${credenciamentoTermoTableName}
      ORDER BY UPPER(BTRIM(COALESCE(termo_adesao, ''))), aditivo ASC
    ) t
    WHERE UPPER(BTRIM(COALESCE(os.termo_adesao, ''))) = t.termo_key
      AND os.termo_codigo IS NULL
  `)
  await pool.query(`ALTER TABLE ${ordemServicoTableName} DROP COLUMN IF EXISTS credenciada_codigo`)
  await pool.query(`ALTER TABLE ${ordemServicoTableName} DROP COLUMN IF EXISTS credenciado`)
  await pool.query(`ALTER TABLE ${ordemServicoTableName} DROP COLUMN IF EXISTS cnpj_cpf`)
  await pool.query(`ALTER TABLE ${credenciamentoTermoTableName} DROP COLUMN IF EXISTS credenciado`)
  await pool.query(`ALTER TABLE ${credenciamentoTermoTableName} DROP COLUMN IF EXISTS cnpj_cpf`)
  await pool.query(`ALTER TABLE ${credenciamentoTermoTableName} DROP COLUMN IF EXISTS representante`)
  await pool.query(`ALTER TABLE ${credenciamentoTermoTableName} DROP COLUMN IF EXISTS cpf_representante`)
  await pool.query(`ALTER TABLE ${credenciamentoTermoTableName} DROP COLUMN IF EXISTS rg_representante`)

  await pool.query(`
    CREATE TABLE IF NOT EXISTS ${cepTableName} (
      cep varchar(9) PRIMARY KEY,
      logradouro varchar(255),
      complemento varchar(255),
      bairro varchar(120),
      municipio varchar(120) NOT NULL,
      uf varchar(2) NOT NULL,
      ibge varchar(10),
      data_inclusao timestamp,
      data_modificacao timestamp
    )
  `)

  await pool.query(`
    CREATE TABLE IF NOT EXISTS ${cepImportRecusaTableName} (
      id bigserial PRIMARY KEY,
      arquivo_xml varchar(255) NOT NULL,
      linha_xml integer,
      cep_xml varchar(20),
      logradouro_xml varchar(255),
      municipio_xml varchar(120),
      uf_xml varchar(10),
      motivo_recusa text NOT NULL,
      data_importacao timestamp
    )
  `)

  // Compatibilidade: tabela ceps pode ter sido criada com 'cidade' em vez de 'municipio'
  await pool.query(`
    DO $$
    BEGIN
      IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='ceps' AND column_name='cidade') THEN
        ALTER TABLE ceps RENAME COLUMN cidade TO municipio;
      END IF;
    END $$
  `)
  await pool.query(`ALTER TABLE ${cepTableName} ADD COLUMN IF NOT EXISTS data_inclusao timestamp`)
  await pool.query(`ALTER TABLE ${cepTableName} ADD COLUMN IF NOT EXISTS data_modificacao timestamp`)
  await pool.query(`ALTER TABLE ${cepTableName} ADD COLUMN IF NOT EXISTS complemento varchar(255)`)
  await pool.query(`
    CREATE TABLE IF NOT EXISTS ${emissaoDocumentoParametroTableName} (
      data_referencia varchar(10) PRIMARY KEY,
      objeto text NOT NULL,
      objeto_licitacao text NOT NULL DEFAULT '',
      credenciante text NOT NULL DEFAULT '',
      titulo_aditivo text NOT NULL DEFAULT '',
      termo_smt text NOT NULL DEFAULT '',
      descricao_aditivo text NOT NULL DEFAULT '',
      corpo_aditivo text NOT NULL DEFAULT '',
      assinaturas_aditivo text NOT NULL DEFAULT '',
      descricao_contrato_pf text NOT NULL DEFAULT '',
      descricao_contrato_pj text NOT NULL DEFAULT '',
      corpo_contrato_pf text NOT NULL DEFAULT '',
      corpo_contrato_pj text NOT NULL DEFAULT '',
      link_modelo_relatorio_contrato_pf text NOT NULL DEFAULT '',
      link_modelo_relatorio_contrato_pj text NOT NULL DEFAULT '',
      texto_despacho text NOT NULL DEFAULT '',
      edital_chamamento_publico varchar(50) NOT NULL,
      obs_01_emissao text NOT NULL,
      obs_02_emissao text NOT NULL,
      rodape_emissao text NOT NULL,
      prefeitura_imagem text NOT NULL DEFAULT '',
      titulo_emissao text NOT NULL DEFAULT '',
      diretor_emissao text NOT NULL DEFAULT '',
      data_inclusao timestamp without time zone NOT NULL DEFAULT NOW(),
      data_modificacao timestamp without time zone NOT NULL DEFAULT NOW()
    )
  `)
  await pool.query(`ALTER TABLE ${emissaoDocumentoParametroTableName} ADD COLUMN IF NOT EXISTS data_referencia varchar(10)`)
  await pool.query(`ALTER TABLE ${emissaoDocumentoParametroTableName} ADD COLUMN IF NOT EXISTS objeto text`)
  await pool.query(`ALTER TABLE ${emissaoDocumentoParametroTableName} ADD COLUMN IF NOT EXISTS objeto_licitacao text NOT NULL DEFAULT ''`)
  await pool.query(`ALTER TABLE ${emissaoDocumentoParametroTableName} ADD COLUMN IF NOT EXISTS credenciante text NOT NULL DEFAULT ''`)
  await pool.query(`ALTER TABLE ${emissaoDocumentoParametroTableName} ADD COLUMN IF NOT EXISTS titulo_aditivo text NOT NULL DEFAULT ''`)
  await pool.query(`ALTER TABLE ${emissaoDocumentoParametroTableName} ADD COLUMN IF NOT EXISTS termo_smt text NOT NULL DEFAULT ''`)
  await pool.query(`ALTER TABLE ${emissaoDocumentoParametroTableName} ADD COLUMN IF NOT EXISTS descricao_aditivo text NOT NULL DEFAULT ''`)
  await pool.query(`ALTER TABLE ${emissaoDocumentoParametroTableName} ADD COLUMN IF NOT EXISTS corpo_aditivo text NOT NULL DEFAULT ''`)
  await pool.query(`ALTER TABLE ${emissaoDocumentoParametroTableName} ADD COLUMN IF NOT EXISTS assinaturas_aditivo text NOT NULL DEFAULT ''`)
  await pool.query(`ALTER TABLE ${emissaoDocumentoParametroTableName} ADD COLUMN IF NOT EXISTS descricao_contrato_pf text NOT NULL DEFAULT ''`)
  await pool.query(`ALTER TABLE ${emissaoDocumentoParametroTableName} ADD COLUMN IF NOT EXISTS descricao_contrato_pj text NOT NULL DEFAULT ''`)
  await pool.query(`ALTER TABLE ${emissaoDocumentoParametroTableName} ADD COLUMN IF NOT EXISTS corpo_contrato_pf text NOT NULL DEFAULT ''`)
  await pool.query(`ALTER TABLE ${emissaoDocumentoParametroTableName} ADD COLUMN IF NOT EXISTS corpo_contrato_pj text NOT NULL DEFAULT ''`)
  await pool.query(`ALTER TABLE ${emissaoDocumentoParametroTableName} ADD COLUMN IF NOT EXISTS link_modelo_relatorio_contrato_pf text NOT NULL DEFAULT ''`)
  await pool.query(`ALTER TABLE ${emissaoDocumentoParametroTableName} ADD COLUMN IF NOT EXISTS link_modelo_relatorio_contrato_pj text NOT NULL DEFAULT ''`)
  await pool.query(`ALTER TABLE ${emissaoDocumentoParametroTableName} ADD COLUMN IF NOT EXISTS texto_despacho text NOT NULL DEFAULT ''`)
  await pool.query(`ALTER TABLE ${emissaoDocumentoParametroTableName} ADD COLUMN IF NOT EXISTS edital_chamamento_publico varchar(50)`)
  await pool.query(`ALTER TABLE ${emissaoDocumentoParametroTableName} ADD COLUMN IF NOT EXISTS obs_01_emissao text`)
  await pool.query(`ALTER TABLE ${emissaoDocumentoParametroTableName} ADD COLUMN IF NOT EXISTS obs_02_emissao text`)
  await pool.query(`ALTER TABLE ${emissaoDocumentoParametroTableName} ADD COLUMN IF NOT EXISTS rodape_emissao text`)
  await pool.query(`ALTER TABLE ${emissaoDocumentoParametroTableName} ADD COLUMN IF NOT EXISTS prefeitura_imagem text NOT NULL DEFAULT ''`)
  await pool.query(`ALTER TABLE ${emissaoDocumentoParametroTableName} ADD COLUMN IF NOT EXISTS titulo_emissao text NOT NULL DEFAULT ''`)
  await pool.query(`ALTER TABLE ${emissaoDocumentoParametroTableName} ADD COLUMN IF NOT EXISTS diretor_emissao text NOT NULL DEFAULT ''`)
  await pool.query(`ALTER TABLE ${emissaoDocumentoParametroTableName} ADD COLUMN IF NOT EXISTS data_inclusao timestamp without time zone`)
  await pool.query(`ALTER TABLE ${emissaoDocumentoParametroTableName} ADD COLUMN IF NOT EXISTS data_modificacao timestamp without time zone`)
  await pool.query(`ALTER TABLE ${emissaoDocumentoParametroTableName} ALTER COLUMN data_inclusao SET DEFAULT NOW()`)
  await pool.query(`ALTER TABLE ${emissaoDocumentoParametroTableName} ALTER COLUMN data_modificacao SET DEFAULT NOW()`)
  await pool.query(
    `UPDATE ${emissaoDocumentoParametroTableName}
     SET titulo_emissao = $1
     WHERE COALESCE(BTRIM(titulo_emissao), '') = ''`,
    [defaultEmissaoDocumentoTitulo],
  )
  await pool.query(
    `UPDATE ${emissaoDocumentoParametroTableName}
     SET diretor_emissao = $1
     WHERE COALESCE(BTRIM(diretor_emissao), '') = ''`,
    [defaultEmissaoDocumentoDiretor],
  )
  await pool.query(
    `UPDATE ${emissaoDocumentoParametroTableName}
     SET objeto_licitacao = $1
     WHERE COALESCE(BTRIM(objeto_licitacao), '') = ''`,
    [defaultEmissaoDocumentoObjetoLicitacao],
  )
  await pool.query(
    `UPDATE ${emissaoDocumentoParametroTableName}
     SET credenciante = $1
     WHERE COALESCE(BTRIM(credenciante), '') = ''`,
    [defaultEmissaoDocumentoCredenciante],
  )
  await pool.query(
    `UPDATE ${emissaoDocumentoParametroTableName}
     SET titulo_aditivo = $1
     WHERE COALESCE(BTRIM(titulo_aditivo), '') = ''`,
    [defaultEmissaoDocumentoTituloAditivo],
  )
  await pool.query(
    `UPDATE ${emissaoDocumentoParametroTableName}
     SET termo_smt = $1
     WHERE COALESCE(BTRIM(termo_smt), '') = ''`,
    [defaultEmissaoDocumentoTermoSmt],
  )
  await pool.query(
    `UPDATE ${emissaoDocumentoParametroTableName}
     SET descricao_aditivo = $1
     WHERE COALESCE(BTRIM(descricao_aditivo), '') = ''`,
    [defaultEmissaoDocumentoDescricaoAditivo],
  )
  await pool.query(
    `UPDATE ${emissaoDocumentoParametroTableName}
     SET corpo_aditivo = $1
     WHERE COALESCE(BTRIM(corpo_aditivo), '') = ''`,
    [defaultEmissaoDocumentoCorpoAditivo],
  )
  await pool.query(
    `UPDATE ${emissaoDocumentoParametroTableName}
     SET assinaturas_aditivo = $1
     WHERE COALESCE(BTRIM(assinaturas_aditivo), '') = ''`,
    [defaultEmissaoDocumentoAssinaturasAditivo],
  )
  await pool.query(
    `UPDATE ${emissaoDocumentoParametroTableName}
     SET texto_despacho = $1
     WHERE COALESCE(BTRIM(texto_despacho), '') = ''`,
    [defaultEmissaoDocumentoTextoDespacho],
  )
  await pool.query(
    `INSERT INTO ${emissaoDocumentoParametroTableName} (
       data_referencia,
       objeto,
       objeto_licitacao,
       credenciante,
       titulo_aditivo,
       termo_smt,
       descricao_aditivo,
       corpo_aditivo,
       assinaturas_aditivo,
      descricao_contrato_pf,
      descricao_contrato_pj,
      corpo_contrato_pf,
      corpo_contrato_pj,
      link_modelo_relatorio_contrato_pf,
      link_modelo_relatorio_contrato_pj,
       texto_despacho,
       edital_chamamento_publico,
       obs_01_emissao,
       obs_02_emissao,
       rodape_emissao,
       prefeitura_imagem,
       titulo_emissao,
       diretor_emissao,
       data_inclusao,
       data_modificacao
     )
    SELECT CAST($1 AS varchar(10)), CAST($2 AS text), CAST($3 AS text), CAST($4 AS text), CAST($5 AS text), CAST($6 AS text), CAST($7 AS text), CAST($8 AS text), CAST($9 AS text), CAST($10 AS text), CAST($11 AS text), CAST($12 AS text), CAST($13 AS text), CAST($14 AS text), CAST($15 AS text), CAST($16 AS text), CAST($17 AS varchar(50)), CAST($18 AS text), CAST($19 AS text), CAST($20 AS text), CAST($21 AS text), CAST($22 AS text), CAST($23 AS text), NOW(), NOW()
     WHERE NOT EXISTS (
       SELECT 1
       FROM ${emissaoDocumentoParametroTableName}
       WHERE BTRIM(data_referencia) = CAST($1 AS text)
     )`,
    [
      '01/01/2022',
      normalizeEmissaoDocumentoParamText('A prestacao de servicos de transporte escolar, nos termos do Edital de Chamamento Publico SMTT/DTP n 001/2022 e do TERMO de Adesao ao Credenciamento e seus respectivos aditivos, visando ao atendimento aos educandos devidamente matriculados na rede municipal de ensino.'),
      defaultEmissaoDocumentoObjetoLicitacao,
      defaultEmissaoDocumentoCredenciante,
      defaultEmissaoDocumentoTituloAditivo,
      defaultEmissaoDocumentoTermoSmt,
      defaultEmissaoDocumentoDescricaoAditivo,
      defaultEmissaoDocumentoCorpoAditivo,
      defaultEmissaoDocumentoAssinaturasAditivo,
      '',
      '',
      '',
      '',
      '',
      '',
      defaultEmissaoDocumentoTextoDespacho,
      normalizeEmissaoDocumentoParamText('001/2022', 50),
      normalizeEmissaoDocumentoParamText('Faz parte desta Ordem de Servico a relacao de alunos transportados (em posse da DRE) conforme Termo de Autorizacao e de Ciencia de Demanda de Transporte Escolar.'),
      normalizeEmissaoDocumentoParamText('Esta Ordem de Servico e emitida estritamente em virtude de:'),
      normalizeEmissaoDocumentoParamText('A SECRETARIA MUNICIPAL DE MOBILIDADE URBANA E TRANSPORTE, por meio do Departamento de Transportes Publicos, emite a presente Ordem de Servico, prevista no TERMO de Adesao ao Credenciamento a favor do CONTRATADO acima, que se obriga a prestar os servicos de transporte escolar, conforme seus termos e caracteristicas operacionais constantes na Ficha de Controle Operacional (FCO) pela respectiva Diretoria Regional de Ensino.'),
      '',
      defaultEmissaoDocumentoTitulo,
      defaultEmissaoDocumentoDiretor,
    ],
  )

  await syncCondutorVinculosFromOrdemServico(pool)
  await syncMonitorVinculosFromOrdemServico(pool)
  const ordemServicoHasOsColumnResult = await pool.query(
    `SELECT EXISTS (
       SELECT 1
       FROM information_schema.columns
       WHERE table_schema = 'public'
         AND table_name = $1
         AND column_name = 'os'
     ) AS has_os`,
    [ordemServicoTableName],
  )
  const ordemServicoHasOsColumn = Boolean(ordemServicoHasOsColumnResult.rows[0]?.has_os)

  if (ordemServicoHasOsColumn) {
    await pool.query(`
      UPDATE ${ordemServicoTableName}
      SET revisao = COALESCE(
        NULLIF(BTRIM(revisao), ''),
        NULLIF(SUBSTRING(UPPER(BTRIM(os)) FROM '([A-Z]+)$'), ''),
        $1
      )
      WHERE COALESCE(BTRIM(revisao), '') = ''
    `, [ordemServicoSemRevisaoLabel])
    await pool.query(`
      UPDATE ${ordemServicoTableName}
      SET num_os = NULLIF(SUBSTRING(UPPER(BTRIM(os)) FROM '-([0-9]+)[A-Z]*$'), '')
      WHERE COALESCE(BTRIM(os), '') <> ''
        AND COALESCE(BTRIM(num_os), '') <> COALESCE(SUBSTRING(UPPER(BTRIM(os)) FROM '-([0-9]+)[A-Z]*$'), '')
    `)
  } else {
    await pool.query(
      `UPDATE ${ordemServicoTableName}
       SET revisao = $1
       WHERE COALESCE(BTRIM(revisao), '') = ''`,
      [ordemServicoSemRevisaoLabel],
    )
  }

  await pool.query(`ALTER TABLE ${ordemServicoTableName} DROP COLUMN IF EXISTS os`)
  await pool.query(`
    UPDATE ${ordemServicoTableName}
    SET codigo_access = COALESCE(NULLIF(BTRIM(codigo_access), ''), codigo::text)
    WHERE COALESCE(BTRIM(codigo_access), '') <> COALESCE(NULLIF(BTRIM(codigo_access), ''), codigo::text)
  `)
  await pool.query(`
    UPDATE ${ordemServicoTableName}
    SET termo_adesao = CASE
      WHEN LENGTH(REGEXP_REPLACE(COALESCE(termo_adesao, ''), '\\D', '', 'g')) <= 4 THEN REGEXP_REPLACE(COALESCE(termo_adesao, ''), '\\D', '', 'g')
      WHEN LENGTH(REGEXP_REPLACE(COALESCE(termo_adesao, ''), '\\D', '', 'g')) >= 5 THEN CONCAT(
        LEFT(REGEXP_REPLACE(COALESCE(termo_adesao, ''), '\\D', '', 'g'), 4),
        '/',
        SUBSTRING(LEFT(REGEXP_REPLACE(COALESCE(termo_adesao, ''), '\\D', '', 'g'), 11) FROM 5)
      )
      ELSE COALESCE(BTRIM(termo_adesao), '')
    END
    WHERE COALESCE(BTRIM(termo_adesao), '') <> CASE
      WHEN LENGTH(REGEXP_REPLACE(COALESCE(termo_adesao, ''), '\\D', '', 'g')) <= 4 THEN REGEXP_REPLACE(COALESCE(termo_adesao, ''), '\\D', '', 'g')
      WHEN LENGTH(REGEXP_REPLACE(COALESCE(termo_adesao, ''), '\\D', '', 'g')) >= 5 THEN CONCAT(
        LEFT(REGEXP_REPLACE(COALESCE(termo_adesao, ''), '\\D', '', 'g'), 4),
        '/',
        SUBSTRING(LEFT(REGEXP_REPLACE(COALESCE(termo_adesao, ''), '\\D', '', 'g'), 11) FROM 5)
      )
      ELSE COALESCE(BTRIM(termo_adesao), '')
    END
  `)
  await pool.query(
    `UPDATE ${ordemServicoTableName}
     SET os_concat = CONCAT(
       COALESCE(BTRIM(termo_adesao), ''),
       '-',
       COALESCE(BTRIM(num_os), ''),
       COALESCE(NULLIF(BTRIM(revisao), ''), $1)
     )
     WHERE COALESCE(BTRIM(os_concat), '') <> CONCAT(
       COALESCE(BTRIM(termo_adesao), ''),
       '-',
       COALESCE(BTRIM(num_os), ''),
       COALESCE(NULLIF(BTRIM(revisao), ''), $1)
     )`,
    [ordemServicoSemRevisaoLabel],
  )
  await pool.query(`
    UPDATE ${ordemServicoTableName}
    SET data_inclusao = COALESCE(data_inclusao, NOW()),
        data_modificacao = COALESCE(data_modificacao, COALESCE(data_inclusao, NOW()))
    WHERE data_inclusao IS NULL OR data_modificacao IS NULL
  `)
  await pool.query(`ALTER TABLE ${ordemServicoTableName} ALTER COLUMN dre_codigo SET NOT NULL`)
  await pool.query(`ALTER TABLE ${ordemServicoTableName} ALTER COLUMN dre_descricao SET NOT NULL`)
  await pool.query(`ALTER TABLE ${ordemServicoTableName} ALTER COLUMN cpf_condutor SET NOT NULL`)
  await pool.query(`ALTER TABLE ${ordemServicoTableName} ALTER COLUMN condutor SET NOT NULL`)
  await pool.query(`ALTER TABLE ${ordemServicoTableName} ALTER COLUMN crm SET NOT NULL`)
  await pool.query(`ALTER TABLE ${ordemServicoTableName} ALTER COLUMN situacao SET NOT NULL`)
  await pool.query(`SELECT setval('${ordemServicoCodigoSequenceName}', GREATEST(COALESCE((SELECT MAX(codigo) FROM ${ordemServicoTableName}), 0), 1), true)`)
  await pool.query(`CREATE UNIQUE INDEX IF NOT EXISTS ordem_servico_codigo_unique_idx ON ${ordemServicoTableName} (codigo)`)
  await pool.query(`DROP INDEX IF EXISTS ordem_servico_os_unique_idx`)
  await pool.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS ordem_servico_chave_composta_unique_idx
      ON ${ordemServicoTableName} (
        UPPER(BTRIM(COALESCE(termo_adesao, ''))),
        UPPER(BTRIM(COALESCE(num_os, ''))),
        UPPER(BTRIM(COALESCE(revisao, '')))
      )
  `)
  await pool.query(`DROP INDEX IF EXISTS ordem_servico_credenciado_idx`)
  await pool.query(`CREATE INDEX IF NOT EXISTS ordem_servico_dre_idx ON ${ordemServicoTableName} (dre_codigo)`)
  await pool.query(`CREATE INDEX IF NOT EXISTS ordem_servico_modalidade_idx ON ${ordemServicoTableName} (modalidade_codigo)`)
  await pool.query(`CREATE INDEX IF NOT EXISTS ordem_servico_condutor_idx ON ${ordemServicoTableName} (cpf_condutor)`)
  await pool.query(`CREATE INDEX IF NOT EXISTS ordem_servico_monitor_idx ON ${ordemServicoTableName} (cpf_monitor)`)
  await pool.query(`CREATE INDEX IF NOT EXISTS ordem_servico_veiculo_idx ON ${ordemServicoTableName} (crm)`)
  await pool.query(`
    CREATE TABLE IF NOT EXISTS ${ordemServicoImportRecusaTableName} (
      id bigserial PRIMARY KEY,
      arquivo_xml varchar(255) NOT NULL,
      linha_xml integer NOT NULL,
      codigo_xml varchar(50),
      os_xml varchar(255),
      num_os_xml varchar(10),
      credenciado_xml varchar(255),
      dre_xml varchar(50),
      cpf_condutor_xml varchar(20),
      cpf_monitor_xml varchar(20),
      crm_xml varchar(20),
      motivo_recusa text NOT NULL,
      data_importacao timestamp without time zone NOT NULL DEFAULT NOW()
    )
  `)
  await pool.query(`ALTER TABLE ${ordemServicoImportRecusaTableName} ADD COLUMN IF NOT EXISTS num_os_xml varchar(10)`)
  await pool.query(`CREATE INDEX IF NOT EXISTS ordem_servico_import_recusa_data_idx ON ${ordemServicoImportRecusaTableName} (data_importacao DESC)`)
  await pool.query(`CREATE INDEX IF NOT EXISTS ordem_servico_import_recusa_arquivo_idx ON ${ordemServicoImportRecusaTableName} (arquivo_xml)`)
  const compactedDreCodes = await compactDreCodes()
  if (compactedDreCodes.updatedDreCount > 0 || compactedDreCodes.updatedOrdemServicoCount > 0) {
    console.log(`Codigos da DRE compactados: ${compactedDreCodes.updatedDreCount}; OrdemServico remapeada: ${compactedDreCodes.updatedOrdemServicoCount}`)
  }
  const updatedOrdemServicoModalidades = await backfillOrdemServicoModalidadesFromDre()
  if (updatedOrdemServicoModalidades > 0) {
    console.log(`Modalidades da OrdemServico atualizadas em lote: ${updatedOrdemServicoModalidades}`)
  }
  await pool.query('CREATE UNIQUE INDEX IF NOT EXISTS login_nome_unique_idx ON login (UPPER(BTRIM(nome)))')
  await pool.query('CREATE UNIQUE INDEX IF NOT EXISTS login_email_unique_idx ON login (LOWER(TRIM(email)))')
  await pool.query('CREATE UNIQUE INDEX IF NOT EXISTS login_dre_unique_idx ON login_dre (login_codigo, dre_codigo)')
}

const server = createServer(async (request, response) => {
  const requestUrl = new URL(request.url ?? '/', 'http://localhost')
  const pathname = requestUrl.pathname

  if (request.method === 'OPTIONS') {
    response.writeHead(204, {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Headers': 'Content-Type',
      'Access-Control-Allow-Methods': 'GET, POST, PUT, PATCH, DELETE, OPTIONS',
    })
    response.end()
    return
  }

  if (request.method === 'GET' && pathname === '/api/dre') {
    try {
      const search = normalizeRequestValue(requestUrl.searchParams.get('search') ?? '')
      const page = Math.max(Number(requestUrl.searchParams.get('page') ?? 1) || 1, 1)
      const pageSize = Math.min(Math.max(Number(requestUrl.searchParams.get('pageSize') ?? 5) || 5, 1), 50)
      const sortBy = normalizeRequestValue(requestUrl.searchParams.get('sortBy') ?? 'codigo')
      const sortDirection = normalizeRequestValue(requestUrl.searchParams.get('sortDirection') ?? 'asc').toLowerCase() === 'desc'
        ? 'DESC'
        : 'ASC'
      const offset = (page - 1) * pageSize
      const filters = []
      const values = []
      const orderByClause = sortBy === 'descricao'
        ? `BTRIM(CAST(descricao AS text)) ${sortDirection}, dre.codigo ASC`
        : `dre.codigo ${sortDirection}`

      if (search) {
        values.push(`%${search}%`)
        filters.push(`(
          CAST(codigo AS text) ILIKE $${values.length}
          OR COALESCE(BTRIM(sigla), '') ILIKE UPPER($${values.length})
          OR COALESCE(BTRIM(codigo_operacional), '') ILIKE UPPER($${values.length})
          OR BTRIM(CAST(descricao AS text)) ILIKE $${values.length}
        )`)
      }

      const whereClause = filters.length ? `WHERE ${filters.join(' AND ')}` : ''
      const countResult = await pool.query(
        `SELECT COUNT(*)::int AS total FROM dre ${whereClause}`,
        values,
      )

      values.push(pageSize)
      values.push(offset)
      const result = await pool.query(
        `SELECT ${dreSelectClause}
         FROM dre
         ${whereClause}
         ORDER BY ${orderByClause}
         LIMIT $${values.length - 1}
         OFFSET $${values.length}`,
        values,
      )
      const total = countResult.rows[0]?.total ?? 0

      sendJson(response, 200, {
        items: result.rows,
        total,
        page,
        pageSize,
        totalPages: Math.max(Math.ceil(total / pageSize), 1),
        sortBy: sortBy === 'descricao' ? 'descricao' : 'codigo',
        sortDirection: sortDirection.toLowerCase(),
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao consultar a tabela dre.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'GET' && pathname === '/api/modalidade') {
    try {
      const search = normalizeRequestValue(requestUrl.searchParams.get('search') ?? '')
      const page = Math.max(Number(requestUrl.searchParams.get('page') ?? 1) || 1, 1)
      const pageSize = Math.min(Math.max(Number(requestUrl.searchParams.get('pageSize') ?? 5) || 5, 1), 50)
      const sortBy = normalizeRequestValue(requestUrl.searchParams.get('sortBy') ?? 'codigo')
      const sortDirection = normalizeRequestValue(requestUrl.searchParams.get('sortDirection') ?? 'asc').toLowerCase() === 'desc'
        ? 'DESC'
        : 'ASC'
      const offset = (page - 1) * pageSize
      const filters = []
      const values = []
      const orderByClause = sortBy === 'descricao'
        ? `BTRIM(CAST(descricao AS text)) ${sortDirection}, CAST(codigo AS text) ASC`
        : `CAST(codigo AS text) ${sortDirection}`

      if (search) {
        values.push(`%${search}%`)
        filters.push(`(
          CAST(codigo AS text) ILIKE $${values.length}
          OR BTRIM(CAST(descricao AS text)) ILIKE $${values.length}
        )`)
      }

      const whereClause = filters.length ? `WHERE ${filters.join(' AND ')}` : ''
      const countResult = await pool.query(
        `SELECT COUNT(*)::int AS total FROM modalidade ${whereClause}`,
        values,
      )

      values.push(pageSize)
      values.push(offset)
      const result = await pool.query(
        `SELECT ${modalidadeSelectClause}
         FROM modalidade
         ${whereClause}
         ORDER BY ${orderByClause}
         LIMIT $${values.length - 1}
         OFFSET $${values.length}`,
        values,
      )
      const total = countResult.rows[0]?.total ?? 0

      sendJson(response, 200, {
        items: result.rows,
        total,
        page,
        pageSize,
        totalPages: Math.max(Math.ceil(total / pageSize), 1),
        sortBy: sortBy === 'descricao' ? 'descricao' : 'codigo',
        sortDirection: sortDirection.toLowerCase(),
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao consultar a tabela modalidade.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'GET' && pathname === '/api/marca-modelo') {
    try {
      const search = normalizeRequestValue(requestUrl.searchParams.get('search') ?? '')
      const page = Math.max(Number(requestUrl.searchParams.get('page') ?? 1) || 1, 1)
      const pageSize = Math.min(Math.max(Number(requestUrl.searchParams.get('pageSize') ?? 5) || 5, 1), 50)
      const sortBy = normalizeRequestValue(requestUrl.searchParams.get('sortBy') ?? 'codigo')
      const sortDirection = normalizeRequestValue(requestUrl.searchParams.get('sortDirection') ?? 'asc').toLowerCase() === 'desc'
        ? 'DESC'
        : 'ASC'
      const offset = (page - 1) * pageSize
      const filters = []
      const values = []
      const numericCodigoOrderClause = `
        CASE WHEN BTRIM(codigo) ~ '^[0-9]+$' THEN 0 ELSE 1 END ASC,
        CASE WHEN BTRIM(codigo) ~ '^[0-9]+$' THEN CAST(BTRIM(codigo) AS bigint) END ${sortDirection},
        BTRIM(codigo) ${sortDirection}
      `
      const orderByClause = sortBy === 'descricao'
        ? `BTRIM(descricao) ${sortDirection}, ${numericCodigoOrderClause}`
        : numericCodigoOrderClause

      if (search) {
        values.push(`%${search}%`)
        filters.push(`(
          CAST(codigo AS text) ILIKE $${values.length}
          OR BTRIM(descricao) ILIKE $${values.length}
        )`)
      }

      const whereClause = filters.length ? `WHERE ${filters.join(' AND ')}` : ''
      const countResult = await pool.query(
        `SELECT COUNT(*)::int AS total FROM marca_modelo ${whereClause}`,
        values,
      )

      values.push(pageSize)
      values.push(offset)
      const result = await pool.query(
        `SELECT CAST(codigo AS text) AS codigo, BTRIM(descricao) AS descricao
         FROM marca_modelo
         ${whereClause}
         ORDER BY ${orderByClause}
         LIMIT $${values.length - 1}
         OFFSET $${values.length}`,
        values,
      )
      const total = countResult.rows[0]?.total ?? 0

      sendJson(response, 200, {
        items: result.rows,
        total,
        page,
        pageSize,
        totalPages: Math.max(Math.ceil(total / pageSize), 1),
        sortBy: sortBy === 'descricao' ? 'descricao' : 'codigo',
        sortDirection: sortDirection.toLowerCase(),
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao consultar a tabela marca/modelo.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'GET' && pathname === '/api/troca') {
    try {
      const search = normalizeRequestValue(requestUrl.searchParams.get('search') ?? '')
      const page = Math.max(Number(requestUrl.searchParams.get('page') ?? 1) || 1, 1)
      const pageSize = Math.min(Math.max(Number(requestUrl.searchParams.get('pageSize') ?? 5) || 5, 1), 50)
      const sortBy = normalizeRequestValue(requestUrl.searchParams.get('sortBy') ?? 'codigo')
      const requestedSortDirection = normalizeRequestValue(requestUrl.searchParams.get('sortDirection') ?? 'asc').toLowerCase() === 'desc'
        ? 'DESC'
        : 'ASC'
      const normalizedSortBy = sortBy === 'lista' ? 'descricao' : sortBy
      const sortDirection = normalizedSortBy === 'descricao' ? requestedSortDirection : 'ASC'
      const offset = (page - 1) * pageSize
      const filters = []
      const values = []
      let orderByClause = `CAST(codigo AS integer) ${sortDirection}, CAST(controle AS integer) ASC`

      if (normalizedSortBy === 'controle') {
        orderByClause = `CAST(controle AS integer) ${sortDirection}, CAST(codigo AS integer) ASC`
      } else if (normalizedSortBy === 'descricao') {
        orderByClause = `BTRIM(lista) ${sortDirection}, CAST(codigo AS integer) ASC, CAST(controle AS integer) ASC`
      }

      if (search) {
        values.push(`%${search}%`)
        filters.push(`(
          CAST(codigo AS text) ILIKE $${values.length}
          OR CAST(controle AS text) ILIKE $${values.length}
          OR BTRIM(lista) ILIKE $${values.length}
        )`)
      }

      const whereClause = filters.length ? `WHERE ${filters.join(' AND ')}` : ''
      const countResult = await pool.query(
        `SELECT COUNT(*)::int AS total FROM tipo_troca ${whereClause}`,
        values,
      )

      values.push(pageSize)
      values.push(offset)
      const result = await pool.query(
        `SELECT ${trocaSelectClause}
         FROM tipo_troca
         ${whereClause}
         ORDER BY ${orderByClause}
         LIMIT $${values.length - 1}
         OFFSET $${values.length}`,
        values,
      )
      const total = countResult.rows[0]?.total ?? 0

      sendJson(response, 200, {
        items: result.rows,
        total,
        page,
        pageSize,
        totalPages: Math.max(Math.ceil(total / pageSize), 1),
        sortBy: normalizedSortBy === 'controle' || normalizedSortBy === 'descricao' ? normalizedSortBy : 'codigo',
        sortDirection: sortDirection.toLowerCase(),
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao consultar a tabela troca.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'GET' && pathname === ordemServicoCollectionPath) {
    try {
      const search = normalizeRequestValue(requestUrl.searchParams.get('search') ?? '')
      const page = Math.max(Number(requestUrl.searchParams.get('page') ?? 1) || 1, 1)
      const pageSize = Math.min(Math.max(Number(requestUrl.searchParams.get('pageSize') ?? 5) || 5, 1), 50)
      const sortBy = normalizeRequestValue(requestUrl.searchParams.get('sortBy') ?? 'chave').toLowerCase()
      const sortDirection = normalizeRequestValue(requestUrl.searchParams.get('sortDirection') ?? 'asc').toLowerCase() === 'desc'
        ? 'DESC'
        : 'ASC'
      const offset = (page - 1) * pageSize
      const values = []
      const filters = []
      const ordemServicoCredenciadoExpression = `COALESCE(BTRIM((SELECT cr.credenciado FROM credenciada cr WHERE cr.codigo = (SELECT credenciada_codigo FROM ${credenciamentoTermoTableName} WHERE codigo = ${ordemServicoTableName}.termo_codigo))), '')`
      let orderByClause = ordemServicoCompositeKeyOrderClause

      if (sortBy === 'num_os') {
        orderByClause = `UPPER(BTRIM(COALESCE(${ordemServicoTableName}.num_os, ''))) ${sortDirection}, UPPER(BTRIM(COALESCE(${ordemServicoTableName}.termo_adesao, ''))) ASC, UPPER(BTRIM(COALESCE(${ordemServicoTableName}.revisao, ''))) ASC, ${ordemServicoTableName}.codigo ASC`
      } else if (sortBy === 'credenciado') {
        orderByClause = `UPPER(${ordemServicoCredenciadoExpression}) ${sortDirection}, ${ordemServicoTableName}.codigo ASC`
      }

      if (search) {
        values.push(`%${search}%`)
        filters.push(`(
          CAST(codigo AS text) ILIKE $${values.length}
          OR COALESCE(BTRIM(termo_adesao), '') ILIKE UPPER($${values.length})
          OR COALESCE(BTRIM(num_os), '') ILIKE UPPER($${values.length})
          OR COALESCE(BTRIM(revisao), '') ILIKE UPPER($${values.length})
          OR COALESCE(BTRIM(os_concat), '') ILIKE UPPER($${values.length})
          OR ${ordemServicoCredenciadoExpression} ILIKE UPPER($${values.length})
          OR COALESCE(BTRIM(dre_codigo), '') ILIKE UPPER($${values.length})
          OR COALESCE(BTRIM(condutor), '') ILIKE UPPER($${values.length})
          OR COALESCE(BTRIM(monitor), '') ILIKE UPPER($${values.length})
          OR COALESCE(BTRIM(crm), '') ILIKE UPPER($${values.length})
        )`)
      }

      const whereClause = filters.length ? `WHERE ${filters.join(' AND ')}` : ''
      const countResult = await pool.query(
        `SELECT COUNT(*)::int AS total FROM ${ordemServicoTableName} ${whereClause}`,
        values,
      )

      values.push(pageSize)
      values.push(offset)
      const result = await pool.query(
        `SELECT
           ${ordemServicoSelectClause}
         FROM ${ordemServicoTableName}
         ${whereClause}
         ORDER BY ${orderByClause}
         LIMIT $${values.length - 1}
         OFFSET $${values.length}`,
        values,
      )
      const total = countResult.rows[0]?.total ?? 0

      sendJson(response, 200, {
        items: result.rows,
        total,
        page,
        pageSize,
        totalPages: Math.max(Math.ceil(total / pageSize), 1),
        sortBy: sortBy === 'num_os' || sortBy === 'credenciado' || sortBy === 'chave' ? sortBy : 'chave',
        sortDirection: sortDirection.toLowerCase(),
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao consultar OrdemServico.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'GET' && pathname === ordemServicoNextNumOsPath) {
    try {
      const termoAdesao = normalizeOrdemServicoTermoAdesao(requestUrl.searchParams.get('termoAdesao') ?? '')

      if (!termoAdesao) {
        sendJson(response, 400, { message: 'Termo e obrigatorio.' })
        return
      }

      const result = await pool.query(
        `SELECT
           COALESCE(
             MAX(
               CASE
                 WHEN NULLIF(REGEXP_REPLACE(COALESCE(num_os, ''), '\\D', '', 'g'), '') IS NULL THEN NULL
                 ELSE REGEXP_REPLACE(COALESCE(num_os, ''), '\\D', '', 'g')::bigint
               END
             ),
             0
           ) AS max_num_os,
           COALESCE(
             MAX(LENGTH(NULLIF(REGEXP_REPLACE(COALESCE(num_os, ''), '\\D', '', 'g'), ''))),
             0
           ) AS max_num_os_length
         FROM ${ordemServicoTableName}
         WHERE UPPER(BTRIM(COALESCE(termo_adesao, ''))) = UPPER($1)`,
        [termoAdesao],
      )

      const maxNumOs = Number(result.rows[0]?.max_num_os ?? 0)
      const maxNumOsLength = Number(result.rows[0]?.max_num_os_length ?? 0)
      const nextNumOs = String(maxNumOs + 1).padStart(Math.max(maxNumOsLength, 3), '0')
      const isNewTerm = maxNumOs === 0

      sendJson(response, 200, { termoAdesao, nextNumOs, isNewTerm })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao consultar o proximo Num OS.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'GET' && pathname === ordemServicoNextRevisaoPath) {
    try {
      const termoAdesao = normalizeOrdemServicoTermoAdesao(requestUrl.searchParams.get('termoAdesao') ?? '')
      const numOs = normalizeRequestValue(requestUrl.searchParams.get('numOs') ?? '').replace(/\D/g, '').slice(0, 10)

      if (!termoAdesao) {
        sendJson(response, 400, { message: 'Termo e obrigatorio.' })
        return
      }

      if (!numOs) {
        sendJson(response, 400, { message: 'Num OS e obrigatorio.' })
        return
      }

      const result = await pool.query(
        `SELECT
           codigo,
           COALESCE(NULLIF(BTRIM(revisao), ''), $3) AS revisao
         FROM ${ordemServicoTableName}
         WHERE UPPER(BTRIM(COALESCE(termo_adesao, ''))) = UPPER($1)
           AND UPPER(BTRIM(COALESCE(num_os, ''))) = UPPER($2)
         ORDER BY codigo DESC
         LIMIT 1`,
        [termoAdesao, numOs, ordemServicoSemRevisaoLabel],
      )

      if (result.rowCount === 0) {
        sendJson(response, 200, {
          termoAdesao,
          numOs,
          codigoBase: null,
          revisaoAtual: '',
          nextRevisao: ordemServicoSemRevisaoLabel,
        })
        return
      }

      const currentRevisao = normalizeRequestValue(result.rows[0]?.revisao).toUpperCase() || ordemServicoSemRevisaoLabel
      const nextRevisao = buildRevisionSequenceLabel(parseRevisionSequenceNumber(currentRevisao) + 1)

      sendJson(response, 200, {
        termoAdesao,
        numOs,
        codigoBase: result.rows[0]?.codigo ?? null,
        revisaoAtual: currentRevisao,
        nextRevisao,
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao consultar a proxima revisao.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'GET' && pathname === emissaoDocumentoParametroResolvePath) {
    try {
      const requestedDate = normalizeEmissaoDocumentoDateKey(requestUrl.searchParams.get('dataReferencia') ?? '') || buildCurrentEmissaoDocumentoDateKey()

      if (!isEmissaoDocumentoDateKeyValid(requestedDate)) {
        sendJson(response, 400, { message: 'Data de referencia invalida. Use dd/mm/yyyy ou yyyy-mm-dd.' })
        return
      }

      const item = await findEmissaoDocumentoParametroByDate(requestedDate)

      if (!item) {
        sendJson(response, 404, { message: 'Parametro de emissao nao encontrado para a data informada.' })
        return
      }

      sendJson(response, 200, {
        item,
        requestedDate,
        exactMatch: item.data_referencia === requestedDate,
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao consultar parametro de emissao.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'GET' && pathname === emissaoDocumentoParametroCollectionPath) {
    try {
      const search = normalizeRequestValue(requestUrl.searchParams.get('search') ?? '')
      const page = Math.max(Number(requestUrl.searchParams.get('page') ?? 1) || 1, 1)
      const pageSize = Math.min(Math.max(Number(requestUrl.searchParams.get('pageSize') ?? 10) || 10, 1), 50)
      const sortDirection = normalizeRequestValue(requestUrl.searchParams.get('sortDirection') ?? 'desc').toLowerCase() === 'asc'
        ? 'ASC'
        : 'DESC'
      const offset = (page - 1) * pageSize
      const values = []
      const filters = []

      if (search) {
        values.push(`%${search}%`)
        filters.push(`(
          COALESCE(BTRIM(data_referencia), '') ILIKE $${values.length}
          OR COALESCE(BTRIM(edital_chamamento_publico), '') ILIKE $${values.length}
          OR COALESCE(BTRIM(objeto), '') ILIKE $${values.length}
          OR COALESCE(BTRIM(obs_01_emissao), '') ILIKE $${values.length}
          OR COALESCE(BTRIM(obs_02_emissao), '') ILIKE $${values.length}
        )`)
      }

      const whereClause = filters.length ? `WHERE ${filters.join(' AND ')}` : ''
      const countResult = await pool.query(
        `SELECT COUNT(*)::int AS total FROM ${emissaoDocumentoParametroTableName} ${whereClause}`,
        values,
      )

      values.push(pageSize)
      values.push(offset)
      const result = await pool.query(
        `SELECT
           ${emissaoDocumentoParametroSelectClause}
         FROM ${emissaoDocumentoParametroTableName}
         ${whereClause}
         ORDER BY TO_DATE(BTRIM(data_referencia), 'DD/MM/YYYY') ${sortDirection}, BTRIM(data_referencia) ${sortDirection}
         LIMIT $${values.length - 1}
         OFFSET $${values.length}`,
        values,
      )
      const total = countResult.rows[0]?.total ?? 0

      sendJson(response, 200, {
        items: result.rows,
        total,
        page,
        pageSize,
        totalPages: Math.max(Math.ceil(total / pageSize), 1),
        sortBy: 'data_referencia',
        sortDirection: sortDirection.toLowerCase(),
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao consultar parametros de emissao.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'GET' && pathname === ordemServicoImportRejectionsPath) {
    try {
      const search = normalizeRequestValue(requestUrl.searchParams.get('search') ?? '')
      const page = Math.max(Number(requestUrl.searchParams.get('page') ?? 1) || 1, 1)
      const pageSize = Math.min(Math.max(Number(requestUrl.searchParams.get('pageSize') ?? 10) || 10, 1), 100)
      const offset = (page - 1) * pageSize
      const values = []
      const filters = []

      if (search) {
        values.push(`%${search}%`)
        filters.push(`(
          arquivo_xml ILIKE $${values.length}
          OR COALESCE(codigo_xml, '') ILIKE $${values.length}
          OR COALESCE(os_xml, '') ILIKE $${values.length}
          OR COALESCE(num_os_xml, '') ILIKE $${values.length}
          OR COALESCE(credenciado_xml, '') ILIKE $${values.length}
          OR COALESCE(dre_xml, '') ILIKE $${values.length}
          OR COALESCE(cpf_condutor_xml, '') ILIKE $${values.length}
          OR COALESCE(cpf_monitor_xml, '') ILIKE $${values.length}
          OR COALESCE(crm_xml, '') ILIKE $${values.length}
          OR motivo_recusa ILIKE $${values.length}
        )`)
      }

      const whereClause = filters.length ? `WHERE ${filters.join(' AND ')}` : ''
      const countResult = await pool.query(
        `SELECT COUNT(*)::int AS total FROM ${ordemServicoImportRecusaTableName} ${whereClause}`,
        values,
      )

      values.push(pageSize)
      values.push(offset)
      const result = await pool.query(
        `SELECT
           ${ordemServicoImportRecusaSelectClause}
         FROM ${ordemServicoImportRecusaTableName}
         ${whereClause}
         ORDER BY data_importacao DESC, linha_xml ASC, id DESC
         LIMIT $${values.length - 1}
         OFFSET $${values.length}`,
        values,
      )
      const total = countResult.rows[0]?.total ?? 0

      sendJson(response, 200, {
        items: result.rows,
        total,
        page,
        pageSize,
        totalPages: Math.max(Math.ceil(total / pageSize), 1),
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao consultar recusas de OrdemServico.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'GET' && pathname === '/api/credenciada/lookup') {
    try {
      const cnpjCpf = normalizeCnpjCpf(requestUrl.searchParams.get('cnpjCpf') ?? '')
      const credenciado = normalizeCredenciadaText(requestUrl.searchParams.get('credenciado') ?? '', 255)
      const item = cnpjCpf
        ? await findCredenciadaByCnpjCpf(cnpjCpf)
        : await findCredenciadaByName(credenciado)

      if (!item) {
        sendJson(response, 404, { message: 'Credenciado nao encontrado na tabela credenciada.' })
        return
      }

      sendJson(response, 200, { item })
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Erro ao localizar credenciado.'
      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'GET' && pathname === '/api/dre/lookup') {
    try {
      const codigo = normalizeRequestValue(requestUrl.searchParams.get('codigo') ?? '').toUpperCase()
      const item = await findDreByCodigo(codigo)

      if (!item) {
        sendJson(response, 404, { message: 'DRE nao encontrada.' })
        return
      }

      sendJson(response, 200, { item })
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Erro ao localizar DRE.'
      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'GET' && pathname === '/api/condutor/lookup') {
    try {
      const cpf = normalizeCpf(requestUrl.searchParams.get('cpf') ?? '')

      if (!cpf || !isCpfValid(cpf)) {
        sendJson(response, 400, { message: 'CPF do condutor deve conter 11 digitos.' })
        return
      }

      const item = await findCondutorByCpf(cpf)

      if (!item) {
        sendJson(response, 404, { message: 'Condutor nao encontrado.' })
        return
      }

      sendJson(response, 200, { item })
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Erro ao localizar condutor.'
      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'GET' && pathname === '/api/monitor/lookup') {
    try {
      const cpf = normalizeCpf(requestUrl.searchParams.get('cpf') ?? '')

      if (!cpf || !isCpfValid(cpf)) {
        sendJson(response, 400, { message: 'CPF do monitor deve conter 11 digitos.' })
        return
      }

      const item = await findMonitorByCpf(cpf)

      if (!item) {
        sendJson(response, 404, { message: 'Monitor nao encontrado.' })
        return
      }

      sendJson(response, 200, { item })
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Erro ao localizar monitor.'
      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'GET' && pathname === ordemServicoActiveCpfPath) {
    try {
      const cpf = normalizeCpf(requestUrl.searchParams.get('cpf') ?? '')
      const excludeCodigo = normalizeCondutorCodigo(requestUrl.searchParams.get('excludeCodigo'))

      if (!cpf || !isCpfValid(cpf)) {
        sendJson(response, 400, { message: 'CPF deve conter 11 digitos.' })
        return
      }

      const item = await findActiveOrdemServicoByCpf(cpf, { excludeCodigo })
      sendJson(response, 200, { active: Boolean(item), item })
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Erro ao validar CPF em OrdemServico ativa.'
      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'GET' && pathname === ordemServicoActivePlacaPath) {
    try {
      const placa = normalizeVehiclePlaca(requestUrl.searchParams.get('placa') ?? '')
      const excludeCodigo = normalizeCondutorCodigo(requestUrl.searchParams.get('excludeCodigo'))

      if (!placa) {
        sendJson(response, 400, { message: 'Placa do veiculo e obrigatoria.' })
        return
      }

      const item = await findActiveOrdemServicoByPlaca(placa, { excludeCodigo })
      sendJson(response, 200, { active: Boolean(item), item })
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Erro ao validar placa em OrdemServico ativa.'
      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'GET' && pathname === '/api/veiculo/lookup') {
    try {
      const crm = normalizeVehicleCrm(requestUrl.searchParams.get('crm') ?? '')

      if (!crm) {
        sendJson(response, 400, { message: 'CRM e obrigatorio.' })
        return
      }

      const item = await findVeiculoByCrm(crm)

      if (!item) {
        sendJson(response, 404, { message: 'Veiculo nao encontrado.' })
        return
      }

      sendJson(response, 200, { item })
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Erro ao localizar veiculo.'
      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'GET' && pathname === '/api/troca/lookup') {
    try {
      const codigo = normalizeRequestValue(requestUrl.searchParams.get('codigo') ?? '')
      const descricao = normalizeTrocaText(requestUrl.searchParams.get('descricao') ?? '', 255)
      const item = await findTrocaByCodigoOrDescricao({ codigo, descricao })

      if (!item) {
        sendJson(response, 404, { message: 'Tipo de troca nao encontrado.' })
        return
      }

      sendJson(response, 200, { item })
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Erro ao localizar tipo de troca.'
      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'GET' && pathname === '/api/seguradora') {
    try {
      const search = normalizeRequestValue(requestUrl.searchParams.get('search') ?? '')
      const page = Math.max(Number(requestUrl.searchParams.get('page') ?? 1) || 1, 1)
      const pageSize = Math.min(Math.max(Number(requestUrl.searchParams.get('pageSize') ?? 5) || 5, 1), 50)
      const sortBy = normalizeRequestValue(requestUrl.searchParams.get('sortBy') ?? 'codigo')
      const requestedSortDirection = normalizeRequestValue(requestUrl.searchParams.get('sortDirection') ?? 'asc').toLowerCase() === 'desc'
        ? 'DESC'
        : 'ASC'
      const sortDirection = sortBy === 'lista' ? requestedSortDirection : 'ASC'
      const offset = (page - 1) * pageSize
      const filters = []
      const values = []
      let orderByClause = `CAST(codigo AS integer) ${sortDirection}, CAST(controle AS integer) ASC`

      if (sortBy === 'controle') {
        orderByClause = `CAST(controle AS integer) ${sortDirection}, CAST(codigo AS integer) ASC`
      } else if (sortBy === 'lista') {
        orderByClause = `BTRIM(lista) ${sortDirection}, CAST(codigo AS integer) ASC, CAST(controle AS integer) ASC`
      }

      if (search) {
        values.push(`%${search}%`)
        filters.push(`(
          CAST(codigo AS text) ILIKE $${values.length}
          OR CAST(controle AS text) ILIKE $${values.length}
          OR BTRIM(lista) ILIKE $${values.length}
        )`)
      }

      const whereClause = filters.length ? `WHERE ${filters.join(' AND ')}` : ''
      const countResult = await pool.query(
        `SELECT COUNT(*)::int AS total FROM seguradora ${whereClause}`,
        values,
      )

      values.push(pageSize)
      values.push(offset)
      const result = await pool.query(
        `SELECT ${seguradoraSelectClause}
         FROM seguradora
         ${whereClause}
         ORDER BY ${orderByClause}
         LIMIT $${values.length - 1}
         OFFSET $${values.length}`,
        values,
      )
      const total = countResult.rows[0]?.total ?? 0

      sendJson(response, 200, {
        items: result.rows,
        total,
        page,
        pageSize,
        totalPages: Math.max(Math.ceil(total / pageSize), 1),
        sortBy: sortBy === 'controle' || sortBy === 'lista' ? sortBy : 'codigo',
        sortDirection: sortDirection.toLowerCase(),
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao consultar a tabela seguradora.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'GET' && pathname === '/api/access') {
    try {
      const search = normalizeRequestValue(requestUrl.searchParams.get('search') ?? '')
      const page = Math.max(Number(requestUrl.searchParams.get('page') ?? 1) || 1, 1)
      const pageSize = Math.min(Math.max(Number(requestUrl.searchParams.get('pageSize') ?? 5) || 5, 1), 50)
      const sortBy = normalizeRequestValue(requestUrl.searchParams.get('sortBy') ?? 'codigo')
      const sortDirection = normalizeRequestValue(requestUrl.searchParams.get('sortDirection') ?? 'asc').toLowerCase() === 'desc'
        ? 'DESC'
        : 'ASC'
      const offset = (page - 1) * pageSize
      const values = []
      const filters = []
      const accessCodigoOrderClause = 'login.codigo'
      const orderByClause = sortBy === 'nome'
        ? `UPPER(BTRIM(nome)) ${sortDirection}, ${accessCodigoOrderClause} ASC`
          : sortBy === 'email'
            ? `LOWER(TRIM(email)) ${sortDirection}, ${accessCodigoOrderClause} ASC`
            : `${accessCodigoOrderClause} ${sortDirection}`

      if (search) {
        values.push(`%${search}%`)
        filters.push(`(
          CAST(codigo AS text) ILIKE $${values.length}
          OR UPPER(BTRIM(nome)) ILIKE UPPER($${values.length})
          OR LOWER(TRIM(email)) ILIKE LOWER($${values.length})
        )`)
      }

      const whereClause = filters.length ? `WHERE ${filters.join(' AND ')}` : ''
      const countResult = await pool.query(
        `SELECT COUNT(*)::int AS total FROM login ${whereClause}`,
        values,
      )

      values.push(pageSize)
      values.push(offset)
      const result = await pool.query(
        `SELECT codigo::text AS codigo, BTRIM(nome) AS nome, TRIM(email) AS email
         FROM login
         ${whereClause}
         ORDER BY ${orderByClause}
         LIMIT $${values.length - 1}
         OFFSET $${values.length}`,
        values,
      )
      const total = countResult.rows[0]?.total ?? 0

      sendJson(response, 200, {
        items: result.rows,
        total,
        page,
        pageSize,
        totalPages: Math.max(Math.ceil(total / pageSize), 1),
        sortBy: sortBy === 'nome' || sortBy === 'email' ? sortBy : 'codigo',
        sortDirection: sortDirection.toLowerCase(),
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao consultar acessos.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'GET' && pathname === '/api/condutor') {
    try {
      const search = normalizeRequestValue(requestUrl.searchParams.get('search') ?? '')
      const page = Math.max(Number(requestUrl.searchParams.get('page') ?? 1) || 1, 1)
      const pageSize = Math.min(Math.max(Number(requestUrl.searchParams.get('pageSize') ?? 5) || 5, 1), 50)
      const sortBy = normalizeRequestValue(requestUrl.searchParams.get('sortBy') ?? 'codigo')
      const sortDirection = normalizeRequestValue(requestUrl.searchParams.get('sortDirection') ?? 'asc').toLowerCase() === 'desc'
        ? 'DESC'
        : 'ASC'
      const offset = (page - 1) * pageSize
      const values = []
      const filters = []
      const orderByClause = sortBy === 'condutor'
        ? `UPPER(BTRIM(condutor)) ${sortDirection}, codigo ASC`
        : `codigo ${sortDirection}`

      if (search) {
        values.push(`%${search}%`)
        filters.push(`(
          CAST(codigo AS text) ILIKE $${values.length}
          OR UPPER(BTRIM(condutor)) ILIKE UPPER($${values.length})
          OR BTRIM(cpf_condutor) ILIKE $${values.length}
          OR UPPER(BTRIM(crmc)) ILIKE UPPER($${values.length})
        )`)
      }

      const whereClause = filters.length ? `WHERE ${filters.join(' AND ')}` : ''
      const countResult = await pool.query(
        `SELECT COUNT(*)::int AS total FROM condutor ${whereClause}`,
        values,
      )

      values.push(pageSize)
      values.push(offset)
      const result = await pool.query(
        `SELECT
           ${condutorSelectClause}
         FROM condutor
         ${whereClause}
         ORDER BY ${orderByClause}
         LIMIT $${values.length - 1}
         OFFSET $${values.length}`,
        values,
      )
      const total = countResult.rows[0]?.total ?? 0

      sendJson(response, 200, {
        items: result.rows,
        total,
        page,
        pageSize,
        totalPages: Math.max(Math.ceil(total / pageSize), 1),
        sortBy: sortBy === 'condutor' ? 'condutor' : 'codigo',
        sortDirection: sortDirection.toLowerCase(),
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao consultar condutores.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'GET' && pathname === '/api/monitor') {
    try {
      const search = normalizeRequestValue(requestUrl.searchParams.get('search') ?? '')
      const page = Math.max(Number(requestUrl.searchParams.get('page') ?? 1) || 1, 1)
      const pageSize = Math.min(Math.max(Number(requestUrl.searchParams.get('pageSize') ?? 5) || 5, 1), 50)
      const sortBy = normalizeRequestValue(requestUrl.searchParams.get('sortBy') ?? 'codigo')
      const sortDirection = normalizeRequestValue(requestUrl.searchParams.get('sortDirection') ?? 'asc').toLowerCase() === 'desc'
        ? 'DESC'
        : 'ASC'
      const offset = (page - 1) * pageSize
      const values = []
      const filters = []
      const orderByClause = sortBy === 'monitor'
        ? `UPPER(BTRIM(monitor)) ${sortDirection}, codigo ASC`
        : `codigo ${sortDirection}`

      if (search) {
        values.push(`%${search}%`)
        filters.push(`(
          CAST(codigo AS text) ILIKE $${values.length}
          OR UPPER(BTRIM(monitor)) ILIKE UPPER($${values.length})
          OR BTRIM(cpf_monitor) ILIKE $${values.length}
          OR COALESCE(BTRIM(rg_monitor), '') ILIKE UPPER($${values.length})
        )`)
      }

      const whereClause = filters.length ? `WHERE ${filters.join(' AND ')}` : ''
      const countResult = await pool.query(
        `SELECT COUNT(*)::int AS total FROM monitor ${whereClause}`,
        values,
      )

      values.push(pageSize)
      values.push(offset)
      const result = await pool.query(
        `SELECT
           ${monitorSelectClause}
         FROM monitor
         ${whereClause}
         ORDER BY ${orderByClause}
         LIMIT $${values.length - 1}
         OFFSET $${values.length}`,
        values,
      )
      const total = countResult.rows[0]?.total ?? 0

      sendJson(response, 200, {
        items: result.rows,
        total,
        page,
        pageSize,
        totalPages: Math.max(Math.ceil(total / pageSize), 1),
        sortBy: sortBy === 'monitor' ? 'monitor' : 'codigo',
        sortDirection: sortDirection.toLowerCase(),
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao consultar monitores.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'GET' && pathname === '/api/monitor/import-rejections') {
    try {
      const search = normalizeRequestValue(requestUrl.searchParams.get('search') ?? '')
      const page = Math.max(Number(requestUrl.searchParams.get('page') ?? 1) || 1, 1)
      const pageSize = Math.min(Math.max(Number(requestUrl.searchParams.get('pageSize') ?? 10) || 10, 1), 100)
      const offset = (page - 1) * pageSize
      const values = []
      const filters = []

      if (search) {
        values.push(`%${search}%`)
        filters.push(`(
          arquivo_xml ILIKE $${values.length}
          OR COALESCE(codigo_xml, '') ILIKE $${values.length}
          OR COALESCE(monitor_xml, '') ILIKE $${values.length}
          OR COALESCE(cpf_monitor_xml, '') ILIKE $${values.length}
          OR COALESCE(rg_monitor_xml, '') ILIKE $${values.length}
          OR COALESCE(tipo_vinculo_xml, '') ILIKE $${values.length}
          OR motivo_recusa ILIKE $${values.length}
        )`)
      }

      const whereClause = filters.length ? `WHERE ${filters.join(' AND ')}` : ''
      const countResult = await pool.query(
        `SELECT COUNT(*)::int AS total FROM monitor_import_recusa ${whereClause}`,
        values,
      )

      values.push(pageSize)
      values.push(offset)
      const result = await pool.query(
        `SELECT
           ${monitorImportRecusaSelectClause}
         FROM monitor_import_recusa
         ${whereClause}
         ORDER BY data_importacao DESC, linha_xml ASC, id DESC
         LIMIT $${values.length - 1}
         OFFSET $${values.length}`,
        values,
      )
      const total = countResult.rows[0]?.total ?? 0

      sendJson(response, 200, {
        items: result.rows,
        total,
        page,
        pageSize,
        totalPages: Math.max(Math.ceil(total / pageSize), 1),
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao consultar recusas de importacao do monitor.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'GET' && pathname === '/api/veiculo') {
    try {
      const search = normalizeRequestValue(requestUrl.searchParams.get('search') ?? '')
      const page = Math.max(Number(requestUrl.searchParams.get('page') ?? 1) || 1, 1)
      const pageSize = Math.min(Math.max(Number(requestUrl.searchParams.get('pageSize') ?? 5) || 5, 1), 50)
      const sortBy = normalizeRequestValue(requestUrl.searchParams.get('sortBy') ?? 'codigo')
      const sortDirection = normalizeRequestValue(requestUrl.searchParams.get('sortDirection') ?? 'asc').toLowerCase() === 'desc'
        ? 'DESC'
        : 'ASC'
      const offset = (page - 1) * pageSize
      const values = []
      const filters = []
      const orderByClause = sortBy === 'placas'
        ? `UPPER(BTRIM(placas)) ${sortDirection}, codigo ASC`
        : `codigo ${sortDirection}`

      if (search) {
        values.push(`%${search}%`)
        filters.push(`(
          CAST(codigo AS text) ILIKE $${values.length}
          OR COALESCE(BTRIM(placas), '') ILIKE UPPER($${values.length})
          OR COALESCE(BTRIM(crm), '') ILIKE UPPER($${values.length})
          OR COALESCE(BTRIM(marca_modelo), '') ILIKE UPPER($${values.length})
          OR COALESCE(BTRIM(titular), '') ILIKE UPPER($${values.length})
        )`)
      }

      const whereClause = filters.length ? `WHERE ${filters.join(' AND ')}` : ''
      const countResult = await pool.query(
        `SELECT COUNT(*)::int AS total FROM veiculo ${whereClause}`,
        values,
      )

      values.push(pageSize)
      values.push(offset)
      const result = await pool.query(
        `SELECT
           ${veiculoSelectClause}
         FROM veiculo
         ${whereClause}
         ORDER BY ${orderByClause}
         LIMIT $${values.length - 1}
         OFFSET $${values.length}`,
        values,
      )
      const total = countResult.rows[0]?.total ?? 0

      sendJson(response, 200, {
        items: result.rows,
        total,
        page,
        pageSize,
        totalPages: Math.max(Math.ceil(total / pageSize), 1),
        sortBy: sortBy === 'placas' ? 'placas' : 'codigo',
        sortDirection: sortDirection.toLowerCase(),
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao consultar veiculos.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'GET' && pathname === credenciamentoTermoCollectionPath) {
    try {
      const search = normalizeRequestValue(requestUrl.searchParams.get('search') ?? '')
      const statusTermo = normalizeRequestValue(requestUrl.searchParams.get('statusTermo') ?? '')
      const page = Math.max(Number(requestUrl.searchParams.get('page') ?? 1) || 1, 1)
      const pageSize = Math.min(Math.max(Number(requestUrl.searchParams.get('pageSize') ?? 20) || 20, 1), 50)
      const sortBy = normalizeRequestValue(requestUrl.searchParams.get('sortBy') ?? 'codigo')
      const sortDirection = normalizeRequestValue(requestUrl.searchParams.get('sortDirection') ?? 'asc').toLowerCase() === 'desc'
        ? 'DESC'
        : 'ASC'
      const offset = (page - 1) * pageSize
      const values = []
      const filters = []
      const normalizedSearchTermo = normalizeOrdemServicoTermoAdesao(search)
      const normalizedSearchTermoDigits = normalizedSearchTermo.replace(/\D/g, '')
      const orderByClause = sortBy === 'termo_adesao'
        ? `UPPER(BTRIM(termo_adesao)) ${sortDirection}, CAST(codigo AS integer) ASC`
        : sortBy === 'credenciado'
          ? `UPPER(BTRIM(${credenciamentoTermoCredenciadoExpression})) ${sortDirection}, CAST(codigo AS integer) ASC`
          : sortBy === 'aditivo'
            ? `aditivo ${sortDirection}, CAST(codigo AS integer) ASC`
            : `CAST(codigo AS integer) ${sortDirection}`

      if (search) {
        values.push(`%${search}%`)
        filters.push(`(
          CAST(codigo AS text) ILIKE $${values.length}
          OR COALESCE(BTRIM(termo_adesao), '') ILIKE UPPER($${values.length})
          OR ${credenciamentoTermoCredenciadoExpression} ILIKE UPPER($${values.length})
          OR COALESCE(BTRIM(sei), '') ILIKE UPPER($${values.length})
          OR CAST(aditivo AS text) ILIKE $${values.length}
        )`)

        if (normalizedSearchTermoDigits) {
          values.push(`%${normalizedSearchTermoDigits}%`)
          filters.push(`${credenciamentoTermoNormalizedTermoExpression} ILIKE $${values.length}`)
        }
      }

      if (statusTermo) {
        values.push(statusTermo)
        filters.push(`UPPER(BTRIM(COALESCE(status_termo, ''))) = UPPER($${values.length})`)
      }

      const whereClause = filters.length ? `WHERE ${filters.join(' AND ')}` : ''
      const countResult = await pool.query(
        `SELECT COUNT(*)::int AS total FROM ${credenciamentoTermoTableName} ${whereClause}`,
        values,
      )

      values.push(pageSize)
      values.push(offset)
      const result = await pool.query(
        `SELECT
           ${credenciamentoTermoSelectClause}
         FROM ${credenciamentoTermoTableName}
         ${whereClause}
         ORDER BY ${orderByClause}
         LIMIT $${values.length - 1}
         OFFSET $${values.length}`,
        values,
      )
      const total = countResult.rows[0]?.total ?? 0
      const items = result.rows.map((item) => ({
        ...item,
        valorContratoExtenso: buildCurrencyExtenso(item.valor_contrato),
      }))

      sendJson(response, 200, {
        items,
        total,
        page,
        pageSize,
        totalPages: Math.max(Math.ceil(total / pageSize), 1),
        sortBy: ['termo_adesao', 'credenciado', 'aditivo'].includes(sortBy) ? sortBy : 'codigo',
        sortDirection: sortDirection.toLowerCase(),
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao consultar credenciamentos termo.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'GET' && pathname === credenciamentoTermoLookupPath) {
    try {
      const termoAdesao = normalizeOrdemServicoTermoAdesao(requestUrl.searchParams.get('termoAdesao') ?? '')

      if (!termoAdesao) {
        sendJson(response, 400, { message: 'Termo e obrigatorio.' })
        return
      }

      const item = await findCredenciamentoTermoByTermoAdesao(termoAdesao)

      if (!item) {
        sendJson(response, 404, { message: 'Termo de adesao nao encontrado na tabela termo.' })
        return
      }

      sendJson(response, 200, {
        item: {
          ...item,
          termoAdesao: normalizeRequestValue(item.termo_adesao),
          credenciadoCodigo: item.credenciada_codigo ? Number(item.credenciada_codigo) : null,
          empresa: normalizeCredenciadaText(item.empresa, 255),
          tipoTermo: normalizeCredenciadaText(item.tipo_termo, 100),
          cnpjCpf: normalizeCnpjCpf(item.cnpj_cpf),
          valorContratoExtenso: buildCurrencyExtenso(item.valor_contrato),
        },
      })
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Erro ao consultar termo.'
      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'GET' && pathname === credenciamentoTermoImportRejectionsPath) {
    try {
      const search = normalizeRequestValue(requestUrl.searchParams.get('search') ?? '')
      const page = Math.max(Number(requestUrl.searchParams.get('page') ?? 1) || 1, 1)
      const pageSize = Math.min(Math.max(Number(requestUrl.searchParams.get('pageSize') ?? 20) || 20, 1), 50)
      const offset = (page - 1) * pageSize
      const values = []
      const filters = []

      if (search) {
        values.push(`%${search}%`)
        filters.push(`(
          COALESCE(BTRIM(arquivo_xml), '') ILIKE $${values.length}
          OR COALESCE(BTRIM(codigo_xml), '') ILIKE $${values.length}
          OR COALESCE(BTRIM(credenciado_xml), '') ILIKE UPPER($${values.length})
          OR COALESCE(BTRIM(aditivo_xml), '') ILIKE $${values.length}
          OR COALESCE(BTRIM(motivo_recusa), '') ILIKE UPPER($${values.length})
        )`)
      }

      const whereClause = filters.length ? `WHERE ${filters.join(' AND ')}` : ''
      const countResult = await pool.query(
        `SELECT COUNT(*)::int AS total FROM ${credenciamentoTermoImportRecusaTableName} ${whereClause}`,
        values,
      )

      values.push(pageSize)
      values.push(offset)
      const result = await pool.query(
        `SELECT
           ${credenciamentoTermoImportRecusaSelectClause}
         FROM ${credenciamentoTermoImportRecusaTableName}
         ${whereClause}
         ORDER BY id DESC
         LIMIT $${values.length - 1}
         OFFSET $${values.length}`,
        values,
      )
      const total = countResult.rows[0]?.total ?? 0

      sendJson(response, 200, {
        items: result.rows,
        total,
        page,
        pageSize,
        totalPages: Math.max(Math.ceil(total / pageSize), 1),
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao consultar recusas de importacao do credenciamento termo.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'GET' && pathname === vinculoCondutorCollectionPath) {
    try {
      const search = normalizeRequestValue(requestUrl.searchParams.get('search') ?? '')
      const page = Math.max(Number(requestUrl.searchParams.get('page') ?? 1) || 1, 1)
      const pageSize = Math.min(Math.max(Number(requestUrl.searchParams.get('pageSize') ?? 5) || 5, 1), 50)
      const sortBy = normalizeRequestValue(requestUrl.searchParams.get('sortBy') ?? 'id')
      const sortDirection = normalizeRequestValue(requestUrl.searchParams.get('sortDirection') ?? 'desc').toLowerCase() === 'asc'
        ? 'ASC'
        : 'DESC'
      const offset = (page - 1) * pageSize
      const values = []
      const filters = []
      const orderByClause = sortBy === 'credenciado'
        ? `UPPER(BTRIM(cr.credenciado)) ${sortDirection}, vc.id DESC`
        : sortBy === 'cpf_condutor'
          ? `COALESCE(BTRIM(cd.cpf_condutor), '') ${sortDirection}, vc.id DESC`
          : `vc.id ${sortDirection}`

      if (search) {
        values.push(`%${search}%`)
        filters.push(`(
          CAST(vc.id AS text) ILIKE $${values.length}
          OR COALESCE(BTRIM(cr.credenciado), '') ILIKE UPPER($${values.length})
          OR COALESCE(BTRIM(cd.condutor), '') ILIKE UPPER($${values.length})
          OR COALESCE(BTRIM(cd.cpf_condutor), '') ILIKE $${values.length}
          OR COALESCE(TO_CHAR(vc.data_os::date, 'YYYY-MM-DD'), '') ILIKE $${values.length}
          OR COALESCE(TO_CHAR(vc.data_admissao_condutor::date, 'YYYY-MM-DD'), '') ILIKE $${values.length}
        )`)
      }

      const whereClause = filters.length ? `WHERE ${filters.join(' AND ')}` : ''
      const fromClause = `
        FROM ${vinculoCondutorTableName} vc
        LEFT JOIN credenciada cr
          ON cr.codigo = vc.credenciada_codigo
        LEFT JOIN condutor cd
          ON cd.codigo = vc.condutor_codigo
      `
      const countResult = await pool.query(
        `SELECT COUNT(*)::int AS total ${fromClause} ${whereClause}`,
        values,
      )

      values.push(pageSize)
      values.push(offset)
      const result = await pool.query(
        `SELECT
           ${vinculoCondutorSelectClause}
         ${fromClause}
         ${whereClause}
         ORDER BY ${orderByClause}
         LIMIT $${values.length - 1}
         OFFSET $${values.length}`,
        values,
      )
      const total = countResult.rows[0]?.total ?? 0

      sendJson(response, 200, {
        items: result.rows,
        total,
        page,
        pageSize,
        totalPages: Math.max(Math.ceil(total / pageSize), 1),
        sortBy: sortBy === 'credenciado' || sortBy === 'cpf_condutor' ? sortBy : 'id',
        sortDirection: sortDirection.toLowerCase(),
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao consultar vinculos de condutor.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'GET' && pathname === '/api/titular') {
    try {
      const search = normalizeRequestValue(requestUrl.searchParams.get('search') ?? '')
      const page = Math.max(Number(requestUrl.searchParams.get('page') ?? 1) || 1, 1)
      const pageSize = Math.min(Math.max(Number(requestUrl.searchParams.get('pageSize') ?? 5) || 5, 1), 50)
      const sortBy = normalizeRequestValue(requestUrl.searchParams.get('sortBy') ?? 'codigo')
      const sortDirection = normalizeRequestValue(requestUrl.searchParams.get('sortDirection') ?? 'asc').toLowerCase() === 'desc'
        ? 'DESC'
        : 'ASC'
      const offset = (page - 1) * pageSize
      const values = []
      const filters = []
      const titularCodigoOrderClause = `
        CASE
          WHEN BTRIM(CAST(codigo AS text)) ~ '^[0-9]+$' THEN CAST(BTRIM(CAST(codigo AS text)) AS bigint)
          ELSE NULL
        END
      `
      const orderByClause = sortBy === 'cnpj_cpf'
        ? `BTRIM(cnpj_cpf) ${sortDirection}, ${titularCodigoOrderClause} ASC, CAST(codigo AS text) ASC`
        : sortBy === 'titular'
          ? `UPPER(BTRIM(titular)) ${sortDirection}, ${titularCodigoOrderClause} ASC, CAST(codigo AS text) ASC`
          : `${titularCodigoOrderClause} ${sortDirection}, CAST(codigo AS text) ${sortDirection}`

      if (search) {
        values.push(`%${search}%`)
        filters.push(`(
          CAST(codigo AS text) ILIKE $${values.length}
          OR COALESCE(BTRIM(cnpj_cpf), '') ILIKE $${values.length}
          OR COALESCE(BTRIM(titular), '') ILIKE UPPER($${values.length})
        )`)
      }

      const whereClause = filters.length ? `WHERE ${filters.join(' AND ')}` : ''
      const countResult = await pool.query(
        `SELECT COUNT(*)::int AS total FROM ${titularTableName} ${whereClause}`,
        values,
      )

      values.push(pageSize)
      values.push(offset)
      const result = await pool.query(
        `SELECT
           ${titularSelectClause}
         FROM ${titularTableName}
         ${whereClause}
         ORDER BY ${orderByClause}
         LIMIT $${values.length - 1}
         OFFSET $${values.length}`,
        values,
      )
      const total = countResult.rows[0]?.total ?? 0

      sendJson(response, 200, {
        items: result.rows,
        total,
        page,
        pageSize,
        totalPages: Math.max(Math.ceil(total / pageSize), 1),
        sortBy: sortBy === 'cnpj_cpf' || sortBy === 'titular' ? sortBy : 'codigo',
        sortDirection: sortDirection.toLowerCase(),
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao consultar titulares do CRM.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'GET' && pathname === '/api/titular/lookup') {
    try {
      const cnpjCpf = normalizeTitularDocument(requestUrl.searchParams.get('cnpjCpf') ?? '')

      if (!cnpjCpf) {
        sendJson(response, 400, { message: 'CNPJ/CPF e obrigatorio.' })
        return
      }

      if (!isCnpjCpfValid(cnpjCpf)) {
        sendJson(response, 400, { message: 'CNPJ/CPF deve conter 11 ou 14 digitos.' })
        return
      }

      const titularItem = await findTitularByCnpjCpf(cnpjCpf)

      if (!titularItem) {
        sendJson(response, 404, { message: 'CNPJ/CPF nao encontrado na tabela titularCrm.' })
        return
      }

      sendJson(response, 200, { item: titularItem })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao consultar titular do CRM por CNPJ/CPF.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'GET' && pathname === '/api/veiculo/import-rejections') {
    try {
      const search = normalizeRequestValue(requestUrl.searchParams.get('search') ?? '')
      const page = Math.max(Number(requestUrl.searchParams.get('page') ?? 1) || 1, 1)
      const pageSize = Math.min(Math.max(Number(requestUrl.searchParams.get('pageSize') ?? 10) || 10, 1), 100)
      const offset = (page - 1) * pageSize
      const values = []
      const filters = []

      if (search) {
        values.push(`%${search}%`)
        filters.push(`(
          arquivo_xml ILIKE $${values.length}
          OR COALESCE(codigo_xml, '') ILIKE $${values.length}
          OR COALESCE(crm_xml, '') ILIKE $${values.length}
          OR COALESCE(placas_xml, '') ILIKE $${values.length}
          OR COALESCE(tipo_de_veiculo_xml, '') ILIKE $${values.length}
          OR motivo_recusa ILIKE $${values.length}
        )`)
      }

      const whereClause = filters.length ? `WHERE ${filters.join(' AND ')}` : ''
      const countResult = await pool.query(
        `SELECT COUNT(*)::int AS total FROM veiculo_import_recusa ${whereClause}`,
        values,
      )

      values.push(pageSize)
      values.push(offset)
      const result = await pool.query(
        `SELECT
           ${veiculoImportRecusaSelectClause}
         FROM veiculo_import_recusa
         ${whereClause}
         ORDER BY data_importacao DESC, linha_xml ASC, id DESC
         LIMIT $${values.length - 1}
         OFFSET $${values.length}`,
        values,
      )
      const total = countResult.rows[0]?.total ?? 0

      sendJson(response, 200, {
        items: result.rows,
        total,
        page,
        pageSize,
        totalPages: Math.max(Math.ceil(total / pageSize), 1),
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao consultar recusas de importacao do veiculo.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'GET' && pathname === vinculoMonitorCollectionPath) {
    try {
      const search = normalizeRequestValue(requestUrl.searchParams.get('search') ?? '')
      const page = Math.max(Number(requestUrl.searchParams.get('page') ?? 1) || 1, 1)
      const pageSize = Math.min(Math.max(Number(requestUrl.searchParams.get('pageSize') ?? 5) || 5, 1), 50)
      const sortBy = normalizeRequestValue(requestUrl.searchParams.get('sortBy') ?? 'id')
      const sortDirection = normalizeRequestValue(requestUrl.searchParams.get('sortDirection') ?? 'desc').toLowerCase() === 'asc'
        ? 'ASC'
        : 'DESC'
      const offset = (page - 1) * pageSize
      const values = []
      const filters = []
      const orderByClause = sortBy === 'credenciado'
        ? `UPPER(BTRIM(cr.credenciado)) ${sortDirection}, vm.id DESC`
        : sortBy === 'cpf_monitor'
          ? `COALESCE(BTRIM(mt.cpf_monitor), '') ${sortDirection}, vm.id DESC`
          : `vm.id ${sortDirection}`

      if (search) {
        values.push(`%${search}%`)
        filters.push(`(
          CAST(vm.id AS text) ILIKE $${values.length}
          OR COALESCE(BTRIM(cr.credenciado), '') ILIKE UPPER($${values.length})
          OR COALESCE(BTRIM(mt.cpf_monitor), '') ILIKE $${values.length}
          OR COALESCE(BTRIM(mt.monitor), '') ILIKE UPPER($${values.length})
          OR COALESCE(TO_CHAR(vm.data_os::date, 'YYYY-MM-DD'), '') ILIKE $${values.length}
        )`)
      }

      const whereClause = filters.length ? `WHERE ${filters.join(' AND ')}` : ''
      const fromClause = `
        FROM ${vinculoMonitorTableName} vm
        LEFT JOIN credenciada cr
          ON cr.codigo = vm.credenciada_codigo
        LEFT JOIN monitor mt
          ON mt.codigo = vm.monitor_codigo
      `
      const countResult = await pool.query(
        `SELECT COUNT(*)::int AS total ${fromClause} ${whereClause}`,
        values,
      )

      values.push(pageSize)
      values.push(offset)
      const result = await pool.query(
        `SELECT
           ${vinculoMonitorSelectClause}
         ${fromClause}
         ${whereClause}
         ORDER BY ${orderByClause}
         LIMIT $${values.length - 1}
         OFFSET $${values.length}`,
        values,
      )
      const total = countResult.rows[0]?.total ?? 0

      sendJson(response, 200, {
        items: result.rows,
        total,
        page,
        pageSize,
        totalPages: Math.max(Math.ceil(total / pageSize), 1),
        sortBy: sortBy === 'credenciado' || sortBy === 'cpf_monitor' ? sortBy : 'id',
        sortDirection: sortDirection.toLowerCase(),
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao consultar vinculos do monitor.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'GET' && pathname === vinculoCondutorImportRejectionsPath) {
    try {
      const search = normalizeRequestValue(requestUrl.searchParams.get('search') ?? '')
      const page = Math.max(Number(requestUrl.searchParams.get('page') ?? 1) || 1, 1)
      const pageSize = Math.min(Math.max(Number(requestUrl.searchParams.get('pageSize') ?? 10) || 10, 1), 100)
      const offset = (page - 1) * pageSize
      const values = []
      const filters = []

      if (search) {
        values.push(`%${search}%`)
        filters.push(`(
          arquivo_xml ILIKE $${values.length}
          OR COALESCE(codigo_xml, '') ILIKE $${values.length}
          OR COALESCE(empregador_xml, '') ILIKE UPPER($${values.length})
          OR COALESCE(cpf_condutor_xml, '') ILIKE $${values.length}
          OR COALESCE(data_os_xml, '') ILIKE $${values.length}
          OR COALESCE(admissao_xml, '') ILIKE $${values.length}
          OR motivo_recusa ILIKE $${values.length}
        )`)
      }

      const whereClause = filters.length ? `WHERE ${filters.join(' AND ')}` : ''
      const countResult = await pool.query(
        `SELECT COUNT(*)::int AS total FROM ${vinculoCondutorImportRecusaTableName} ${whereClause}`,
        values,
      )

      values.push(pageSize)
      values.push(offset)
      const result = await pool.query(
        `SELECT
           ${vinculoCondutorImportRecusaSelectClause}
         FROM ${vinculoCondutorImportRecusaTableName}
         ${whereClause}
         ORDER BY data_importacao DESC, linha_xml ASC, id DESC
         LIMIT $${values.length - 1}
         OFFSET $${values.length}`,
        values,
      )
      const total = countResult.rows[0]?.total ?? 0

      sendJson(response, 200, {
        items: result.rows,
        total,
        page,
        pageSize,
        totalPages: Math.max(Math.ceil(total / pageSize), 1),
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao consultar recusas de importacao do vinculo do condutor.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'GET' && pathname === vinculoMonitorImportRejectionsPath) {
    try {
      const search = normalizeRequestValue(requestUrl.searchParams.get('search') ?? '')
      const page = Math.max(Number(requestUrl.searchParams.get('page') ?? 1) || 1, 1)
      const pageSize = Math.min(Math.max(Number(requestUrl.searchParams.get('pageSize') ?? 10) || 10, 1), 100)
      const offset = (page - 1) * pageSize
      const values = []
      const filters = []

      if (search) {
        values.push(`%${search}%`)
        filters.push(`(
          arquivo_xml ILIKE $${values.length}
          OR COALESCE(codigo_xml, '') ILIKE $${values.length}
          OR COALESCE(empregador_xml, '') ILIKE UPPER($${values.length})
          OR COALESCE(cpf_monitor_xml, '') ILIKE $${values.length}
          OR COALESCE(data_os_xml, '') ILIKE $${values.length}
          OR COALESCE(admissao_xml, '') ILIKE $${values.length}
          OR motivo_recusa ILIKE $${values.length}
        )`)
      }

      const whereClause = filters.length ? `WHERE ${filters.join(' AND ')}` : ''
      const countResult = await pool.query(
        `SELECT COUNT(*)::int AS total FROM ${vinculoMonitorImportRecusaTableName} ${whereClause}`,
        values,
      )

      values.push(pageSize)
      values.push(offset)
      const result = await pool.query(
        `SELECT
           ${vinculoMonitorImportRecusaSelectClause}
         FROM ${vinculoMonitorImportRecusaTableName}
         ${whereClause}
         ORDER BY data_importacao DESC, linha_xml ASC, id DESC
         LIMIT $${values.length - 1}
         OFFSET $${values.length}`,
        values,
      )
      const total = countResult.rows[0]?.total ?? 0

      sendJson(response, 200, {
        items: result.rows,
        total,
        page,
        pageSize,
        totalPages: Math.max(Math.ceil(total / pageSize), 1),
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao consultar recusas de importacao do vinculo do monitor.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'GET' && pathname === cepLookupPath) {
    try {
      const cepParam = normalizeCep(requestUrl.searchParams.get('cep') ?? '')
      const useCloud = requestUrl.searchParams.get('cloud') === 'true'

      if (!cepParam || !isCepValid(cepParam)) {
        sendJson(response, 400, { message: 'CEP invalido.' })
        return
      }

      const result = await pool.query(
        `SELECT ${cepSelectClause} FROM ${cepTableName} WHERE BTRIM(cep) = $1 LIMIT 1`,
        [cepParam],
      )

      if (!result.rows[0]) {
        if (!useCloud) {
          sendJson(response, 404, { message: 'CEP nao encontrado na base local.' })
          return
        }

        const persistCepLookupRecord = async (record) => {
          try {
            await pool.query(
              `INSERT INTO ${cepTableName} (cep, logradouro, complemento, bairro, municipio, uf, ibge, data_inclusao, data_modificacao)
               VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), NOW())
               ON CONFLICT (cep) DO UPDATE SET
                 logradouro = EXCLUDED.logradouro,
                 complemento = EXCLUDED.complemento,
                 bairro = EXCLUDED.bairro,
                 municipio = EXCLUDED.municipio,
                 uf = EXCLUDED.uf,
                 ibge = EXCLUDED.ibge,
                 data_modificacao = NOW()`,
              [record.cep, record.logradouro, record.complemento, record.bairro, record.municipio, record.uf, record.ibge],
            )
          } catch {
            // Falha ao salvar localmente nao impede retorno
          }
        }

        const digits = cepParam.replace('-', '')

        // Consulta ViaCEP primeiro e, se indisponivel, tenta BrasilAPI/OpenCEP.
        try {
          const viaCepRes = await fetch(`https://viacep.com.br/ws/${digits}/json/`)

          if (viaCepRes.ok) {
            const viaCepData = await viaCepRes.json()

            if (!viaCepData.erro) {
              const newRecord = {
                cep: cepParam,
                logradouro: (viaCepData.logradouro || '').toUpperCase(),
                complemento: (viaCepData.complemento || '').toUpperCase(),
                bairro: (viaCepData.bairro || '').toUpperCase(),
                municipio: (viaCepData.localidade || '').toUpperCase(),
                uf: (viaCepData.uf || '').toUpperCase(),
                ibge: viaCepData.ibge || '',
              }

              await persistCepLookupRecord(newRecord)

              sendJson(response, 200, {
                item: { ...newRecord, data_inclusao: null, data_modificacao: null },
                source: 'viacep',
              })
              return
            }
          }
        } catch {
          // ViaCEP indisponivel.
        }

        try {
          const brasilApiRes = await fetch(`https://brasilapi.com.br/api/cep/v1/${digits}`)

          if (brasilApiRes.ok) {
            const brasilApiData = await brasilApiRes.json()
            const newRecord = {
              cep: cepParam,
              logradouro: String(brasilApiData.street || '').toUpperCase(),
              complemento: '',
              bairro: String(brasilApiData.neighborhood || '').toUpperCase(),
              municipio: String(brasilApiData.city || '').toUpperCase(),
              uf: String(brasilApiData.state || '').toUpperCase(),
              ibge: String(brasilApiData.city_ibge || ''),
            }

            await persistCepLookupRecord(newRecord)

            sendJson(response, 200, {
              item: { ...newRecord, data_inclusao: null, data_modificacao: null },
              source: 'brasilapi',
            })
            return
          }
        } catch {
          // BrasilAPI/OpenCEP indisponivel.
        }

        sendJson(response, 404, { message: 'CEP nao encontrado.' })
        return
      }

      sendJson(response, 200, { item: result.rows[0] })
    } catch (error) {
      sendJson(response, 500, { message: error instanceof Error ? error.message : 'Erro ao consultar CEP.' })
    }

    return
  }

  if (request.method === 'GET' && pathname === cepImportRejectionsPath) {
    try {
      const search = normalizeRequestValue(requestUrl.searchParams.get('search') ?? '')
      const page = Math.max(Number(requestUrl.searchParams.get('page') ?? 1) || 1, 1)
      const pageSize = Math.min(Math.max(Number(requestUrl.searchParams.get('pageSize') ?? 20) || 20, 1), 100)
      const offset = (page - 1) * pageSize
      const values = []
      const filters = []

      if (search) {
        values.push(`%${search}%`)
        filters.push(`(
          arquivo_xml ILIKE $${values.length}
          OR COALESCE(cep_xml, '') ILIKE $${values.length}
          OR COALESCE(municipio_xml, '') ILIKE $${values.length}
          OR COALESCE(uf_xml, '') ILIKE $${values.length}
          OR motivo_recusa ILIKE $${values.length}
        )`)
      }

      const whereClause = filters.length ? `WHERE ${filters.join(' AND ')}` : ''
      const countResult = await pool.query(
        `SELECT COUNT(*)::int AS total FROM ${cepImportRecusaTableName} ${whereClause}`,
        values,
      )

      values.push(pageSize)
      values.push(offset)
      const result = await pool.query(
        `SELECT ${cepImportRecusaSelectClause}
         FROM ${cepImportRecusaTableName}
         ${whereClause}
         ORDER BY data_importacao DESC, linha_xml ASC, id DESC
         LIMIT $${values.length - 1}
         OFFSET $${values.length}`,
        values,
      )

      const total = countResult.rows[0]?.total ?? 0

      sendJson(response, 200, {
        items: result.rows,
        total,
        page,
        pageSize,
        totalPages: Math.max(Math.ceil(total / pageSize), 1),
      })
    } catch (error) {
      sendJson(response, 500, { message: error instanceof Error ? error.message : 'Erro ao consultar recusas de importacao de CEP.' })
    }

    return
  }

  if (request.method === 'GET' && pathname === cepCollectionPath) {
    try {
      const page = Math.max(1, Number(requestUrl.searchParams.get('page') ?? 1) || 1)
      const pageSize = Math.min(50, Math.max(1, Number(requestUrl.searchParams.get('pageSize') ?? 20) || 20))
      const search = normalizeRequestValue(requestUrl.searchParams.get('search') ?? '')
      const sortBy = requestUrl.searchParams.get('sortBy') === 'municipio' ? 'municipio' : 'cep'
      const sortDirection = requestUrl.searchParams.get('sortDirection') === 'desc' ? 'DESC' : 'ASC'
      const params = []

      let whereClause = ''

      if (search) {
        params.push(`%${search.toUpperCase()}%`)
        whereClause = `WHERE UPPER(BTRIM(COALESCE(cep, ''))) LIKE $1
          OR UPPER(BTRIM(COALESCE(municipio, ''))) LIKE $1
          OR UPPER(BTRIM(COALESCE(logradouro, ''))) LIKE $1
          OR UPPER(BTRIM(COALESCE(bairro, ''))) LIKE $1
          OR UPPER(BTRIM(COALESCE(uf, ''))) LIKE $1`
      }

      const countResult = await pool.query(
        `SELECT COUNT(*)::int AS total FROM ${cepTableName} ${whereClause}`,
        params,
      )

      const total = countResult.rows[0]?.total ?? 0
      const totalPages = Math.max(1, Math.ceil(total / pageSize))
      const safePage = Math.min(page, totalPages)
      const offset = (safePage - 1) * pageSize

      const result = await pool.query(
        `SELECT ${cepSelectClause}
         FROM ${cepTableName}
         ${whereClause}
         ORDER BY ${sortBy} ${sortDirection}
         LIMIT $${params.length + 1} OFFSET $${params.length + 2}`,
        [...params, pageSize, offset],
      )

      sendJson(response, 200, {
        items: result.rows,
        total,
        page: safePage,
        pageSize,
        totalPages,
        sortBy,
        sortDirection: sortDirection.toLowerCase(),
      })
    } catch (error) {
      sendJson(response, 500, { message: error instanceof Error ? error.message : 'Erro ao consultar CEPs.' })
    }

    return
  }

  if (request.method === 'GET' && pathname === '/api/condutor/import-rejections') {
    try {
      const search = normalizeRequestValue(requestUrl.searchParams.get('search') ?? '')
      const page = Math.max(Number(requestUrl.searchParams.get('page') ?? 1) || 1, 1)
      const pageSize = Math.min(Math.max(Number(requestUrl.searchParams.get('pageSize') ?? 10) || 10, 1), 100)
      const offset = (page - 1) * pageSize
      const values = []
      const filters = []

      if (search) {
        values.push(`%${search}%`)
        filters.push(`(
          arquivo_xml ILIKE $${values.length}
          OR COALESCE(codigo_xml, '') ILIKE $${values.length}
          OR COALESCE(condutor_xml, '') ILIKE $${values.length}
          OR COALESCE(cpf_condutor_xml, '') ILIKE $${values.length}
          OR COALESCE(crmc_xml, '') ILIKE $${values.length}
          OR COALESCE(tipo_vinculo_xml, '') ILIKE $${values.length}
          OR motivo_recusa ILIKE $${values.length}
        )`)
      }

      const whereClause = filters.length ? `WHERE ${filters.join(' AND ')}` : ''
      const countResult = await pool.query(
        `SELECT COUNT(*)::int AS total FROM condutor_import_recusa ${whereClause}`,
        values,
      )

      values.push(pageSize)
      values.push(offset)
      const result = await pool.query(
        `SELECT
           ${condutorImportRecusaSelectClause}
         FROM condutor_import_recusa
         ${whereClause}
         ORDER BY data_importacao DESC, linha_xml ASC, id DESC
         LIMIT $${values.length - 1}
         OFFSET $${values.length}`,
        values,
      )
      const total = countResult.rows[0]?.total ?? 0

      sendJson(response, 200, {
        items: result.rows,
        total,
        page,
        pageSize,
        totalPages: Math.max(Math.ceil(total / pageSize), 1),
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao consultar recusas de importacao do condutor.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'GET' && pathname === '/api/credenciada') {
    try {
      const search = normalizeRequestValue(requestUrl.searchParams.get('search') ?? '')
      const page = Math.max(Number(requestUrl.searchParams.get('page') ?? 1) || 1, 1)
      const pageSize = Math.min(Math.max(Number(requestUrl.searchParams.get('pageSize') ?? 5) || 5, 1), 50)
      const sortBy = normalizeRequestValue(requestUrl.searchParams.get('sortBy') ?? 'codigo')
      const sortDirection = normalizeRequestValue(requestUrl.searchParams.get('sortDirection') ?? 'asc').toLowerCase() === 'desc'
        ? 'DESC'
        : 'ASC'
      const offset = (page - 1) * pageSize
      const values = []
      const filters = []
      const orderByClause = sortBy === 'credenciado'
        ? `UPPER(BTRIM(credenciado)) ${sortDirection}, codigo ASC`
        : `codigo ${sortDirection}`

      if (search) {
        values.push(`%${search}%`)
        filters.push(`(
          CAST(codigo AS text) ILIKE $${values.length}
          OR UPPER(BTRIM(credenciado)) ILIKE UPPER($${values.length})
          OR BTRIM(cnpj_cpf) ILIKE $${values.length}
          OR UPPER(COALESCE(BTRIM(representante), '')) ILIKE UPPER($${values.length})
          OR UPPER(COALESCE((SELECT BTRIM(c.municipio) FROM ceps c WHERE c.cep = BTRIM(credenciada.cep)), '')) ILIKE UPPER($${values.length})
        )`)
      }

      const whereClause = filters.length ? `WHERE ${filters.join(' AND ')}` : ''
      const countResult = await pool.query(
        `SELECT COUNT(*)::int AS total FROM credenciada ${whereClause}`,
        values,
      )

      values.push(pageSize)
      values.push(offset)
      const result = await pool.query(
        `SELECT
           ${credenciadaSelectClause}
         FROM credenciada
         ${whereClause}
         ORDER BY ${orderByClause}
         LIMIT $${values.length - 1}
         OFFSET $${values.length}`,
        values,
      )
      const total = countResult.rows[0]?.total ?? 0

      sendJson(response, 200, {
        items: result.rows,
        total,
        page,
        pageSize,
        totalPages: Math.max(Math.ceil(total / pageSize), 1),
        sortBy: sortBy === 'credenciado' ? 'credenciado' : 'codigo',
        sortDirection: sortDirection.toLowerCase(),
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao consultar credenciadas.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'GET' && pathname === '/api/credenciada/import-rejections') {
    try {
      const search = normalizeRequestValue(requestUrl.searchParams.get('search') ?? '')
      const page = Math.max(Number(requestUrl.searchParams.get('page') ?? 1) || 1, 1)
      const pageSize = Math.min(Math.max(Number(requestUrl.searchParams.get('pageSize') ?? 10) || 10, 1), 100)
      const offset = (page - 1) * pageSize
      const values = []
      const filters = []

      if (search) {
        values.push(`%${search}%`)
        filters.push(`(
          arquivo_xml ILIKE $${values.length}
          OR COALESCE(codigo_xml, '') ILIKE $${values.length}
          OR COALESCE(credenciado_xml, '') ILIKE $${values.length}
          OR COALESCE(cnpj_cpf_xml, '') ILIKE $${values.length}
          OR COALESCE(representante_xml, '') ILIKE $${values.length}
          OR COALESCE(status_xml, '') ILIKE $${values.length}
          OR motivo_recusa ILIKE $${values.length}
        )`)
      }

      const whereClause = filters.length ? `WHERE ${filters.join(' AND ')}` : ''
      const countResult = await pool.query(
        `SELECT COUNT(*)::int AS total FROM credenciada_import_recusa ${whereClause}`,
        values,
      )

      values.push(pageSize)
      values.push(offset)
      const result = await pool.query(
        `SELECT
           ${credenciadaImportRecusaSelectClause}
         FROM credenciada_import_recusa
         ${whereClause}
         ORDER BY data_importacao DESC, linha_xml ASC, id DESC
         LIMIT $${values.length - 1}
         OFFSET $${values.length}`,
        values,
      )
      const total = countResult.rows[0]?.total ?? 0

      sendJson(response, 200, {
        items: result.rows,
        total,
        page,
        pageSize,
        totalPages: Math.max(Math.ceil(total / pageSize), 1),
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao consultar recusas de importacao da credenciada.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'GET' && pathname === '/api/login-dre/options') {
    try {
      const loginsResult = await pool.query(
        'SELECT codigo::text AS codigo, BTRIM(nome) AS nome FROM login ORDER BY codigo ASC',
      )
      const dreResult = await pool.query(
        'SELECT CAST(codigo AS text) AS codigo, BTRIM(CAST(descricao AS text)) AS descricao FROM dre ORDER BY codigo ASC',
      )

      sendJson(response, 200, {
        loginOptions: loginsResult.rows,
        dreOptions: dreResult.rows,
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao carregar opcoes de login e DRE.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'GET' && pathname === '/api/login-dre') {
    try {
      const search = normalizeRequestValue(requestUrl.searchParams.get('search') ?? '')
      const page = Math.max(Number(requestUrl.searchParams.get('page') ?? 1) || 1, 1)
      const pageSize = Math.min(Math.max(Number(requestUrl.searchParams.get('pageSize') ?? 5) || 5, 1), 50)
      const sortBy = normalizeRequestValue(requestUrl.searchParams.get('sortBy') ?? 'login_codigo')
      const sortDirection = normalizeRequestValue(requestUrl.searchParams.get('sortDirection') ?? 'asc').toLowerCase() === 'desc'
        ? 'DESC'
        : 'ASC'
      const offset = (page - 1) * pageSize
      const values = []
      const filters = []
      const orderByClause = sortBy === 'login_nome'
        ? `UPPER(BTRIM(login_table.nome)) ${sortDirection}, relation.login_codigo ASC, relation.dre_codigo ASC`
        : sortBy === 'dre_descricao'
          ? `BTRIM(CAST(dre_table.descricao AS text)) ${sortDirection}, relation.login_codigo ASC, relation.dre_codigo ASC`
          : sortBy === 'dre_codigo'
            ? `relation.dre_codigo ${sortDirection}, relation.login_codigo ASC`
            : `relation.login_codigo ${sortDirection}, relation.dre_codigo ASC`

      if (search) {
        values.push(`%${search}%`)
        filters.push(`(
          CAST(relation.login_codigo AS text) ILIKE $${values.length}
          OR UPPER(BTRIM(login_table.nome)) ILIKE UPPER($${values.length})
          OR CAST(relation.dre_codigo AS text) ILIKE $${values.length}
          OR BTRIM(CAST(dre_table.descricao AS text)) ILIKE $${values.length}
        )`)
      }

      const whereClause = filters.length ? `WHERE ${filters.join(' AND ')}` : ''
      const fromClause = `
        FROM login_dre relation
        INNER JOIN login login_table ON login_table.codigo = relation.login_codigo
        INNER JOIN dre dre_table ON dre_table.codigo = relation.dre_codigo
      `

      const countResult = await pool.query(
        `SELECT COUNT(*)::int AS total ${fromClause} ${whereClause}`,
        values,
      )

      values.push(pageSize)
      values.push(offset)
      const result = await pool.query(
        `SELECT
           relation.login_codigo::text AS login_codigo,
           BTRIM(login_table.nome) AS login_nome,
           relation.dre_codigo::text AS dre_codigo,
           BTRIM(CAST(dre_table.descricao AS text)) AS dre_descricao
         ${fromClause}
         ${whereClause}
         ORDER BY ${orderByClause}
         LIMIT $${values.length - 1}
         OFFSET $${values.length}`,
        values,
      )
      const total = countResult.rows[0]?.total ?? 0

      sendJson(response, 200, {
        items: result.rows,
        total,
        page,
        pageSize,
        totalPages: Math.max(Math.ceil(total / pageSize), 1),
        sortBy: ['login_nome', 'dre_codigo', 'dre_descricao'].includes(sortBy) ? sortBy : 'login_codigo',
        sortDirection: sortDirection.toLowerCase(),
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao consultar login_dre.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'POST' && pathname === '/api/auth/login') {
    try {
      const body = await readJsonBody(request)
      const email = typeof body.email === 'string' ? body.email.trim() : ''
      const password = typeof body.password === 'string' ? body.password.trim() : ''

      if (!email || !password) {
        sendJson(response, 400, { message: 'Email e senha sao obrigatorios.' })
        return
      }

      const result = await pool.query(
        'SELECT codigo, nome, email, password, descricao FROM login WHERE LOWER(TRIM(email)) = LOWER($1) LIMIT 1',
        [email],
      )

      if (result.rowCount === 0) {
        sendJson(response, 401, { message: 'Usuario ou senha invalidos.' })
        return
      }

      const dbUser = result.rows[0]
      const passwordHash = normalizeDbValue(dbUser.descricao)
      const legacyPassword = normalizeDbValue(dbUser.password)

      if (!verifyPassword(password, passwordHash || legacyPassword)) {
        sendJson(response, 401, { message: 'Usuario ou senha invalidos.' })
        return
      }

      sendJson(response, 200, {
        token: createToken(email),
        user: {
          codigo: String(dbUser.codigo ?? ''),
          name: normalizeDbValue(dbUser.nome),
          email: normalizeDbValue(dbUser.email),
        },
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro interno ao autenticar.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'POST' && pathname === '/api/auth/register') {
    try {
      const body = await readJsonBody(request)
      const nome = normalizeRequestValue(body.nome)
      const email = normalizeRequestValue(body.email)
      const password = normalizeRequestValue(body.password)

      const result = await createAccess(nome, email, password)
      sendJson(response, result.status, result.payload)
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao cadastrar acesso.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'POST' && pathname === '/api/access') {
    try {
      const body = await readJsonBody(request)
      const nome = normalizeRequestValue(body.nome)
      const email = normalizeRequestValue(body.email)
      const password = normalizeRequestValue(body.password)

      const result = await createAccess(nome, email, password)
      sendJson(response, result.status, result.payload)
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao cadastrar acesso.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'POST' && pathname === '/api/dre') {
    try {
      const body = await readJsonBody(request)
      const sigla = normalizeDreSigla(body.sigla)
      const codigoOperacional = normalizeDreOperationalCode(body.codigoOperacional)
      const descricao = normalizeRequestValue(body.descricao)

      if (sigla.length !== 2) {
        sendJson(response, 400, { message: 'Sigla deve conter 2 letras maiusculas.' })
        return
      }

      if (!descricao) {
        sendJson(response, 400, { message: 'Descricao e obrigatoria.' })
        return
      }

      const duplicateDescriptionResult = await pool.query(
        'SELECT 1 FROM dre WHERE BTRIM(CAST(descricao AS text)) = $1 LIMIT 1',
        [descricao],
      )

      if (duplicateDescriptionResult.rowCount > 0) {
        sendJson(response, 409, { message: 'Descricao ja cadastrada.' })
        return
      }

      if (codigoOperacional) {
        const duplicateOperationalCodeResult = await pool.query(
          `SELECT 1
           FROM dre
           WHERE UPPER(BTRIM(COALESCE(codigo_operacional, ''))) = $1
           LIMIT 1`,
          [codigoOperacional],
        )

        if (duplicateOperationalCodeResult.rowCount > 0) {
          sendJson(response, 409, { message: 'Codigo operacional ja cadastrado.' })
          return
        }
      }

      const insertResult = await pool.query(
        `INSERT INTO dre (sigla, codigo_operacional, descricao)
         VALUES ($1, NULLIF($2, ''), $3)
         RETURNING ${dreSelectClause}`,
        [sigla, codigoOperacional, descricao],
      )

      sendJson(response, 201, {
        item: insertResult.rows[0],
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao gravar o registro dre.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'POST' && pathname === '/api/modalidade') {
    try {
      const body = await readJsonBody(request)
      const descricao = normalizeRequestValue(body.descricao)

      if (!descricao) {
        sendJson(response, 400, { message: 'Descricao e obrigatoria.' })
        return
      }

      const duplicateDescriptionResult = await pool.query(
        'SELECT 1 FROM modalidade WHERE UPPER(BTRIM(CAST(descricao AS text))) = UPPER($1) LIMIT 1',
        [descricao],
      )

      if (duplicateDescriptionResult.rowCount > 0) {
        sendJson(response, 409, { message: 'Descricao ja cadastrada.' })
        return
      }

      const insertResult = await pool.query(
        `INSERT INTO modalidade (descricao)
         VALUES ($1)
         RETURNING ${modalidadeSelectClause}`,
        [descricao],
      )

      sendJson(response, 201, {
        item: insertResult.rows[0],
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao gravar o registro modalidade.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'POST' && pathname === '/api/troca') {
    try {
      const body = await readJsonBody(request)
      const validationResult = await validateTrocaPayload({
        controle: body.controle,
        lista: body.lista,
      })

      if (validationResult.status !== 200) {
        sendJson(response, validationResult.status, validationResult.payload)
        return
      }

      const insertResult = await pool.query(
        `INSERT INTO tipo_troca (
           controle,
           lista,
           data_inclusao,
           data_modificacao
         )
         VALUES ($1, $2, NOW(), NOW())
         RETURNING ${trocaSelectClause}`,
        [
          validationResult.payload.controle,
          validationResult.payload.lista,
        ],
      )

      sendJson(response, 201, {
        item: insertResult.rows[0],
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao cadastrar troca.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'POST' && pathname === emissaoDocumentoParametroCollectionPath) {
    try {
      const body = await readJsonBody(request)
      const validationResult = await validateEmissaoDocumentoParametroPayload({
        dataReferencia: body.dataReferencia,
        objeto: body.objeto,
        objetoLicitacao: body.objetoLicitacao,
        credenciante: body.credenciante,
        tituloAditivo: body.tituloAditivo,
        termoSmt: body.termoSmt,
        descricaoAditivo: body.descricaoAditivo,
        corpoAditivo: body.corpoAditivo,
        assinaturasAditivo: body.assinaturasAditivo,
        descricaoContratoPf: body.descricaoContratoPf,
        descricaoContratoPj: body.descricaoContratoPj,
        corpoContratoPf: body.corpoContratoPf,
        corpoContratoPj: body.corpoContratoPj,
        linkModeloRelatorioContratoPf: body.linkModeloRelatorioContratoPf,
        linkModeloRelatorioContratoPj: body.linkModeloRelatorioContratoPj,
        textoDespacho: body.textoDespacho,
        editalChamamentoPublico: body.editalChamamentoPublico,
        obs01Emissao: body.obs01Emissao,
        obs02Emissao: body.obs02Emissao,
        rodapeEmissao: body.rodapeEmissao,
        prefeituraImagem: body.prefeituraImagem,
        tituloEmissao: body.tituloEmissao,
        diretorEmissao: body.diretorEmissao,
      })

      if (validationResult.status !== 200) {
        sendJson(response, validationResult.status, validationResult.payload)
        return
      }

      const insertResult = await pool.query(
        `INSERT INTO ${emissaoDocumentoParametroTableName} (
           data_referencia,
           objeto,
            objeto_licitacao,
            credenciante,
            titulo_aditivo,
            termo_smt,
            descricao_aditivo,
            corpo_aditivo,
            assinaturas_aditivo,
              descricao_contrato_pf,
              descricao_contrato_pj,
              corpo_contrato_pf,
              corpo_contrato_pj,
              link_modelo_relatorio_contrato_pf,
              link_modelo_relatorio_contrato_pj,
            texto_despacho,
           edital_chamamento_publico,
           obs_01_emissao,
           obs_02_emissao,
           rodape_emissao,
           prefeitura_imagem,
           titulo_emissao,
           diretor_emissao,
           data_inclusao,
           data_modificacao
         )
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, NOW(), NOW())
         RETURNING ${emissaoDocumentoParametroSelectClause}`,
        [
          validationResult.payload.dataReferencia,
          validationResult.payload.objeto,
          validationResult.payload.objetoLicitacao,
          validationResult.payload.credenciante,
          validationResult.payload.tituloAditivo,
          validationResult.payload.termoSmt,
          validationResult.payload.descricaoAditivo,
          validationResult.payload.corpoAditivo,
          validationResult.payload.assinaturasAditivo,
          validationResult.payload.descricaoContratoPf,
          validationResult.payload.descricaoContratoPj,
          validationResult.payload.corpoContratoPf,
          validationResult.payload.corpoContratoPj,
          validationResult.payload.linkModeloRelatorioContratoPf,
          validationResult.payload.linkModeloRelatorioContratoPj,
          validationResult.payload.textoDespacho,
          validationResult.payload.editalChamamentoPublico,
          validationResult.payload.obs01Emissao,
          validationResult.payload.obs02Emissao,
          validationResult.payload.rodapeEmissao,
          validationResult.payload.prefeituraImagem,
          validationResult.payload.tituloEmissao,
          validationResult.payload.diretorEmissao,
        ],
      )

      sendJson(response, 201, { item: insertResult.rows[0] })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao cadastrar parametro de emissao.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'POST' && pathname === '/api/marca-modelo') {
    try {
      const body = await readJsonBody(request)
      const descricao = normalizeTrocaText(body.descricao, 255)

      if (!descricao) {
        sendJson(response, 400, { message: 'Descricao e obrigatoria.' })
        return
      }

      const duplicateDescriptionResult = await pool.query(
        'SELECT 1 FROM marca_modelo WHERE BTRIM(descricao) = $1 LIMIT 1',
        [descricao],
      )

      if (duplicateDescriptionResult.rowCount > 0) {
        sendJson(response, 409, { message: 'Descricao ja cadastrada.' })
        return
      }

      const insertResult = await pool.query(
        'INSERT INTO marca_modelo (descricao, data_inclusao, data_modificacao) VALUES ($1, NOW(), NOW()) RETURNING CAST(codigo AS text) AS codigo, BTRIM(descricao) AS descricao',
        [descricao],
      )

      sendJson(response, 201, {
        item: insertResult.rows[0],
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao gravar o registro marca/modelo.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'POST' && pathname === '/api/marca-modelo/import-xml') {
    try {
      const body = await readJsonBody(request)
      const importResult = await importMarcaModeloXmlFile(body.fileName)

      sendJson(response, 200, importResult)
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao importar XML de marca/modelo.'

      sendJson(response, 400, { message })
    }

    return
  }

  if (request.method === 'POST' && pathname === '/api/seguradora') {
    try {
      const body = await readJsonBody(request)
      const validationResult = await validateSeguradoraPayload({
        controle: body.controle,
        descricao: body.descricao ?? body.lista,
      })

      if (validationResult.status !== 200) {
        sendJson(response, validationResult.status, validationResult.payload)
        return
      }

      const insertResult = await pool.query(
        `INSERT INTO seguradora (
           controle,
           lista,
           data_inclusao,
           data_modificacao
         )
         VALUES ($1, $2, NOW(), NOW())
         RETURNING ${seguradoraSelectClause}`,
        [
          validationResult.payload.controle,
          validationResult.payload.descricao,
        ],
      )

      sendJson(response, 201, {
        item: insertResult.rows[0],
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao cadastrar seguradora.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'POST' && pathname === '/api/login-dre') {
    try {
      const body = await readJsonBody(request)
      const validationResult = await validateLoginDrePayload({
        loginCodigo: body.loginCodigo,
        dreCodigo: body.dreCodigo,
      })

      if (validationResult.status !== 200) {
        sendJson(response, validationResult.status, validationResult.payload)
        return
      }

      const { loginCodigo, dreCodigo } = validationResult.payload
      const insertResult = await pool.query(
        `INSERT INTO login_dre (login_codigo, dre_codigo)
         VALUES ($1, $2)
         RETURNING login_codigo::text AS login_codigo, dre_codigo::text AS dre_codigo`,
        [loginCodigo, dreCodigo],
      )

      sendJson(response, 201, {
        item: {
          ...insertResult.rows[0],
          login_nome: validationResult.payload.loginNome,
          dre_descricao: validationResult.payload.dreDescricao,
        },
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao cadastrar login_dre.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'POST' && pathname === '/api/login-dre/assign-all') {
    try {
      const body = await readJsonBody(request)
      const validationResult = await validateLoginCodigo(body.loginCodigo)

      if (validationResult.status !== 200) {
        sendJson(response, validationResult.status, validationResult.payload)
        return
      }

      const { loginCodigo, loginNome } = validationResult.payload
      const totalsResult = await pool.query(
        `SELECT
           (SELECT COUNT(*)::int FROM dre) AS total_dres,
           (SELECT COUNT(*)::int FROM login_dre WHERE login_codigo = $1) AS existing_dres`,
        [loginCodigo],
      )

      const insertResult = await pool.query(
        `INSERT INTO login_dre (login_codigo, dre_codigo)
         SELECT $1, dre_table.codigo
         FROM dre dre_table
         WHERE NOT EXISTS (
           SELECT 1
           FROM login_dre relation
           WHERE relation.login_codigo = $1
             AND relation.dre_codigo = dre_table.codigo
         )
         RETURNING dre_codigo::text AS dre_codigo`,
        [loginCodigo],
      )

      const totals = totalsResult.rows[0] ?? { total_dres: 0, existing_dres: 0 }
      const insertedCount = insertResult.rowCount ?? 0
      const totalDres = totals.total_dres ?? 0
      const existingCount = totals.existing_dres ?? 0

      sendJson(response, 201, {
        message: insertedCount > 0
          ? `${insertedCount} DRE(s) vinculada(s) ao usuario ${loginNome}.`
          : `Nenhuma nova DRE foi vinculada ao usuario ${loginNome}.`,
        item: {
          login_codigo: String(loginCodigo),
          login_nome: loginNome,
          total_dres: totalDres,
          existing_dres: existingCount,
          inserted_dres: insertedCount,
        },
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao vincular DREs ao usuario.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'POST' && pathname === '/api/condutor') {
    try {
      const body = await readJsonBody(request)
      const client = await pool.connect()

      try {
        await client.query('BEGIN')
        await client.query('LOCK TABLE condutor IN EXCLUSIVE MODE')
        const nextCodigo = await getNextCondutorCodigo(client)
        const validationResult = await validateCondutorPayload({
          codigo: nextCodigo,
          condutor: body.condutor,
          cpfCondutor: body.cpfCondutor,
          crmc: body.crmc,
          validadeCrmc: body.validadeCrmc,
          validadeCurso: body.validadeCurso,
          tipoVinculo: body.tipoVinculo,
          historico: body.historico,
        })

        if (validationResult.status !== 200) {
          await client.query('ROLLBACK')
          sendJson(response, validationResult.status, validationResult.payload)
          return
        }

        const insertResult = await client.query(
          `INSERT INTO condutor (
             codigo,
             condutor,
             cpf_condutor,
             crmc,
             validade_crmc,
             validade_curso,
             tipo_vinculo,
             historico,
             data_inclusao,
             data_modificacao
           )
           VALUES ($1, $2, $3, $4, $5::date, $6::date, NULLIF($7, ''), NULLIF($8, ''), NOW(), NOW())
           RETURNING ${condutorSelectClause}`,
          [
            validationResult.payload.codigo,
            validationResult.payload.condutor,
            validationResult.payload.cpfCondutor,
            validationResult.payload.crmc,
            validationResult.payload.validadeCrmc,
            validationResult.payload.validadeCurso,
            validationResult.payload.tipoVinculo,
            validationResult.payload.historico,
          ],
        )

        await client.query('COMMIT')

        sendJson(response, 201, {
          item: insertResult.rows[0],
        })
      } catch (error) {
        await client.query('ROLLBACK')
        throw error
      } finally {
        client.release()
      }
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao cadastrar condutor.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'POST' && pathname === '/api/condutor/import-xml') {
    try {
      const body = await readJsonBody(request)
      const result = await importCondutorXmlFile(body.fileName)

      sendJson(response, 200, {
        message: 'Importacao de condutores concluida com sucesso.',
        ...result,
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao importar condutores do XML.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'POST' && pathname === smokeRunPath) {
    try {
      const body = await readJsonBody(request)
      const result = await runSmokeSuiteCommand(body.suite)
      const statusCode = result.status === 'passed' ? 200 : 500

      sendJson(response, statusCode, {
        message: result.status === 'passed'
          ? `Smoke ${result.suite === 'all' ? 'completo da aplicacao' : `da suite ${result.suite}`} executado com sucesso.`
          : `Smoke ${result.suite === 'all' ? 'completo da aplicacao' : `da suite ${result.suite}`} finalizado com falhas.`,
        ...result,
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao executar smoke da aplicacao.'

      sendJson(response, 500, { message, status: 'failed' })
    }

    return
  }

  if (request.method === 'POST' && pathname === '/api/monitor') {
    try {
      const body = await readJsonBody(request)
      const client = await pool.connect()

      try {
        await client.query('BEGIN')
        await client.query('LOCK TABLE monitor IN EXCLUSIVE MODE')
        const nextCodigo = await getNextMonitorCodigo(client)
      const validationResult = await validateMonitorPayload({
        codigo: nextCodigo,
        monitor: body.monitor,
        rgMonitor: body.rgMonitor,
        cpfMonitor: body.cpfMonitor,
        cursoMonitor: body.cursoMonitor,
        validadeCurso: body.validadeCurso,
        tipoVinculo: body.tipoVinculo,
        nascimento: body.nascimento,
      })

      if (validationResult.status !== 200) {
        await client.query('ROLLBACK')
        sendJson(response, validationResult.status, validationResult.payload)
        return
      }

      const insertResult = await client.query(
        `INSERT INTO monitor (
           codigo,
           monitor,
           rg_monitor,
           cpf_monitor,
           curso_monitor,
           validade_curso,
           tipo_vinculo,
           nascimento,
           data_inclusao,
           data_modificacao
         )
         VALUES ($1, $2, NULLIF($3, ''), $4, NULLIF($5, '')::date, NULLIF($6, '')::date, NULLIF($7, ''), NULLIF($8, '')::date, NOW(), NOW())
         RETURNING ${monitorSelectClause}`,
        [
          validationResult.payload.codigo,
          validationResult.payload.monitor,
          validationResult.payload.rgMonitor,
          validationResult.payload.cpfMonitor,
          validationResult.payload.cursoMonitor,
          validationResult.payload.validadeCurso,
          validationResult.payload.tipoVinculo,
          validationResult.payload.nascimento,
        ],
      )

        await client.query('COMMIT')

      sendJson(response, 201, {
        item: insertResult.rows[0],
      })
      } catch (error) {
        await client.query('ROLLBACK')
        throw error
      } finally {
        client.release()
      }
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao cadastrar monitor.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'POST' && pathname === '/api/monitor/import-xml') {
    try {
      const body = await readJsonBody(request)
      const result = await importMonitorXmlFile(body.fileName)

      sendJson(response, 200, {
        message: 'Importacao de monitores concluida com sucesso.',
        ...result,
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao importar monitores do XML.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'POST' && pathname === '/api/veiculo') {
    try {
      const body = await readJsonBody(request)
      const validationResult = await validateVeiculoPayload({
        codigo: null,
        crm: body.crm,
        placas: body.placas,
        ano: body.ano,
        capDetran: body.capDetran,
        capTeg: body.capTeg,
        capTegCreche: body.capTegCreche,
        capAcessivel: body.capAcessivel,
        valCrm: body.valCrm,
        seguradora: body.seguradora,
        seguroInicio: body.seguroInicio,
        seguroTermino: body.seguroTermino,
        tipoDeBancada: body.tipoDeBancada,
        tipoDeVeiculo: body.tipoDeVeiculo,
        marcaModelo: body.marcaModelo,
        titular: body.titular,
        cnpjCpf: body.cnpjCpf,
        valorVeiculo: body.valorVeiculo,
        osEspecial: body.osEspecial,
      })

      if (validationResult.status !== 200) {
        sendJson(response, validationResult.status, validationResult.payload)
        return
      }

      const insertResult = await pool.query(
        `INSERT INTO veiculo (
           crm,
           placas,
           ano,
           cap_detran,
           cap_teg,
           cap_teg_creche,
           cap_acessivel,
           val_crm,
           seguradora,
           seguro_inicio,
           seguro_termino,
           tipo_de_bancada,
           tipo_de_veiculo,
           marca_modelo,
           titular,
           cnpj_cpf,
           valor_veiculo,
           os_especial,
           data_inclusao,
           data_modificacao
         )
         VALUES (NULLIF($1, ''), NULLIF($2, ''), $3, $4, $5, $6, $7, NULLIF($8, '')::date, NULLIF($9, ''), NULLIF($10, '')::date, NULLIF($11, '')::date, NULLIF($12, ''), NULLIF($13, ''), NULLIF($14, ''), NULLIF($15, ''), NULLIF($16, ''), $17, NULLIF($18, ''), NOW(), NOW())
         RETURNING ${veiculoSelectClause}`,
        [
          validationResult.payload.crm,
          validationResult.payload.placas,
          validationResult.payload.ano,
          validationResult.payload.capDetran,
          validationResult.payload.capTeg,
          validationResult.payload.capTegCreche,
          validationResult.payload.capAcessivel,
          validationResult.payload.valCrm,
          validationResult.payload.seguradora,
          validationResult.payload.seguroInicio,
          validationResult.payload.seguroTermino,
          validationResult.payload.tipoDeBancada,
          validationResult.payload.tipoDeVeiculo,
          validationResult.payload.marcaModelo,
          validationResult.payload.titular,
          validationResult.payload.cnpjCpf,
          validationResult.payload.valorVeiculo,
          validationResult.payload.osEspecial,
        ],
      )

      sendJson(response, 201, {
        item: insertResult.rows[0],
      })
    } catch (error) {
      const persistenceError = getVeiculoPersistenceError(error, 'Erro ao cadastrar veiculo.')

      sendJson(response, persistenceError.status, { message: persistenceError.message })
    }

    return
  }

  if (request.method === 'POST' && pathname === credenciamentoTermoCollectionPath) {
    try {
      const body = await readJsonBody(request)
      const validationResult = await validateCredenciamentoTermoPayload(body, pool, { requireAditivo: false })

      if (validationResult.status !== 200) {
        sendJson(response, validationResult.status, validationResult.payload)
        return
      }

      const client = await pool.connect()

      try {
        await client.query('BEGIN')
        await client.query('SELECT pg_advisory_xact_lock(hashtext($1))', [validationResult.payload.termoAdesao])
        const latestTermo = await findLatestCredenciamentoTermoByTermoAdesao(
          validationResult.payload.termoAdesao,
          client,
          { forUpdate: true },
        )
        const latestTermoItem = latestTermo
          ? await fetchCredenciamentoTermoItemByCodigo(client, latestTermo.codigo)
          : null
        const createPayload = buildCredenciamentoTermoCreatePayload(validationResult.payload, latestTermoItem)
        let nextAditivo = 0

        if (latestTermo) {
          nextAditivo = Number.isInteger(Number(latestTermo.aditivo))
            ? Number(latestTermo.aditivo) + 1
            : 1

          await client.query(
            `UPDATE ${credenciamentoTermoTableName}
               SET termino_vigencia = (CURRENT_DATE - INTERVAL '1 day')::date,
                   status_aditivo = 'PUBLICAR',
                   data_modificacao = NOW()
             WHERE codigo = $1`,
            [latestTermo.codigo],
          )
        }

        const insertResult = await client.query(
          `INSERT INTO ${credenciamentoTermoTableName} (
             codigo_xml,
             credenciada_codigo,
             termo_adesao,
             sei,
             aditivo,
             situacao_publicacao,
             situacao_emissao,
             inicio_vigencia,
             termino_vigencia,
             comp_data_aditivo,
             status_aditivo,
             data_pub_aditivo,
             check_aditivo,
             status_termo,
             tipo_termo,
             especificacao_sei,
             valor_contrato,
             data_publicacao,
             valor_contrato_atualizado,
             vencimento_geral,
             mes_renovacao,
             tp_optante,
             data_inclusao,
             data_modificacao
           )
           VALUES ($1, $2, $3, NULLIF($4, ''), $5, NULLIF($6, ''), NULLIF($7, ''), NULLIF($8, '')::date, NULLIF($9, '')::date, NULLIF($10, '')::date, NULLIF($11, ''), NULLIF($12, '')::date, $13, NULLIF($14, ''), NULLIF($15, ''), NULLIF($16, ''), $17, NULLIF($18, '')::date, $19, NULLIF($20, '')::date, NULLIF($21, ''), NULLIF($22, ''), NOW(), NOW())
           RETURNING codigo`,
          [
              createPayload.codigoXml,
              createPayload.credenciadaCodigo,
              createPayload.termoAdesao,
              createPayload.sei,
            nextAditivo,
              createPayload.situacaoPublicacao,
              createPayload.situacaoEmissao,
              createPayload.inicioVigencia,
              createPayload.terminoVigencia,
              createPayload.compDataAditivo,
              createPayload.statusAditivo,
              createPayload.dataPubAditivo,
              createPayload.checkAditivo,
              createPayload.statusTermo,
              createPayload.tipoTermo,
              createPayload.especificacaoSei,
              createPayload.valorContrato,
              createPayload.dataPublicacao,
              createPayload.valorContratoAtualizado,
              createPayload.vencimentoGeral,
              createPayload.mesRenovacao,
              createPayload.tpOptante,
          ],
        )

        await client.query('COMMIT')

        const item = await fetchCredenciamentoTermoItemByCodigo(pool, insertResult.rows[0].codigo)
        sendJson(response, 201, { item })
      } catch (error) {
        await client.query('ROLLBACK')
        throw error
      } finally {
        client.release()
      }
    } catch (error) {
      const persistenceError = getCredenciamentoTermoPersistenceError(error, 'Erro ao cadastrar credenciamento termo.')
      sendJson(response, persistenceError.status, { message: persistenceError.message })
    }

    return
  }

  if (request.method === 'POST' && pathname === credenciamentoTermoImportXmlPath) {
    try {
      const body = await readJsonBody(request)
      const result = await importCredenciamentoTermoXmlFile(body.fileName)

      sendJson(response, 200, {
        message: 'Importacao de credenciamento termo concluida com sucesso.',
        ...result,
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao importar credenciamento termo do XML.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'POST' && pathname === '/api/veiculo/import-xml') {
    try {
      const body = await readJsonBody(request)
      const result = await importVeiculoXmlFile(body.fileName)

      sendJson(response, 200, {
        message: 'Importacao de veiculos concluida com sucesso.',
        ...result,
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao importar veiculos do XML.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'POST' && pathname === vinculoCondutorCollectionPath) {
    try {
      const body = await readJsonBody(request)
      const validationResult = await validateVinculoCondutorPayload({
        termoAdesao: body.termoAdesao,
        numOs: body.numOs,
        revisao: body.revisao,
        credenciadaCodigo: body.credenciadaCodigo,
        credenciado: body.credenciado,
        dataOs: body.dataOs,
        dataAdmissaoCondutor: body.dataAdmissaoCondutor,
        condutorCodigo: body.condutorCodigo,
        cpfCondutor: body.cpfCondutor,
      })

      if (validationResult.status !== 200) {
        sendJson(response, validationResult.status, validationResult.payload)
        return
      }

      const insertResult = await pool.query(
        `INSERT INTO ${vinculoCondutorTableName} (
           termo_adesao,
           num_os,
           revisao,
           credenciada_codigo,
           data_os,
           data_admissao_condutor,
           condutor_codigo,
           data_inclusao
         )
         VALUES (NULLIF($1, ''), NULLIF($2, ''), NULLIF($3, ''), $4, NULLIF($5, '')::date, NULLIF($6, '')::date, $7, NOW())
         RETURNING id`,
        [
          validationResult.payload.termoAdesao,
          validationResult.payload.numOs,
          validationResult.payload.revisao,
          validationResult.payload.credenciadaCodigo,
          validationResult.payload.dataOs,
          validationResult.payload.dataAdmissaoCondutor,
          validationResult.payload.condutorCodigo,
        ],
      )

      const item = await fetchVinculoCondutorItemById(pool, insertResult.rows[0].id)
      sendJson(response, 201, { item })
    } catch (error) {
      const persistenceError = getVinculoCondutorPersistenceError(error, 'Erro ao cadastrar vinculo do condutor.')
      sendJson(response, persistenceError.status, { message: persistenceError.message })
    }

    return
  }

  if (request.method === 'POST' && pathname === vinculoCondutorImportXmlPath) {
    try {
      const body = await readJsonBody(request)
      const result = await importVinculoCondutorXmlFile(body.fileName)

      sendJson(response, 200, {
        message: 'Importacao de vinculos do condutor concluida com sucesso.',
        ...result,
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao importar vinculos do condutor do XML.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'POST' && pathname === vinculoMonitorCollectionPath) {
    try {
      const body = await readJsonBody(request)
      const validationResult = await validateVinculoMonitorPayload({
        termoAdesao: body.termoAdesao,
        numOs: body.numOs,
        revisao: body.revisao,
        credenciadaCodigo: body.credenciadaCodigo,
        credenciado: body.credenciado,
        dataOs: body.dataOs,
        dataAdmissaoMonitor: body.dataAdmissaoMonitor,
        monitorCodigo: body.monitorCodigo,
        cpfMonitor: body.cpfMonitor,
      })

      if (validationResult.status !== 200) {
        sendJson(response, validationResult.status, validationResult.payload)
        return
      }

      const insertResult = await pool.query(
        `INSERT INTO ${vinculoMonitorTableName} (
           termo_adesao,
           num_os,
           revisao,
           credenciada_codigo,
           data_os,
           data_admissao_monitor,
           monitor_codigo,
           data_inclusao
         )
         VALUES (NULLIF($1, ''), NULLIF($2, ''), NULLIF($3, ''), $4, NULLIF($5, '')::date, NULLIF($6, '')::date, $7, NOW())
         RETURNING id`,
        [
          validationResult.payload.termoAdesao,
          validationResult.payload.numOs,
          validationResult.payload.revisao,
          validationResult.payload.credenciadaCodigo,
          validationResult.payload.dataOs,
          validationResult.payload.dataAdmissaoMonitor,
          validationResult.payload.monitorCodigo,
        ],
      )

      const item = await fetchVinculoMonitorItemById(pool, insertResult.rows[0].id)
      sendJson(response, 201, { item })
    } catch (error) {
      const persistenceError = getVinculoMonitorPersistenceError(error, 'Erro ao cadastrar vinculo do monitor.')
      sendJson(response, persistenceError.status, { message: persistenceError.message })
    }

    return
  }

  if (request.method === 'POST' && pathname === vinculoMonitorImportXmlPath) {
    try {
      const body = await readJsonBody(request)
      const result = await importVinculoMonitorXmlFile(body.fileName)

      sendJson(response, 200, {
        message: 'Importacao de vinculos do monitor concluida com sucesso.',
        ...result,
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao importar vinculos do monitor do XML.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'POST' && pathname === cepImportXmlPath) {
    try {
      const body = await readJsonBody(request)
      const result = await importCepsXmlFile(body.fileName)

      sendJson(response, 200, {
        message: 'Importacao de CEPs concluida com sucesso.',
        ...result,
      })
    } catch (error) {
      sendJson(response, 500, { message: error instanceof Error ? error.message : 'Erro ao importar CEPs do XML.' })
    }

    return
  }

  if (request.method === 'POST' && pathname === cepCollectionPath) {
    try {
      const body = await readJsonBody(request)
      const validationResult = await validateCepPayload(body)

      if (validationResult.status !== 200) {
        sendJson(response, validationResult.status, validationResult.payload)
        return
      }

      const { payload } = validationResult

      const result = await pool.query(
        `INSERT INTO ${cepTableName} (
           cep, logradouro, complemento, bairro, municipio, uf, ibge, data_inclusao, data_modificacao
         )
         VALUES ($1, NULLIF($2, ''), NULLIF($3, ''), NULLIF($4, ''), $5, $6, NULLIF($7, ''), NOW(), NOW())
         RETURNING ${cepSelectClause}`,
        [payload.cep, payload.logradouro, payload.complemento, payload.bairro, payload.municipio, payload.uf, payload.ibge],
      )

      sendJson(response, 201, { item: result.rows[0] })
    } catch (error) {
      sendJson(response, 500, { message: error instanceof Error ? error.message : 'Erro ao cadastrar CEP.' })
    }

    return
  }

  if (request.method === 'POST' && pathname === ordemServicoCollectionPath) {
    try {
      const body = await readJsonBody(request)
      const substitutionSourceCodigo = normalizeCondutorCodigo(body.substitutionSourceCodigo)
      const activationSourceCodigo = normalizeCondutorCodigo(body.activationSourceCodigo)
      const validationResult = await validateOrdemServicoPayload({
        codigoAccess: body.codigoAccess,
        termoAdesao: body.termoAdesao,
        numOs: body.numOs,
        revisao: body.revisao,
        vigenciaOs: body.vigenciaOs,
        credenciado: body.credenciado,
        cnpjCpf: body.cnpjCpf,
        dreCodigo: body.dreCodigo,
        modalidadeDescricao: body.modalidadeDescricao,
        cpfCondutor: body.cpfCondutor,
        dataAdmissaoCondutor: body.dataAdmissaoCondutor,
        cpfPreposto: body.cpfPreposto,
        prepostoInicio: body.prepostoInicio,
        prepostoDias: body.prepostoDias,
        crm: body.crm,
        cpfMonitor: body.cpfMonitor,
        dataAdmissaoMonitor: body.dataAdmissaoMonitor,
        situacao: body.situacao,
        tipoTroca: body.tipoTroca,
        conexao: body.conexao,
        dataEncerramento: body.dataEncerramento,
        anotacao: body.anotacao,
        uniaoTermos: body.uniaoTermos,
        substitutionSourceCodigo,
        requireCodigo: false,
      })

      if (validationResult.status !== 200) {
        sendJson(response, validationResult.status, validationResult.payload)
        return
      }

      const client = await pool.connect()

      try {
        await client.query('BEGIN')

        const insertResult = await client.query(
          `INSERT INTO ${ordemServicoTableName} (
             codigo_access,
             termo_adesao,
             num_os,
             revisao,
             os_concat,
             vigencia_os,
             termo_codigo,
             dre_codigo,
             dre_descricao,
             modalidade_codigo,
             modalidade_descricao,
             cpf_condutor,
             condutor,
             data_admissao_condutor,
             cpf_preposto,
             preposto_condutor,
             preposto_inicio,
             preposto_dias,
             crm,
             veiculo_placas,
             cpf_monitor,
             monitor,
             data_admissao_monitor,
             situacao,
             tipo_troca_codigo,
             tipo_troca_descricao,
             conexao,
             data_encerramento,
             anotacao,
             uniao_termos,
             data_inclusao,
             data_modificacao
           )
           VALUES (NULLIF($1, ''), NULLIF($2, ''), NULLIF($3, ''), NULLIF($4, ''), NULLIF($5, ''), NULLIF($6, '')::date, $7, $8, $9, $10, NULLIF($11, ''), $12, $13, NULLIF($14, '')::date, NULLIF($15, ''), NULLIF($16, ''), NULLIF($17, '')::date, $18, $19, NULLIF($20, ''), NULLIF($21, ''), NULLIF($22, ''), NULLIF($23, '')::date, $24, $25, NULLIF($26, ''), NULLIF($27, ''), NULLIF($28, '')::date, NULLIF($29, ''), NULLIF($30, ''), NOW(), NOW())
           RETURNING codigo`,
          [
            validationResult.payload.codigoAccess,
            validationResult.payload.termoAdesao,
            validationResult.payload.numOs,
            validationResult.payload.revisao,
            validationResult.payload.osConcat,
            validationResult.payload.vigenciaOs,
            validationResult.payload.termoCodigo,
            validationResult.payload.dreCodigo,
            validationResult.payload.dreDescricao,
            validationResult.payload.modalidadeCodigo,
            validationResult.payload.modalidadeDescricao,
            validationResult.payload.cpfCondutor,
            validationResult.payload.condutor,
            validationResult.payload.dataAdmissaoCondutor,
            validationResult.payload.cpfPreposto,
            validationResult.payload.prepostoCondutor,
            validationResult.payload.prepostoInicio,
            validationResult.payload.prepostoDias,
            validationResult.payload.crm,
            validationResult.payload.veiculoPlacas,
            validationResult.payload.cpfMonitor,
            validationResult.payload.monitor,
            validationResult.payload.dataAdmissaoMonitor,
            validationResult.payload.situacao,
            validationResult.payload.tipoTrocaCodigo,
            validationResult.payload.tipoTrocaDescricao,
            validationResult.payload.conexao,
            validationResult.payload.dataEncerramento,
            validationResult.payload.anotacao,
            validationResult.payload.uniaoTermos,
          ],
        )

        const insertedCodigo = Number(insertResult.rows[0].codigo)
        if (activationSourceCodigo === null || Number.isNaN(activationSourceCodigo)) {
          let previousCodigo = substitutionSourceCodigo

          if (previousCodigo === null || Number.isNaN(previousCodigo)) {
            const previousCodigoResult = await client.query(
              `SELECT codigo
               FROM ${ordemServicoTableName}
               WHERE termo_adesao = $1
                 AND num_os = $2
                 AND codigo <> $3
               ORDER BY data_modificacao DESC NULLS LAST, data_inclusao DESC NULLS LAST, codigo DESC
               LIMIT 1`,
              [validationResult.payload.termoAdesao, validationResult.payload.numOs, insertedCodigo],
            )

            previousCodigo = previousCodigoResult.rows[0]?.codigo ?? null
          }

          if (previousCodigo !== null && !Number.isNaN(Number(previousCodigo))) {
            await client.query(
              `UPDATE ${ordemServicoTableName}
               SET data_encerramento = (CURRENT_DATE - INTERVAL '1 day')::date,
                   situacao = 'Substituido',
                   data_modificacao = NOW()
               WHERE codigo = $1`,
              [Number(previousCodigo)],
            )
          }
        }

        await rebalanceOrdemServicoRevisions(client)
        await syncCondutorVinculosFromOrdemServico(client)
        await syncMonitorVinculosFromOrdemServico(client)

        const item = await fetchOrdemServicoItemByCodigo(client, insertedCodigo)

        await client.query('COMMIT')
        sendJson(response, 201, { item })
      } catch (error) {
        await client.query('ROLLBACK')
        throw error
      } finally {
        client.release()
      }
    } catch (error) {
      const persistenceError = getOrdemServicoPersistenceError(error, 'Erro ao cadastrar OrdemServico.')
      sendJson(response, persistenceError.status, { message: persistenceError.message })
    }

    return
  }

  if (request.method === 'POST' && pathname === ordemServicoImportXmlPath) {
    try {
      const body = await readJsonBody(request)
      const result = await importOrdemServicoXmlFile(body.fileName)

      sendJson(response, 200, {
        message: 'Importacao de OrdemServico concluida com sucesso.',
        ...result,
      })
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Erro ao importar OrdemServico do XML.'
      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'POST' && pathname === '/api/titular') {
    try {
      const body = await readJsonBody(request)
      const validationResult = await validateTitularPayload({
        cnpjCpf: body.cnpjCpf,
        titular: body.titular,
      })

      if (validationResult.status !== 200) {
        sendJson(response, validationResult.status, validationResult.payload)
        return
      }

      const insertResult = await pool.query(
        `INSERT INTO ${titularTableName} (
           cnpj_cpf,
           titular,
           data_inclusao,
           data_modificacao
         )
         VALUES ($1, $2, NOW(), NOW())
         RETURNING ${titularSelectClause}`,
        [
          validationResult.payload.cnpjCpf,
          validationResult.payload.titular,
        ],
      )

      sendJson(response, 201, {
        item: insertResult.rows[0],
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao cadastrar titular do CRM.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'POST' && pathname === '/api/credenciada') {
    try {
      const body = await readJsonBody(request)
      const nextCodigoResult = await pool.query("SELECT nextval('credenciada_codigo_seq') AS codigo")
      const generatedCodigo = Number(nextCodigoResult.rows[0]?.codigo)
      const validationResult = await validateCredenciadaPayload({
        codigo: generatedCodigo,
        credenciado: body.credenciado,
        tipoPessoa: body.tipoPessoa,
        cnpjCpf: body.cnpjCpf,
        cep: body.cep,
        numero: body.numero,
        complemento: body.complemento,
        email: body.email,
        telefone1: body.telefone1,
        telefone2: body.telefone2,
        representante: body.representante,
        cpfRepresentante: body.cpfRepresentante,
        status: body.status,
      })

      if (validationResult.status !== 200) {
        sendJson(response, validationResult.status, validationResult.payload)
        return
      }

      const insertResult = await pool.query(
        `INSERT INTO credenciada (
           codigo,
           placa,
           empresa,
           condutor,
           tipo_pessoa,
           credenciado,
           cnpj_cpf,
           cep,
           numero,
           complemento,
           email,
           telefone_01,
           telefone_02,
           representante,
           cpf_representante,
           status,
           data_inclusao,
           data_modificacao
         )
         VALUES ($1, $2, $3, $4, $5, $6, $7, NULLIF($8, ''), NULLIF($9, ''), NULLIF($10, ''), NULLIF($11, ''), NULLIF($12, ''), NULLIF($13, ''), NULLIF($14, ''), NULLIF($15, ''), NULLIF($16, ''), NOW(), NOW())
         RETURNING ${credenciadaSelectClause}`,
        [
          validationResult.payload.codigo,
          validationResult.payload.placa,
          validationResult.payload.empresa,
          validationResult.payload.condutor,
          validationResult.payload.tipoPessoa,
          validationResult.payload.credenciado,
          validationResult.payload.cnpjCpf,
          validationResult.payload.cep,
          validationResult.payload.numero,
          validationResult.payload.complemento,
          validationResult.payload.email,
          validationResult.payload.telefone1,
          validationResult.payload.telefone2,
          validationResult.payload.representante,
          validationResult.payload.cpfRepresentante,
          validationResult.payload.status,
        ],
      )

      sendJson(response, 201, {
        item: insertResult.rows[0],
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao cadastrar credenciada.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'POST' && pathname === '/api/credenciada/import-xml') {
    try {
      const body = await readJsonBody(request)
      const result = await importCredenciadaXmlFile(body.fileName)

      sendJson(response, 200, {
        message: 'Importacao de credenciadas concluida com sucesso.',
        ...result,
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao importar credenciadas do XML.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'PUT' && getDreCodigoFromUrl(pathname)) {
    try {
      const originalCodigo = getDreCodigoFromUrl(pathname)
      const body = await readJsonBody(request)
      const sigla = normalizeDreSigla(body.sigla)
      const codigoOperacional = normalizeDreOperationalCode(body.codigoOperacional)
      const descricao = normalizeRequestValue(body.descricao)

      if (!originalCodigo) {
        sendJson(response, 400, { message: 'Codigo original invalido.' })
        return
      }

      if (sigla.length !== 2) {
        sendJson(response, 400, { message: 'Sigla deve conter 2 letras maiusculas.' })
        return
      }

      if (!descricao) {
        sendJson(response, 400, { message: 'Descricao e obrigatoria.' })
        return
      }

      const existingResult = await pool.query(
        'SELECT 1 FROM dre WHERE CAST(codigo AS text) = $1 LIMIT 1',
        [originalCodigo],
      )

      if (existingResult.rowCount === 0) {
        sendJson(response, 404, { message: 'Registro da DRE nao encontrado.' })
        return
      }

      const duplicateDescriptionResult = await pool.query(
        'SELECT 1 FROM dre WHERE BTRIM(CAST(descricao AS text)) = $1 AND CAST(codigo AS text) <> $2 LIMIT 1',
        [descricao, originalCodigo],
      )

      if (duplicateDescriptionResult.rowCount > 0) {
        sendJson(response, 409, { message: 'Descricao ja cadastrada.' })
        return
      }

      if (codigoOperacional) {
        const duplicateOperationalCodeResult = await pool.query(
          `SELECT 1
           FROM dre
           WHERE UPPER(BTRIM(COALESCE(codigo_operacional, ''))) = $1
             AND CAST(codigo AS text) <> $2
           LIMIT 1`,
          [codigoOperacional, originalCodigo],
        )

        if (duplicateOperationalCodeResult.rowCount > 0) {
          sendJson(response, 409, { message: 'Codigo operacional ja cadastrado.' })
          return
        }
      }

      const updateResult = await pool.query(
        `UPDATE dre
         SET sigla = $1,
             codigo_operacional = NULLIF($2, ''),
             descricao = $3
         WHERE CAST(codigo AS text) = $4
         RETURNING ${dreSelectClause}`,
        [sigla, codigoOperacional, descricao, originalCodigo],
      )

      sendJson(response, 200, {
        item: updateResult.rows[0],
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao alterar o registro dre.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'PUT' && getModalidadeCodigoFromUrl(pathname)) {
    try {
      const originalCodigo = getModalidadeCodigoFromUrl(pathname)
      const body = await readJsonBody(request)
      const descricao = normalizeRequestValue(body.descricao)

      if (!originalCodigo) {
        sendJson(response, 400, { message: 'Codigo original invalido.' })
        return
      }

      if (!descricao) {
        sendJson(response, 400, { message: 'Descricao e obrigatoria.' })
        return
      }

      const existingResult = await pool.query(
        'SELECT 1 FROM modalidade WHERE CAST(codigo AS text) = $1 LIMIT 1',
        [originalCodigo],
      )

      if (existingResult.rowCount === 0) {
        sendJson(response, 404, { message: 'Registro da modalidade nao encontrado.' })
        return
      }

      const duplicateDescriptionResult = await pool.query(
        'SELECT 1 FROM modalidade WHERE UPPER(BTRIM(CAST(descricao AS text))) = UPPER($1) AND CAST(codigo AS text) <> $2 LIMIT 1',
        [descricao, originalCodigo],
      )

      if (duplicateDescriptionResult.rowCount > 0) {
        sendJson(response, 409, { message: 'Descricao ja cadastrada.' })
        return
      }

      const updateResult = await pool.query(
        `UPDATE modalidade
         SET descricao = $1
         WHERE CAST(codigo AS text) = $2
         RETURNING ${modalidadeSelectClause}`,
        [descricao, originalCodigo],
      )

      sendJson(response, 200, {
        item: updateResult.rows[0],
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao alterar modalidade.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'PUT' && getTitularCodigoFromUrl(pathname)) {
    try {
      const originalCodigo = getTitularCodigoFromUrl(pathname)
      const body = await readJsonBody(request)

      if (!originalCodigo) {
        sendJson(response, 400, { message: 'Codigo original invalido.' })
        return
      }

      const existingResult = await pool.query(
        `SELECT 1 FROM ${titularTableName} WHERE codigo = $1 LIMIT 1`,
        [originalCodigo],
      )

      if (existingResult.rowCount === 0) {
        sendJson(response, 404, { message: 'Registro de titular do CRM nao encontrado.' })
        return
      }

      const validationResult = await validateTitularPayload({
        codigo: originalCodigo,
        cnpjCpf: body.cnpjCpf,
        titular: body.titular,
        originalCodigo,
      })

      if (validationResult.status !== 200) {
        sendJson(response, validationResult.status, validationResult.payload)
        return
      }

      const updateResult = await pool.query(
        `UPDATE ${titularTableName}
         SET cnpj_cpf = $1,
             titular = $2,
             data_modificacao = NOW()
         WHERE codigo = $3
         RETURNING ${titularSelectClause}`,
        [
          validationResult.payload.cnpjCpf,
          validationResult.payload.titular,
          originalCodigo,
        ],
      )

      sendJson(response, 200, {
        item: updateResult.rows[0],
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao alterar o titular do CRM.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'PUT' && getAccessCodigoFromUrl(pathname)) {
    try {
      const originalCodigo = Number(getAccessCodigoFromUrl(pathname))
      const body = await readJsonBody(request)
      const nome = normalizeRequestValue(body.nome)
      const email = normalizeRequestValue(body.email)
      const password = normalizeRequestValue(body.password)

      if (!Number.isInteger(originalCodigo) || originalCodigo <= 0) {
        sendJson(response, 400, { message: 'Codigo original invalido.' })
        return
      }

      const validationResult = await validateAccessPayload({
        nome,
        email,
        password,
        originalCodigo,
        requirePassword: false,
      })

      if (validationResult.status !== 200) {
        sendJson(response, validationResult.status, validationResult.payload)
        return
      }

      const existingResult = await pool.query(
        'SELECT codigo, nome, email, password, descricao FROM login WHERE codigo = $1 LIMIT 1',
        [originalCodigo],
      )

      if (existingResult.rowCount === 0) {
        sendJson(response, 404, { message: 'Acesso nao encontrado.' })
        return
      }

      const currentUser = existingResult.rows[0]
      const { nome: normalizedNome, email: normalizedEmail, password: normalizedPassword } = validationResult.payload
      const passwordPayload = password
        ? createAccessHashPayload(normalizedPassword)
        : {
            password: currentUser.password,
            descricao: currentUser.descricao,
          }

      const updateResult = await pool.query(
        `UPDATE login
         SET nome = $1, email = $2, password = $3, descricao = $4
         WHERE codigo = $5
         RETURNING codigo::text AS codigo, BTRIM(nome) AS nome, TRIM(email) AS email`,
        [normalizedNome, normalizedEmail, passwordPayload.password, passwordPayload.descricao, originalCodigo],
      )

      sendJson(response, 200, {
        item: updateResult.rows[0],
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao alterar acesso.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'PUT' && getTrocaCodigoFromUrl(pathname)) {
    try {
      const originalCodigo = Number(getTrocaCodigoFromUrl(pathname))
      const body = await readJsonBody(request)

      if (!Number.isInteger(originalCodigo) || originalCodigo <= 0) {
        sendJson(response, 400, { message: 'Codigo original invalido.' })
        return
      }

      const existingResult = await pool.query(
        'SELECT 1 FROM tipo_troca WHERE codigo = $1 LIMIT 1',
        [originalCodigo],
      )

      if (existingResult.rowCount === 0) {
        sendJson(response, 404, { message: 'Troca nao encontrada.' })
        return
      }

      const validationResult = await validateTrocaPayload({
        codigo: originalCodigo,
        controle: body.controle,
        lista: body.lista,
        originalCodigo,
      })

      if (validationResult.status !== 200) {
        sendJson(response, validationResult.status, validationResult.payload)
        return
      }

      const updateResult = await pool.query(
        `UPDATE tipo_troca
         SET controle = $1,
             lista = $2,
             data_modificacao = NOW()
         WHERE codigo = $3
         RETURNING ${trocaSelectClause}`,
        [
          validationResult.payload.controle,
          validationResult.payload.lista,
          originalCodigo,
        ],
      )

      sendJson(response, 200, {
        item: updateResult.rows[0],
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao alterar troca.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'PUT' && getEmissaoDocumentoParametroDataFromUrl(pathname)) {
    try {
      const originalDataReferencia = getEmissaoDocumentoParametroDataFromUrl(pathname)
      const body = await readJsonBody(request)

      if (!originalDataReferencia) {
        sendJson(response, 400, { message: 'Data original invalida.' })
        return
      }

      const normalizedOriginalDataReferencia = normalizeEmissaoDocumentoDateKey(originalDataReferencia)
      const existingResult = await pool.query(
        `SELECT 1
         FROM ${emissaoDocumentoParametroTableName}
         WHERE BTRIM(data_referencia) = $1
         LIMIT 1`,
        [normalizedOriginalDataReferencia],
      )

      if (existingResult.rowCount === 0) {
        sendJson(response, 404, { message: 'Parametro de emissao nao encontrado.' })
        return
      }

      const validationResult = await validateEmissaoDocumentoParametroPayload({
        dataReferencia: body.dataReferencia,
        objeto: body.objeto,
        objetoLicitacao: body.objetoLicitacao,
        credenciante: body.credenciante,
        tituloAditivo: body.tituloAditivo,
        termoSmt: body.termoSmt,
        descricaoAditivo: body.descricaoAditivo,
        corpoAditivo: body.corpoAditivo,
        assinaturasAditivo: body.assinaturasAditivo,
        descricaoContratoPf: body.descricaoContratoPf,
        descricaoContratoPj: body.descricaoContratoPj,
        corpoContratoPf: body.corpoContratoPf,
        corpoContratoPj: body.corpoContratoPj,
        linkModeloRelatorioContratoPf: body.linkModeloRelatorioContratoPf,
        linkModeloRelatorioContratoPj: body.linkModeloRelatorioContratoPj,
        textoDespacho: body.textoDespacho,
        editalChamamentoPublico: body.editalChamamentoPublico,
        obs01Emissao: body.obs01Emissao,
        obs02Emissao: body.obs02Emissao,
        rodapeEmissao: body.rodapeEmissao,
        prefeituraImagem: body.prefeituraImagem,
        tituloEmissao: body.tituloEmissao,
        diretorEmissao: body.diretorEmissao,
        originalDataReferencia: normalizedOriginalDataReferencia,
      })

      if (validationResult.status !== 200) {
        sendJson(response, validationResult.status, validationResult.payload)
        return
      }

      const updateResult = await pool.query(
        `UPDATE ${emissaoDocumentoParametroTableName}
         SET data_referencia = $1,
             objeto = $2,
             objeto_licitacao = $3,
             credenciante = $4,
             titulo_aditivo = $5,
             termo_smt = $6,
             descricao_aditivo = $7,
             corpo_aditivo = $8,
             assinaturas_aditivo = $9,
             descricao_contrato_pf = $10,
             descricao_contrato_pj = $11,
             corpo_contrato_pf = $12,
             corpo_contrato_pj = $13,
             link_modelo_relatorio_contrato_pf = $14,
             link_modelo_relatorio_contrato_pj = $15,
             texto_despacho = $16,
             edital_chamamento_publico = $17,
             obs_01_emissao = $18,
             obs_02_emissao = $19,
             rodape_emissao = $20,
             prefeitura_imagem = $21,
             titulo_emissao = $22,
             diretor_emissao = $23,
             data_modificacao = NOW()
           WHERE BTRIM(data_referencia) = $24
         RETURNING ${emissaoDocumentoParametroSelectClause}`,
        [
          validationResult.payload.dataReferencia,
          validationResult.payload.objeto,
          validationResult.payload.objetoLicitacao,
          validationResult.payload.credenciante,
          validationResult.payload.tituloAditivo,
          validationResult.payload.termoSmt,
          validationResult.payload.descricaoAditivo,
          validationResult.payload.corpoAditivo,
          validationResult.payload.assinaturasAditivo,
          validationResult.payload.descricaoContratoPf,
          validationResult.payload.descricaoContratoPj,
          validationResult.payload.corpoContratoPf,
          validationResult.payload.corpoContratoPj,
          validationResult.payload.linkModeloRelatorioContratoPf,
          validationResult.payload.linkModeloRelatorioContratoPj,
          validationResult.payload.textoDespacho,
          validationResult.payload.editalChamamentoPublico,
          validationResult.payload.obs01Emissao,
          validationResult.payload.obs02Emissao,
          validationResult.payload.rodapeEmissao,
          validationResult.payload.prefeituraImagem,
          validationResult.payload.tituloEmissao,
          validationResult.payload.diretorEmissao,
          normalizedOriginalDataReferencia,
        ],
      )

      sendJson(response, 200, { item: updateResult.rows[0] })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao alterar parametro de emissao.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'PUT' && getMarcaModeloCodigoFromUrl(pathname)) {
    try {
      const originalCodigo = getMarcaModeloCodigoFromUrl(pathname)
      const body = await readJsonBody(request)
      const descricao = normalizeTrocaText(body.descricao, 255)

      if (!originalCodigo) {
        sendJson(response, 400, { message: 'Codigo original invalido.' })
        return
      }

      if (!descricao) {
        sendJson(response, 400, { message: 'Descricao e obrigatoria.' })
        return
      }

      const existingResult = await pool.query(
        'SELECT 1 FROM marca_modelo WHERE CAST(codigo AS text) = $1 LIMIT 1',
        [originalCodigo],
      )

      if (existingResult.rowCount === 0) {
        sendJson(response, 404, { message: 'Registro de marca/modelo nao encontrado.' })
        return
      }

      const duplicateDescriptionResult = await pool.query(
        'SELECT 1 FROM marca_modelo WHERE BTRIM(descricao) = $1 AND CAST(codigo AS text) <> $2 LIMIT 1',
        [descricao, originalCodigo],
      )

      if (duplicateDescriptionResult.rowCount > 0) {
        sendJson(response, 409, { message: 'Descricao ja cadastrada.' })
        return
      }

      const updateResult = await pool.query(
        'UPDATE marca_modelo SET descricao = $1, data_modificacao = NOW() WHERE CAST(codigo AS text) = $2 RETURNING CAST(codigo AS text) AS codigo, BTRIM(descricao) AS descricao',
        [descricao, originalCodigo],
      )

      sendJson(response, 200, {
        item: updateResult.rows[0],
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao alterar o registro marca/modelo.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'PUT' && getSeguradoraCodigoFromUrl(pathname)) {
    try {
      const originalCodigo = Number(getSeguradoraCodigoFromUrl(pathname))
      const body = await readJsonBody(request)

      if (!Number.isInteger(originalCodigo) || originalCodigo <= 0) {
        sendJson(response, 400, { message: 'Codigo original invalido.' })
        return
      }

      const existingResult = await pool.query(
        'SELECT 1 FROM seguradora WHERE codigo = $1 LIMIT 1',
        [originalCodigo],
      )

      if (existingResult.rowCount === 0) {
        sendJson(response, 404, { message: 'Seguradora nao encontrada.' })
        return
      }

      const validationResult = await validateSeguradoraPayload({
        codigo: originalCodigo,
        controle: body.controle,
        descricao: body.descricao ?? body.lista,
        originalCodigo,
      })

      if (validationResult.status !== 200) {
        sendJson(response, validationResult.status, validationResult.payload)
        return
      }

      const updateResult = await pool.query(
        `UPDATE seguradora
         SET controle = $1,
             lista = $2,
             data_modificacao = NOW()
         WHERE codigo = $3
         RETURNING ${seguradoraSelectClause}`,
        [
          validationResult.payload.controle,
          validationResult.payload.descricao,
          originalCodigo,
        ],
      )

      sendJson(response, 200, {
        item: updateResult.rows[0],
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao alterar seguradora.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'PUT' && getCondutorCodigoFromUrl(pathname)) {
    try {
      const originalCodigo = Number(getCondutorCodigoFromUrl(pathname))
      const body = await readJsonBody(request)

      if (!Number.isInteger(originalCodigo) || originalCodigo <= 0) {
        sendJson(response, 400, { message: 'Codigo original invalido.' })
        return
      }

      const existingResult = await pool.query(
        'SELECT 1 FROM condutor WHERE codigo = $1 LIMIT 1',
        [originalCodigo],
      )

      if (existingResult.rowCount === 0) {
        sendJson(response, 404, { message: 'Condutor nao encontrado.' })
        return
      }

      const validationResult = await validateCondutorPayload({
        codigo: body.codigo,
        condutor: body.condutor,
        cpfCondutor: body.cpfCondutor,
        crmc: body.crmc,
        validadeCrmc: body.validadeCrmc,
        validadeCurso: body.validadeCurso,
        tipoVinculo: body.tipoVinculo,
        historico: body.historico,
        originalCodigo,
      })

      if (validationResult.status !== 200) {
        sendJson(response, validationResult.status, validationResult.payload)
        return
      }

      const updateResult = await pool.query(
        `UPDATE condutor
         SET codigo = $1,
             condutor = $2,
             cpf_condutor = $3,
             crmc = $4,
             validade_crmc = $5::date,
             validade_curso = $6::date,
             tipo_vinculo = NULLIF($7, ''),
             historico = NULLIF($8, ''),
             data_modificacao = NOW()
         WHERE codigo = $9
         RETURNING ${condutorSelectClause}`,
        [
          validationResult.payload.codigo,
          validationResult.payload.condutor,
          validationResult.payload.cpfCondutor,
          validationResult.payload.crmc,
          validationResult.payload.validadeCrmc,
          validationResult.payload.validadeCurso,
          validationResult.payload.tipoVinculo,
          validationResult.payload.historico,
          originalCodigo,
        ],
      )

      sendJson(response, 200, {
        item: updateResult.rows[0],
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao alterar condutor.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'PUT' && getMonitorCodigoFromUrl(pathname)) {
    try {
      const originalCodigo = Number(getMonitorCodigoFromUrl(pathname))
      const body = await readJsonBody(request)

      if (!Number.isInteger(originalCodigo) || originalCodigo <= 0) {
        sendJson(response, 400, { message: 'Codigo original invalido.' })
        return
      }

      const existingResult = await pool.query(
        'SELECT 1 FROM monitor WHERE codigo = $1 LIMIT 1',
        [originalCodigo],
      )

      if (existingResult.rowCount === 0) {
        sendJson(response, 404, { message: 'Monitor nao encontrado.' })
        return
      }

      const validationResult = await validateMonitorPayload({
        codigo: body.codigo,
        monitor: body.monitor,
        rgMonitor: body.rgMonitor,
        cpfMonitor: body.cpfMonitor,
        cursoMonitor: body.cursoMonitor,
        validadeCurso: body.validadeCurso,
        tipoVinculo: body.tipoVinculo,
        nascimento: body.nascimento,
        originalCodigo,
      })

      if (validationResult.status !== 200) {
        sendJson(response, validationResult.status, validationResult.payload)
        return
      }

      const updateResult = await pool.query(
        `UPDATE monitor
         SET codigo = $1,
             monitor = $2,
             rg_monitor = NULLIF($3, ''),
             cpf_monitor = $4,
             curso_monitor = NULLIF($5, '')::date,
             validade_curso = NULLIF($6, '')::date,
             tipo_vinculo = NULLIF($7, ''),
             nascimento = NULLIF($8, '')::date,
             data_modificacao = NOW()
         WHERE codigo = $9
         RETURNING ${monitorSelectClause}`,
        [
          validationResult.payload.codigo,
          validationResult.payload.monitor,
          validationResult.payload.rgMonitor,
          validationResult.payload.cpfMonitor,
          validationResult.payload.cursoMonitor,
          validationResult.payload.validadeCurso,
          validationResult.payload.tipoVinculo,
          validationResult.payload.nascimento,
          originalCodigo,
        ],
      )

      sendJson(response, 200, {
        item: updateResult.rows[0],
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao alterar monitor.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'PUT' && getCepFromUrl(pathname)) {
    try {
      const originalCep = normalizeCep(decodeURIComponent(getCepFromUrl(pathname)))
      const body = await readJsonBody(request)

      if (!originalCep || !isCepValid(originalCep)) {
        sendJson(response, 400, { message: 'CEP invalido na URL.' })
        return
      }

      const existingResult = await pool.query(
        `SELECT 1 FROM ${cepTableName} WHERE BTRIM(cep) = $1 LIMIT 1`,
        [originalCep],
      )

      if (existingResult.rowCount === 0) {
        sendJson(response, 404, { message: 'CEP nao encontrado.' })
        return
      }

      const validationResult = await validateCepPayload({ ...body, originalCep })

      if (validationResult.status !== 200) {
        sendJson(response, validationResult.status, validationResult.payload)
        return
      }

      const { payload } = validationResult

      const updateResult = await pool.query(
        `UPDATE ${cepTableName}
         SET cep = $1,
             logradouro = NULLIF($2, ''),
             complemento = NULLIF($3, ''),
             bairro = NULLIF($4, ''),
             municipio = $5,
             uf = $6,
             ibge = NULLIF($7, ''),
             data_modificacao = NOW()
         WHERE BTRIM(cep) = $8
         RETURNING ${cepSelectClause}`,
        [payload.cep, payload.logradouro, payload.complemento, payload.bairro, payload.municipio, payload.uf, payload.ibge, originalCep],
      )

      sendJson(response, 200, { item: updateResult.rows[0] })
    } catch (error) {
      sendJson(response, 500, { message: error instanceof Error ? error.message : 'Erro ao alterar CEP.' })
    }

    return
  }

  if (request.method === 'PUT' && getVeiculoCodigoFromUrl(pathname)) {
    try {
      const originalCodigo = Number(getVeiculoCodigoFromUrl(pathname))
      const body = await readJsonBody(request)

      if (!Number.isInteger(originalCodigo) || originalCodigo <= 0) {
        sendJson(response, 400, { message: 'Codigo original invalido.' })
        return
      }

      const existingResult = await pool.query(
        'SELECT 1 FROM veiculo WHERE codigo = $1 LIMIT 1',
        [originalCodigo],
      )

      if (existingResult.rowCount === 0) {
        sendJson(response, 404, { message: 'Veiculo nao encontrado.' })
        return
      }

      const validationResult = await validateVeiculoPayload({
        codigo: originalCodigo,
        crm: body.crm,
        placas: body.placas,
        ano: body.ano,
        capDetran: body.capDetran,
        capTeg: body.capTeg,
        capTegCreche: body.capTegCreche,
        capAcessivel: body.capAcessivel,
        valCrm: body.valCrm,
        seguradora: body.seguradora,
        seguroInicio: body.seguroInicio,
        seguroTermino: body.seguroTermino,
        tipoDeBancada: body.tipoDeBancada,
        tipoDeVeiculo: body.tipoDeVeiculo,
        marcaModelo: body.marcaModelo,
        titular: body.titular,
        cnpjCpf: body.cnpjCpf,
        valorVeiculo: body.valorVeiculo,
        osEspecial: body.osEspecial,
        originalCodigo,
      })

      if (validationResult.status !== 200) {
        sendJson(response, validationResult.status, validationResult.payload)
        return
      }

      const updateResult = await pool.query(
        `UPDATE veiculo
         SET crm = NULLIF($1, ''),
             placas = NULLIF($2, ''),
             ano = $3,
             cap_detran = $4,
             cap_teg = $5,
             cap_teg_creche = $6,
             cap_acessivel = $7,
             val_crm = NULLIF($8, '')::date,
             seguradora = NULLIF($9, ''),
             seguro_inicio = NULLIF($10, '')::date,
             seguro_termino = NULLIF($11, '')::date,
             tipo_de_bancada = NULLIF($12, ''),
             tipo_de_veiculo = NULLIF($13, ''),
             marca_modelo = NULLIF($14, ''),
             titular = NULLIF($15, ''),
             cnpj_cpf = NULLIF($16, ''),
             valor_veiculo = $17,
             os_especial = NULLIF($18, ''),
             data_modificacao = NOW()
         WHERE codigo = $19
         RETURNING ${veiculoSelectClause}`,
        [
          validationResult.payload.crm,
          validationResult.payload.placas,
          validationResult.payload.ano,
          validationResult.payload.capDetran,
          validationResult.payload.capTeg,
          validationResult.payload.capTegCreche,
          validationResult.payload.capAcessivel,
          validationResult.payload.valCrm,
          validationResult.payload.seguradora,
          validationResult.payload.seguroInicio,
          validationResult.payload.seguroTermino,
          validationResult.payload.tipoDeBancada,
          validationResult.payload.tipoDeVeiculo,
          validationResult.payload.marcaModelo,
          validationResult.payload.titular,
          validationResult.payload.cnpjCpf,
          validationResult.payload.valorVeiculo,
          validationResult.payload.osEspecial,
          originalCodigo,
        ],
      )

      sendJson(response, 200, {
        item: updateResult.rows[0],
      })
    } catch (error) {
      const persistenceError = getVeiculoPersistenceError(error, 'Erro ao alterar veiculo.')

      sendJson(response, persistenceError.status, { message: persistenceError.message })
    }

    return
  }

  if (request.method === 'PUT' && getCredenciamentoTermoCodigoFromUrl(pathname)) {
    try {
      const originalCodigo = Number(getCredenciamentoTermoCodigoFromUrl(pathname))

      if (!Number.isInteger(originalCodigo) || originalCodigo <= 0) {
        sendJson(response, 400, { message: 'Codigo invalido para alteracao.' })
        return
      }

      const body = await readJsonBody(request)
      const validationResult = await validateCredenciamentoTermoPayload(body)

      if (validationResult.status !== 200) {
        sendJson(response, validationResult.status, validationResult.payload)
        return
      }

      const updateResult = await pool.query(
        `UPDATE ${credenciamentoTermoTableName}
         SET codigo_xml = $1,
             credenciada_codigo = $2,
             termo_adesao = $3,
             sei = NULLIF($4, ''),
             aditivo = $5,
             situacao_publicacao = NULLIF($6, ''),
             situacao_emissao = NULLIF($7, ''),
             inicio_vigencia = NULLIF($8, '')::date,
             termino_vigencia = NULLIF($9, '')::date,
             comp_data_aditivo = NULLIF($10, '')::date,
             status_aditivo = NULLIF($11, ''),
             data_pub_aditivo = NULLIF($12, '')::date,
             check_aditivo = $13,
             status_termo = NULLIF($14, ''),
             tipo_termo = NULLIF($15, ''),
             especificacao_sei = NULLIF($16, ''),
             valor_contrato = $17,
            data_publicacao = NULLIF($18, '')::date,
            valor_contrato_atualizado = $19,
            vencimento_geral = NULLIF($20, '')::date,
            mes_renovacao = NULLIF($21, ''),
            tp_optante = NULLIF($22, ''),
             data_modificacao = NOW()
           WHERE codigo = $23
         RETURNING codigo`,
        [
          validationResult.payload.codigoXml,
          validationResult.payload.credenciadaCodigo,
          validationResult.payload.termoAdesao,
          validationResult.payload.sei,
          validationResult.payload.aditivo,
          validationResult.payload.situacaoPublicacao,
          validationResult.payload.situacaoEmissao,
          validationResult.payload.inicioVigencia,
          validationResult.payload.terminoVigencia,
          validationResult.payload.compDataAditivo,
          validationResult.payload.statusAditivo,
          validationResult.payload.dataPubAditivo,
          validationResult.payload.checkAditivo,
          validationResult.payload.statusTermo,
          validationResult.payload.tipoTermo,
          validationResult.payload.especificacaoSei,
          validationResult.payload.valorContrato,
          validationResult.payload.dataPublicacao,
          validationResult.payload.valorContratoAtualizado,
          validationResult.payload.vencimentoGeral,
          validationResult.payload.mesRenovacao,
          validationResult.payload.tpOptante,
          originalCodigo,
        ],
      )

      if (updateResult.rowCount === 0) {
        sendJson(response, 404, { message: 'Credenciamento termo nao encontrado.' })
        return
      }

      const item = await fetchCredenciamentoTermoItemByCodigo(pool, originalCodigo)
      sendJson(response, 200, { item })
    } catch (error) {
      const persistenceError = getCredenciamentoTermoPersistenceError(error, 'Erro ao alterar credenciamento termo.')
      sendJson(response, persistenceError.status, { message: persistenceError.message })
    }

    return
  }

  if (request.method === 'PUT' && getVinculoCondutorIdFromUrl(pathname)) {
    try {
      const originalId = Number(getVinculoCondutorIdFromUrl(pathname))
      const body = await readJsonBody(request)

      if (!Number.isInteger(originalId) || originalId <= 0) {
        sendJson(response, 400, { message: 'Codigo original invalido.' })
        return
      }

      const existingItem = await fetchVinculoCondutorItemById(pool, originalId)

      if (!existingItem) {
        sendJson(response, 404, { message: 'Vinculo do condutor nao encontrado.' })
        return
      }

      const validationResult = await validateVinculoCondutorPayload({
        termoAdesao: body.termoAdesao,
        numOs: body.numOs,
        revisao: body.revisao,
        credenciadaCodigo: body.credenciadaCodigo,
        credenciado: body.credenciado,
        dataOs: body.dataOs,
        dataAdmissaoCondutor: body.dataAdmissaoCondutor,
        condutorCodigo: body.condutorCodigo,
        cpfCondutor: body.cpfCondutor,
      })

      if (validationResult.status !== 200) {
        sendJson(response, validationResult.status, validationResult.payload)
        return
      }

      await pool.query(
        `UPDATE ${vinculoCondutorTableName}
            SET termo_adesao = NULLIF($1, ''),
                num_os = NULLIF($2, ''),
                revisao = NULLIF($3, ''),
                credenciada_codigo = $4,
                data_os = NULLIF($5, '')::date,
                data_admissao_condutor = NULLIF($6, '')::date,
                condutor_codigo = $7
          WHERE id = $8`,
        [
          validationResult.payload.termoAdesao,
          validationResult.payload.numOs,
          validationResult.payload.revisao,
          validationResult.payload.credenciadaCodigo,
          validationResult.payload.dataOs,
          validationResult.payload.dataAdmissaoCondutor,
          validationResult.payload.condutorCodigo,
          originalId,
        ],
      )

      const item = await fetchVinculoCondutorItemById(pool, originalId)
      sendJson(response, 200, { item })
    } catch (error) {
      const persistenceError = getVinculoCondutorPersistenceError(error, 'Erro ao alterar vinculo do condutor.')
      sendJson(response, persistenceError.status, { message: persistenceError.message })
    }

    return
  }

  if (request.method === 'PUT' && getVinculoMonitorIdFromUrl(pathname)) {
    try {
      const originalId = Number(getVinculoMonitorIdFromUrl(pathname))
      const body = await readJsonBody(request)

      if (!Number.isInteger(originalId) || originalId <= 0) {
        sendJson(response, 400, { message: 'Codigo original invalido.' })
        return
      }

      const existingItem = await fetchVinculoMonitorItemById(pool, originalId)

      if (!existingItem) {
        sendJson(response, 404, { message: 'Vinculo do monitor nao encontrado.' })
        return
      }

      const validationResult = await validateVinculoMonitorPayload({
        termoAdesao: body.termoAdesao,
        numOs: body.numOs,
        revisao: body.revisao,
        credenciadaCodigo: body.credenciadaCodigo,
        credenciado: body.credenciado,
        dataOs: body.dataOs,
        dataAdmissaoMonitor: body.dataAdmissaoMonitor,
        monitorCodigo: body.monitorCodigo,
        cpfMonitor: body.cpfMonitor,
      })

      if (validationResult.status !== 200) {
        sendJson(response, validationResult.status, validationResult.payload)
        return
      }

      await pool.query(
        `UPDATE ${vinculoMonitorTableName}
            SET termo_adesao = NULLIF($1, ''),
                num_os = NULLIF($2, ''),
                revisao = NULLIF($3, ''),
                credenciada_codigo = $4,
                data_os = NULLIF($5, '')::date,
                data_admissao_monitor = NULLIF($6, '')::date,
                monitor_codigo = $7
          WHERE id = $8`,
        [
          validationResult.payload.termoAdesao,
          validationResult.payload.numOs,
          validationResult.payload.revisao,
          validationResult.payload.credenciadaCodigo,
          validationResult.payload.dataOs,
          validationResult.payload.dataAdmissaoMonitor,
          validationResult.payload.monitorCodigo,
          originalId,
        ],
      )

      const item = await fetchVinculoMonitorItemById(pool, originalId)
      sendJson(response, 200, { item })
    } catch (error) {
      const persistenceError = getVinculoMonitorPersistenceError(error, 'Erro ao alterar vinculo do monitor.')
      sendJson(response, persistenceError.status, { message: persistenceError.message })
    }

    return
  }

  if (request.method === 'PUT' && getCredenciadaCodigoFromUrl(pathname)) {
    try {
      const originalCodigo = Number(getCredenciadaCodigoFromUrl(pathname))
      const body = await readJsonBody(request)

      if (!Number.isInteger(originalCodigo) || originalCodigo <= 0) {
        sendJson(response, 400, { message: 'Codigo original invalido.' })
        return
      }

      const existingResult = await pool.query(
        'SELECT 1 FROM credenciada WHERE codigo = $1 LIMIT 1',
        [originalCodigo],
      )

      if (existingResult.rowCount === 0) {
        sendJson(response, 404, { message: 'Credenciada nao encontrada.' })
        return
      }

      const validationResult = await validateCredenciadaPayload({
        codigo: originalCodigo,
        credenciado: body.credenciado,
        tipoPessoa: body.tipoPessoa,
        cnpjCpf: body.cnpjCpf,
        cep: body.cep,
        numero: body.numero,
        complemento: body.complemento,
        email: body.email,
        telefone1: body.telefone1,
        telefone2: body.telefone2,
        representante: body.representante,
        cpfRepresentante: body.cpfRepresentante,
        status: body.status,
        originalCodigo,
      })

      if (validationResult.status !== 200) {
        sendJson(response, validationResult.status, validationResult.payload)
        return
      }

      const updateResult = await pool.query(
        `UPDATE credenciada
         SET placa = $1,
           empresa = $2,
           condutor = $3,
           tipo_pessoa = $4,
           credenciado = $5,
           cnpj_cpf = $6,
           cep = NULLIF($7, ''),
           numero = NULLIF($8, ''),
           complemento = NULLIF($9, ''),
           email = NULLIF($10, ''),
           telefone_01 = NULLIF($11, ''),
           telefone_02 = NULLIF($12, ''),
           representante = NULLIF($13, ''),
           cpf_representante = NULLIF($14, ''),
           status = NULLIF($15, ''),
             data_modificacao = NOW()
         WHERE codigo = $16
         RETURNING ${credenciadaSelectClause}`,
        [
          validationResult.payload.placa,
          validationResult.payload.empresa,
          validationResult.payload.condutor,
          validationResult.payload.tipoPessoa,
          validationResult.payload.credenciado,
          validationResult.payload.cnpjCpf,
          validationResult.payload.cep,
          validationResult.payload.numero,
          validationResult.payload.complemento,
          validationResult.payload.email,
          validationResult.payload.telefone1,
          validationResult.payload.telefone2,
          validationResult.payload.representante,
          validationResult.payload.cpfRepresentante,
          validationResult.payload.status,
          originalCodigo,
        ],
      )

      sendJson(response, 200, {
        item: updateResult.rows[0],
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao alterar credenciada.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'PUT' && getLoginDrePairFromUrl(pathname)) {
    try {
      const pair = getLoginDrePairFromUrl(pathname)
      const originalLoginCodigo = Number(pair?.loginCodigo)
      const originalDreCodigo = Number(pair?.dreCodigo)
      const body = await readJsonBody(request)

      if (!Number.isInteger(originalLoginCodigo) || originalLoginCodigo <= 0 || !Number.isInteger(originalDreCodigo) || originalDreCodigo <= 0) {
        sendJson(response, 400, { message: 'Chave original do relacionamento e invalida.' })
        return
      }

      const existingResult = await pool.query(
        'SELECT 1 FROM login_dre WHERE login_codigo = $1 AND dre_codigo = $2 LIMIT 1',
        [originalLoginCodigo, originalDreCodigo],
      )

      if (existingResult.rowCount === 0) {
        sendJson(response, 404, { message: 'Relacionamento login x DRE nao encontrado.' })
        return
      }

      const validationResult = await validateLoginDrePayload({
        loginCodigo: body.loginCodigo,
        dreCodigo: body.dreCodigo,
        originalLoginCodigo,
        originalDreCodigo,
      })

      if (validationResult.status !== 200) {
        sendJson(response, validationResult.status, validationResult.payload)
        return
      }

      const { loginCodigo, dreCodigo, loginNome, dreDescricao } = validationResult.payload
      const updateResult = await pool.query(
        `UPDATE login_dre
         SET login_codigo = $1, dre_codigo = $2
         WHERE login_codigo = $3 AND dre_codigo = $4
         RETURNING login_codigo::text AS login_codigo, dre_codigo::text AS dre_codigo`,
        [loginCodigo, dreCodigo, originalLoginCodigo, originalDreCodigo],
      )

      sendJson(response, 200, {
        item: {
          ...updateResult.rows[0],
          login_nome: loginNome,
          dre_descricao: dreDescricao,
        },
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao alterar login_dre.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'PUT' && getOrdemServicoCodigoFromUrl(pathname)) {
    try {
      const originalCodigo = Number(getOrdemServicoCodigoFromUrl(pathname))
      const body = await readJsonBody(request)

      if (!Number.isInteger(originalCodigo) || originalCodigo <= 0) {
        sendJson(response, 400, { message: 'Codigo original invalido.' })
        return
      }

      const existingResult = await pool.query(
        `SELECT TO_CHAR(data_inclusao::date, 'YYYY-MM-DD') AS data_inclusao,
                TO_CHAR(vigencia_os::date, 'YYYY-MM-DD') AS vigencia_os
         FROM ${ordemServicoTableName}
         WHERE codigo = $1
         LIMIT 1`,
        [originalCodigo],
      )

      if (existingResult.rowCount === 0) {
        sendJson(response, 404, { message: 'OrdemServico nao encontrada.' })
        return
      }

      const existingItem = existingResult.rows[0]

      const validationResult = await validateOrdemServicoPayload({
        codigo: body.codigo ?? originalCodigo,
        codigoAccess: body.codigoAccess,
        termoAdesao: body.termoAdesao,
        numOs: body.numOs,
        revisao: body.revisao,
        vigenciaOs: existingItem.vigencia_os,
        credenciado: body.credenciado,
        cnpjCpf: body.cnpjCpf,
        dreCodigo: body.dreCodigo,
        modalidadeDescricao: body.modalidadeDescricao,
        cpfCondutor: body.cpfCondutor,
        dataAdmissaoCondutor: body.dataAdmissaoCondutor,
        cpfPreposto: body.cpfPreposto,
        prepostoInicio: body.prepostoInicio,
        prepostoDias: body.prepostoDias,
        crm: body.crm,
        cpfMonitor: body.cpfMonitor,
        dataAdmissaoMonitor: body.dataAdmissaoMonitor,
        situacao: body.situacao,
        tipoTroca: body.tipoTroca,
        conexao: body.conexao,
        dataEncerramento: body.dataEncerramento,
        anotacao: body.anotacao,
        uniaoTermos: body.uniaoTermos,
        originalCodigo,
        skipVigenciaValidation: true,
      })

      if (validationResult.status !== 200) {
        sendJson(response, validationResult.status, validationResult.payload)
        return
      }

      const client = await pool.connect()

      try {
        await client.query('BEGIN')

        const updateResult = await client.query(
          `UPDATE ${ordemServicoTableName}
           SET codigo = $1,
               codigo_access = NULLIF($2, ''),
               termo_adesao = NULLIF($3, ''),
               num_os = NULLIF($4, ''),
               revisao = NULLIF($5, ''),
               os_concat = NULLIF($6, ''),
               vigencia_os = NULLIF($7, '')::date,
               termo_codigo = $8,
               dre_codigo = $9,
               dre_descricao = $10,
               modalidade_codigo = $11,
               modalidade_descricao = NULLIF($12, ''),
               cpf_condutor = $13,
               condutor = $14,
               data_admissao_condutor = NULLIF($15, '')::date,
               cpf_preposto = NULLIF($16, ''),
               preposto_condutor = NULLIF($17, ''),
               preposto_inicio = NULLIF($18, '')::date,
               preposto_dias = $19,
               crm = $20,
               veiculo_placas = NULLIF($21, ''),
               cpf_monitor = NULLIF($22, ''),
               monitor = NULLIF($23, ''),
               data_admissao_monitor = NULLIF($24, '')::date,
               situacao = $25,
               tipo_troca_codigo = $26,
               tipo_troca_descricao = NULLIF($27, ''),
               conexao = NULLIF($28, ''),
               data_encerramento = NULLIF($29, '')::date,
               anotacao = NULLIF($30, ''),
               uniao_termos = NULLIF($31, ''),
               data_modificacao = NOW()
           WHERE codigo = $32`,
          [
            validationResult.payload.codigo,
            validationResult.payload.codigoAccess,
            validationResult.payload.termoAdesao,
            validationResult.payload.numOs,
            validationResult.payload.revisao,
            validationResult.payload.osConcat,
            validationResult.payload.vigenciaOs,
            validationResult.payload.termoCodigo,
            validationResult.payload.dreCodigo,
            validationResult.payload.dreDescricao,
            validationResult.payload.modalidadeCodigo,
            validationResult.payload.modalidadeDescricao,
            validationResult.payload.cpfCondutor,
            validationResult.payload.condutor,
            validationResult.payload.dataAdmissaoCondutor,
            validationResult.payload.cpfPreposto,
            validationResult.payload.prepostoCondutor,
            validationResult.payload.prepostoInicio,
            validationResult.payload.prepostoDias,
            validationResult.payload.crm,
            validationResult.payload.veiculoPlacas,
            validationResult.payload.cpfMonitor,
            validationResult.payload.monitor,
            validationResult.payload.dataAdmissaoMonitor,
            validationResult.payload.situacao,
            validationResult.payload.tipoTrocaCodigo,
            validationResult.payload.tipoTrocaDescricao,
            validationResult.payload.conexao,
            validationResult.payload.dataEncerramento,
            validationResult.payload.anotacao,
            validationResult.payload.uniaoTermos,
            originalCodigo,
          ],
        )

        if (updateResult.rowCount === 0) {
          await client.query('ROLLBACK')
          sendJson(response, 404, { message: 'OrdemServico nao encontrada.' })
          return
        }

        await rebalanceOrdemServicoRevisions(client)
        await syncCondutorVinculosFromOrdemServico(client)
        await syncMonitorVinculosFromOrdemServico(client)

        const item = await fetchOrdemServicoItemByCodigo(client, validationResult.payload.codigo)

        await client.query('COMMIT')
        sendJson(response, 200, { item })
      } catch (error) {
        await client.query('ROLLBACK')
        throw error
      } finally {
        client.release()
      }
    } catch (error) {
      const persistenceError = getOrdemServicoPersistenceError(error, 'Erro ao alterar OrdemServico.')
      sendJson(response, persistenceError.status, { message: persistenceError.message })
    }

    return
  }

  if (request.method === 'DELETE' && getDreCodigoFromUrl(pathname)) {
    try {
      const codigo = getDreCodigoFromUrl(pathname)

      if (!codigo) {
        sendJson(response, 400, { message: 'Codigo invalido para exclusao.' })
        return
      }

      const deleteResult = await pool.query(
        'DELETE FROM dre WHERE CAST(codigo AS text) = $1 RETURNING CAST(codigo AS text) AS codigo',
        [codigo],
      )

      if (deleteResult.rowCount === 0) {
        sendJson(response, 404, { message: 'Registro da DRE nao encontrado.' })
        return
      }

      sendJson(response, 200, {
        deletedCodigo: deleteResult.rows[0].codigo,
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao excluir o registro dre.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'DELETE' && getModalidadeCodigoFromUrl(pathname)) {
    try {
      const codigo = getModalidadeCodigoFromUrl(pathname)

      if (!codigo) {
        sendJson(response, 400, { message: 'Codigo invalido para exclusao.' })
        return
      }

      const deleteResult = await pool.query(
        'DELETE FROM modalidade WHERE CAST(codigo AS text) = $1 RETURNING CAST(codigo AS text) AS codigo',
        [codigo],
      )

      if (deleteResult.rowCount === 0) {
        sendJson(response, 404, { message: 'Registro da modalidade nao encontrado.' })
        return
      }

      sendJson(response, 200, {
        deletedCodigo: deleteResult.rows[0].codigo,
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao excluir o registro modalidade.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'DELETE' && getTitularCodigoFromUrl(pathname)) {
    try {
      const codigo = getTitularCodigoFromUrl(pathname)

      if (!codigo) {
        sendJson(response, 400, { message: 'Codigo invalido para exclusao.' })
        return
      }

      const deleteResult = await pool.query(
        `DELETE FROM ${titularTableName} WHERE codigo = $1 RETURNING codigo::text AS codigo`,
        [codigo],
      )

      if (deleteResult.rowCount === 0) {
        sendJson(response, 404, { message: 'Registro de titular do CRM nao encontrado.' })
        return
      }

      sendJson(response, 200, {
        deletedCodigo: deleteResult.rows[0].codigo,
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao excluir o titular do CRM.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'DELETE' && getTrocaCodigoFromUrl(pathname)) {
    try {
      const codigo = getTrocaCodigoFromUrl(pathname)

      if (!codigo) {
        sendJson(response, 400, { message: 'Codigo invalido para exclusao.' })
        return
      }

      const deleteResult = await pool.query(
        'DELETE FROM tipo_troca WHERE codigo = $1 RETURNING codigo::text AS codigo',
        [codigo],
      )

      if (deleteResult.rowCount === 0) {
        sendJson(response, 404, { message: 'Troca nao encontrada.' })
        return
      }

      sendJson(response, 200, {
        deletedCodigo: deleteResult.rows[0].codigo,
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao excluir troca.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'DELETE' && getEmissaoDocumentoParametroDataFromUrl(pathname)) {
    try {
      const dataReferencia = normalizeEmissaoDocumentoDateKey(getEmissaoDocumentoParametroDataFromUrl(pathname))

      if (!dataReferencia) {
        sendJson(response, 400, { message: 'Data invalida para exclusao.' })
        return
      }

      const deleteResult = await pool.query(
        `DELETE FROM ${emissaoDocumentoParametroTableName}
         WHERE BTRIM(data_referencia) = $1
         RETURNING BTRIM(data_referencia) AS data_referencia`,
        [dataReferencia],
      )

      if (deleteResult.rowCount === 0) {
        sendJson(response, 404, { message: 'Parametro de emissao nao encontrado.' })
        return
      }

      sendJson(response, 200, { deletedDataReferencia: deleteResult.rows[0].data_referencia })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao excluir parametro de emissao.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'DELETE' && getAccessCodigoFromUrl(pathname)) {
    try {
      const codigo = Number(getAccessCodigoFromUrl(pathname))

      if (!Number.isInteger(codigo) || codigo <= 0) {
        sendJson(response, 400, { message: 'Codigo invalido para exclusao.' })
        return
      }

      const deleteResult = await pool.query(
        'DELETE FROM login WHERE codigo = $1 RETURNING codigo::text AS codigo, TRIM(email) AS email',
        [codigo],
      )

      if (deleteResult.rowCount === 0) {
        sendJson(response, 404, { message: 'Acesso nao encontrado.' })
        return
      }

      sendJson(response, 200, {
        deletedCodigo: deleteResult.rows[0].codigo,
        deletedEmail: deleteResult.rows[0].email,
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao excluir acesso.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'DELETE' && getCondutorCodigoFromUrl(pathname)) {
    try {
      const codigo = Number(getCondutorCodigoFromUrl(pathname))

      if (!Number.isInteger(codigo) || codigo <= 0) {
        sendJson(response, 400, { message: 'Codigo invalido para exclusao.' })
        return
      }

      const deleteResult = await pool.query(
        'DELETE FROM condutor WHERE codigo = $1 RETURNING codigo::text AS codigo',
        [codigo],
      )

      if (deleteResult.rowCount === 0) {
        sendJson(response, 404, { message: 'Condutor nao encontrado.' })
        return
      }

      sendJson(response, 200, {
        deletedCodigo: deleteResult.rows[0].codigo,
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao excluir condutor.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'DELETE' && getMonitorCodigoFromUrl(pathname)) {
    try {
      const codigo = Number(getMonitorCodigoFromUrl(pathname))

      if (!Number.isInteger(codigo) || codigo <= 0) {
        sendJson(response, 400, { message: 'Codigo invalido para exclusao.' })
        return
      }

      const deleteResult = await pool.query(
        'DELETE FROM monitor WHERE codigo = $1 RETURNING codigo::text AS codigo',
        [codigo],
      )

      if (deleteResult.rowCount === 0) {
        sendJson(response, 404, { message: 'Monitor nao encontrado.' })
        return
      }

      sendJson(response, 200, {
        deletedCodigo: deleteResult.rows[0].codigo,
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao excluir monitor.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'DELETE' && getCepFromUrl(pathname)) {
    try {
      const cepToDelete = normalizeCep(decodeURIComponent(getCepFromUrl(pathname)))

      if (!cepToDelete || !isCepValid(cepToDelete)) {
        sendJson(response, 400, { message: 'CEP invalido para exclusao.' })
        return
      }

      const deleteResult = await pool.query(
        `DELETE FROM ${cepTableName} WHERE BTRIM(cep) = $1 RETURNING BTRIM(cep) AS cep`,
        [cepToDelete],
      )

      if (deleteResult.rowCount === 0) {
        sendJson(response, 404, { message: 'CEP nao encontrado.' })
        return
      }

      sendJson(response, 200, { deletedCep: deleteResult.rows[0].cep })
    } catch (error) {
      sendJson(response, 500, { message: error instanceof Error ? error.message : 'Erro ao excluir CEP.' })
    }

    return
  }

  if (request.method === 'DELETE' && getMarcaModeloCodigoFromUrl(pathname)) {
    try {
      const codigo = getMarcaModeloCodigoFromUrl(pathname)

      if (!codigo) {
        sendJson(response, 400, { message: 'Codigo invalido para exclusao.' })
        return
      }

      const deleteResult = await pool.query(
        'DELETE FROM marca_modelo WHERE CAST(codigo AS text) = $1 RETURNING CAST(codigo AS text) AS codigo',
        [codigo],
      )

      if (deleteResult.rowCount === 0) {
        sendJson(response, 404, { message: 'Registro de marca/modelo nao encontrado.' })
        return
      }

      sendJson(response, 200, {
        deletedCodigo: deleteResult.rows[0].codigo,
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao excluir o registro marca/modelo.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'DELETE' && getVeiculoCodigoFromUrl(pathname)) {
    try {
      const codigo = Number(getVeiculoCodigoFromUrl(pathname))

      if (!Number.isInteger(codigo) || codigo <= 0) {
        sendJson(response, 400, { message: 'Codigo invalido para exclusao.' })
        return
      }

      const deleteResult = await pool.query(
        'DELETE FROM veiculo WHERE codigo = $1 RETURNING codigo::text AS codigo',
        [codigo],
      )

      if (deleteResult.rowCount === 0) {
        sendJson(response, 404, { message: 'Veiculo nao encontrado.' })
        return
      }

      sendJson(response, 200, {
        deletedCodigo: deleteResult.rows[0].codigo,
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao excluir veiculo.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'DELETE' && getCredenciamentoTermoCodigoFromUrl(pathname)) {
    try {
      const codigo = Number(getCredenciamentoTermoCodigoFromUrl(pathname))

      if (!Number.isInteger(codigo) || codigo <= 0) {
        sendJson(response, 400, { message: 'Codigo invalido para exclusao.' })
        return
      }

      const deleteResult = await pool.query(
        `DELETE FROM ${credenciamentoTermoTableName}
         WHERE codigo = $1
         RETURNING codigo::text AS codigo`,
        [codigo],
      )

      if (deleteResult.rowCount === 0) {
        sendJson(response, 404, { message: 'Credenciamento termo nao encontrado.' })
        return
      }

      sendJson(response, 200, { deletedCodigo: deleteResult.rows[0].codigo })
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Erro ao excluir credenciamento termo.'
      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'DELETE' && getVinculoCondutorIdFromUrl(pathname)) {
    try {
      const id = Number(getVinculoCondutorIdFromUrl(pathname))

      if (!Number.isInteger(id) || id <= 0) {
        sendJson(response, 400, { message: 'Codigo invalido para exclusao.' })
        return
      }

      const deleteResult = await pool.query(
        `DELETE FROM ${vinculoCondutorTableName}
          WHERE id = $1
          RETURNING id::text AS id`,
        [id],
      )

      if (deleteResult.rowCount === 0) {
        sendJson(response, 404, { message: 'Vinculo do condutor nao encontrado.' })
        return
      }

      sendJson(response, 200, {
        deletedCodigo: deleteResult.rows[0].id,
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao excluir vinculo do condutor.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'DELETE' && getCredenciadaCodigoFromUrl(pathname)) {
    try {
      const codigo = Number(getCredenciadaCodigoFromUrl(pathname))

      if (!Number.isInteger(codigo) || codigo <= 0) {
        sendJson(response, 400, { message: 'Codigo invalido para exclusao.' })
        return
      }

      const deleteResult = await pool.query(
        'DELETE FROM credenciada WHERE codigo = $1 RETURNING codigo::text AS codigo',
        [codigo],
      )

      if (deleteResult.rowCount === 0) {
        sendJson(response, 404, { message: 'Credenciada nao encontrada.' })
        return
      }

      sendJson(response, 200, {
        deletedCodigo: deleteResult.rows[0].codigo,
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao excluir credenciada.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'DELETE' && getLoginDrePairFromUrl(pathname)) {
    try {
      const pair = getLoginDrePairFromUrl(pathname)
      const loginCodigo = Number(pair?.loginCodigo)
      const dreCodigo = Number(pair?.dreCodigo)

      if (!Number.isInteger(loginCodigo) || loginCodigo <= 0 || !Number.isInteger(dreCodigo) || dreCodigo <= 0) {
        sendJson(response, 400, { message: 'Chave do relacionamento invalida para exclusao.' })
        return
      }

      const deleteResult = await pool.query(
        `DELETE FROM login_dre
         WHERE login_codigo = $1 AND dre_codigo = $2
         RETURNING login_codigo::text AS login_codigo, dre_codigo::text AS dre_codigo`,
        [loginCodigo, dreCodigo],
      )

      if (deleteResult.rowCount === 0) {
        sendJson(response, 404, { message: 'Relacionamento login x DRE nao encontrado.' })
        return
      }

      sendJson(response, 200, {
        deletedLoginCodigo: deleteResult.rows[0].login_codigo,
        deletedDreCodigo: deleteResult.rows[0].dre_codigo,
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao excluir login_dre.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'DELETE' && getSeguradoraCodigoFromUrl(pathname)) {
    try {
      const codigo = getSeguradoraCodigoFromUrl(pathname)

      if (!codigo) {
        sendJson(response, 400, { message: 'Codigo invalido para exclusao.' })
        return
      }

      const deleteResult = await pool.query(
        'DELETE FROM seguradora WHERE codigo = $1 RETURNING codigo::text AS codigo',
        [codigo],
      )

      if (deleteResult.rowCount === 0) {
        sendJson(response, 404, { message: 'Seguradora nao encontrada.' })
        return
      }

      sendJson(response, 200, {
        deletedCodigo: deleteResult.rows[0].codigo,
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao excluir a seguradora.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'DELETE' && getVinculoMonitorIdFromUrl(pathname)) {
    try {
      const id = Number(getVinculoMonitorIdFromUrl(pathname))

      if (!Number.isInteger(id) || id <= 0) {
        sendJson(response, 400, { message: 'Codigo invalido para exclusao.' })
        return
      }

      const deleteResult = await pool.query(
        `DELETE FROM ${vinculoMonitorTableName}
          WHERE id = $1
          RETURNING id::text AS id`,
        [id],
      )

      if (deleteResult.rowCount === 0) {
        sendJson(response, 404, { message: 'Vinculo do monitor nao encontrado.' })
        return
      }

      sendJson(response, 200, {
        deletedCodigo: deleteResult.rows[0].id,
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao excluir vinculo do monitor.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'PATCH' && getOrdemServicoCodigoFromUrl(pathname)) {
    try {
      const codigo = Number(getOrdemServicoCodigoFromUrl(pathname))

      if (!Number.isInteger(codigo) || codigo <= 0) {
        sendJson(response, 400, { message: 'Codigo invalido.' })
        return
      }

      const body = await readJsonBody(request)
      const situacao = normalizeCredenciamentoSituacao(body.situacao)
      const dataEncerramento = normalizeRequestValue(body.dataEncerramento)

      if (!situacao) {
        sendJson(response, 400, { message: 'Situacao invalida.' })
        return
      }

      if (dataEncerramento && !isDateInputValid(dataEncerramento)) {
        sendJson(response, 400, { message: 'Data de encerramento invalida.' })
        return
      }

      if ((situacao === 'Cancelado' || situacao === 'Inativo' || situacao === 'Substituido') && !dataEncerramento) {
        sendJson(response, 400, { message: 'Data de encerramento e obrigatoria para a situacao informada.' })
        return
      }

      const client = await pool.connect()

      try {
        await client.query('BEGIN')

        const updateResult = await client.query(
          `UPDATE ${ordemServicoTableName}
           SET situacao = $2,
               data_encerramento = $3,
               data_modificacao = NOW()
           WHERE codigo = $1
           RETURNING codigo::text AS codigo`,
          [codigo, situacao, dataEncerramento || null],
        )

        if (updateResult.rowCount === 0) {
          await client.query('ROLLBACK')
          sendJson(response, 404, { message: 'OrdemServico nao encontrada.' })
          return
        }

        await client.query('COMMIT')
        sendJson(response, 200, { codigo: updateResult.rows[0].codigo })
      } catch (error) {
        await client.query('ROLLBACK')
        throw error
      } finally {
        client.release()
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Erro ao atualizar OrdemServico.'
      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'DELETE' && getOrdemServicoCodigoFromUrl(pathname)) {
    try {
      const codigo = Number(getOrdemServicoCodigoFromUrl(pathname))

      if (!Number.isInteger(codigo) || codigo <= 0) {
        sendJson(response, 400, { message: 'Codigo invalido.' })
        return
      }

      const client = await pool.connect()

      try {
        await client.query('BEGIN')

        const deleteResult = await client.query(
          `DELETE FROM ${ordemServicoTableName}
           WHERE codigo = $1
           RETURNING codigo::text AS codigo`,
          [codigo],
        )

        if (deleteResult.rowCount === 0) {
          await client.query('ROLLBACK')
          sendJson(response, 404, { message: 'OrdemServico nao encontrada.' })
          return
        }

        await rebalanceOrdemServicoRevisions(client)
        await syncCondutorVinculosFromOrdemServico(client)
        await syncMonitorVinculosFromOrdemServico(client)

        await client.query('COMMIT')
        sendJson(response, 200, { deletedCodigo: deleteResult.rows[0].codigo })
      } catch (error) {
        await client.query('ROLLBACK')
        throw error
      } finally {
        client.release()
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Erro ao excluir OrdemServico.'
      sendJson(response, 500, { message })
    }

    return
  }

  sendJson(response, 404, { message: 'Rota nao encontrada.' })
})

server.on('error', async (error) => {
  if (error && typeof error === 'object' && 'code' in error && error.code === 'EADDRINUSE') {
    console.error(`Falha ao iniciar a API: a porta ${port} ja esta em uso.`)
  } else {
    console.error('Falha ao iniciar a API:', error)
  }

  await pool.end()
  process.exit(1)
})

ensureDatabaseSchema()
  .then(() => {
    return seedTrocaTableFromXmlIfEmpty()
  })
  .then(() => {
    return seedSeguradoraTableFromXmlIfEmpty()
  })
  .then(() => {
    return seedMarcaModeloTableFromXmlIfEmpty()
  })
  .then(() => {
    return seedTitularTableFromXmlIfEmpty()
  })
  .then(() => {
    return rebalanceOrdemServicoRevisions(pool)
  })
  .then(() => {
    server.listen(port, () => {
      console.log(`Auth API escutando na porta ${port}`)
    })
  })
  .catch(async (error) => {
    console.error('Falha ao preparar esquema do banco:', error)
    await pool.end()
    process.exit(1)
  })


