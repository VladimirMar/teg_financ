import { mkdir, readFile, writeFile } from 'node:fs/promises'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { XMLParser } from 'fast-xml-parser'
import { Pool } from 'pg'

const workspaceRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..')
const importXmlDirectory = path.join(workspaceRoot, 'importXML')
const defaultFileName = process.env.ORDEM_SERVICO_XML_FILE ?? 'OrdemServico.xml'
const reportPath = process.env.ORDEM_SERVICO_VALIDATION_REPORT_PATH
  ?? path.join(importXmlDirectory, 'ordem_servico_validation_summary.json')
const shouldCheckDbReferences = /^(1|true|yes)$/i.test(process.env.CHECK_DB_REFERENCES ?? '')
const requestedFileName = String(process.argv[2] ?? defaultFileName).trim()
const resolvedXmlPath = path.resolve(importXmlDirectory, path.basename(requestedFileName))

const pool = shouldCheckDbReferences
  ? new Pool({
    host: process.env.PGHOST ?? 'localhost',
    port: Number(process.env.PGPORT ?? 5432),
    user: process.env.PGUSER ?? 'postgres',
    password: process.env.PGPASSWORD ?? '12345',
    database: process.env.PGDATABASE ?? 'teg_financ',
  })
  : null

if (!resolvedXmlPath.startsWith(importXmlDirectory)) {
  throw new Error('Arquivo XML invalido.')
}

const parser = new XMLParser({
  ignoreAttributes: false,
  trimValues: true,
})

const normalizeValue = (value) => value === null || value === undefined ? '' : String(value).trim()
const normalizeDigits = (value) => normalizeValue(value).replace(/\D/g, '')

const normalizeCpf = (value) => {
  const digits = normalizeDigits(value).slice(0, 11)

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

const normalizeCnpjCpf = (value) => {
  const digits = normalizeDigits(value)

  if (digits.length <= 11) {
    return normalizeCpf(digits)
  }

  const trimmed = digits.slice(0, 14)

  if (trimmed.length <= 2) {
    return trimmed
  }

  if (trimmed.length <= 5) {
    return `${trimmed.slice(0, 2)}.${trimmed.slice(2)}`
  }

  if (trimmed.length <= 8) {
    return `${trimmed.slice(0, 2)}.${trimmed.slice(2, 5)}.${trimmed.slice(5)}`
  }

  if (trimmed.length <= 12) {
    return `${trimmed.slice(0, 2)}.${trimmed.slice(2, 5)}.${trimmed.slice(5, 8)}/${trimmed.slice(8)}`
  }

  return `${trimmed.slice(0, 2)}.${trimmed.slice(2, 5)}.${trimmed.slice(5, 8)}/${trimmed.slice(8, 12)}-${trimmed.slice(12, 14)}`
}

const normalizeCrm = (value) => {
  const digits = normalizeDigits(value).slice(0, 8)

  if (digits.length <= 3) {
    return digits
  }

  if (digits.length <= 6) {
    return `${digits.slice(0, 3)}.${digits.slice(3)}`
  }

  return `${digits.slice(0, 3)}.${digits.slice(3, 6)}-${digits.slice(6, 8)}`
}

const normalizeOperationalCode = (value, maxLength) => {
  return normalizeValue(value)
    .replace(/\s+/g, ' ')
    .slice(0, maxLength)
}

const extractNumOs = (value) => {
  const normalizedValue = normalizeValue(value).toUpperCase()
  const match = normalizedValue.match(/-(\d+)[A-Z]*$/)

  return match ? match[1].slice(0, 10) : ''
}

const normalizeSituacao = (value) => {
  const normalizedKey = normalizeValue(value)
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z]/g, '')

  if (normalizedKey === 'ativo') return 'Ativo'
  if (normalizedKey === 'inativo') return 'Inativo'
  if (normalizedKey === 'cancelado') return 'Cancelado'
  return ''
}

const normalizeXmlDateInput = (value) => {
  const normalizedValue = normalizeValue(value)

  if (!normalizedValue) {
    return ''
  }

  const onlyDateMatch = normalizedValue.match(/^(\d{4})-(\d{2})-(\d{2})/)
  if (onlyDateMatch) {
    return `${onlyDateMatch[1]}-${onlyDateMatch[2]}-${onlyDateMatch[3]}`
  }

  const brazilianDateMatch = normalizedValue.match(/^(\d{2})\/(\d{2})\/(\d{4})$/)
  if (brazilianDateMatch) {
    return `${brazilianDateMatch[3]}-${brazilianDateMatch[2]}-${brazilianDateMatch[1]}`
  }

  return normalizedValue
}

const isPositiveIntegerString = (value) => /^\d+$/.test(value) && Number(value) > 0
const isDateInputValid = (value) => /^\d{4}-\d{2}-\d{2}$/.test(value) && !Number.isNaN(new Date(`${value}T00:00:00`).getTime())
const isCpfValid = (value) => normalizeDigits(value).length === 11
const isCnpjCpfValid = (value) => {
  const digits = normalizeDigits(value)
  return digits.length === 11 || digits.length === 14
}
const isVehicleCrmValid = (value) => /^\d{3}\.\d{3}-\d{2}$/.test(value)

const findCredenciadaByCnpjCpf = async (cnpjCpf) => {
  if (!pool) return null

  const digits = normalizeDigits(cnpjCpf)

  if (!digits) {
    return null
  }

  const result = await pool.query(
    `SELECT codigo::text AS codigo
     FROM credenciada
     WHERE regexp_replace(COALESCE(cnpj_cpf, ''), '[^0-9]', '', 'g') = $1
     ORDER BY codigo ASC
     LIMIT 1`,
    [digits],
  )

  return result.rows[0] ?? null
}

const findCredenciadaByName = async (credenciado) => {
  if (!pool) return null

  const normalizedCredenciado = normalizeOperationalCode(credenciado, 255).toUpperCase()

  if (!normalizedCredenciado) {
    return null
  }

  const result = await pool.query(
    `SELECT codigo::text AS codigo
     FROM credenciada
     WHERE UPPER(BTRIM(credenciado)) = $1
     ORDER BY codigo ASC
     LIMIT 1`,
    [normalizedCredenciado],
  )

  return result.rows[0] ?? null
}

const findDreByCodigo = async (codigo) => {
  if (!pool) return null

  const normalizedCodigo = normalizeOperationalCode(codigo, 30).toUpperCase()

  if (!normalizedCodigo) {
    return null
  }

  const result = await pool.query(
    `SELECT codigo::text AS codigo
     FROM dre
     WHERE CAST(codigo AS text) = $1
        OR UPPER(BTRIM(COALESCE(codigo_operacional, ''))) = $1
     LIMIT 1`,
    [normalizedCodigo],
  )

  return result.rows[0] ?? null
}

const findCondutorByCpf = async (cpfCondutor) => {
  if (!pool) return null

  const digits = normalizeDigits(cpfCondutor)

  if (!digits) {
    return null
  }

  const result = await pool.query(
    `SELECT codigo::text AS codigo
     FROM condutor
     WHERE regexp_replace(COALESCE(cpf_condutor, ''), '[^0-9]', '', 'g') = $1
     ORDER BY codigo ASC
     LIMIT 1`,
    [digits],
  )

  return result.rows[0] ?? null
}

const findMonitorByCpf = async (cpfMonitor) => {
  if (!pool) return null

  const digits = normalizeDigits(cpfMonitor)

  if (!digits) {
    return null
  }

  const result = await pool.query(
    `SELECT codigo::text AS codigo
     FROM monitor
     WHERE regexp_replace(COALESCE(cpf_monitor, ''), '[^0-9]', '', 'g') = $1
     ORDER BY codigo ASC
     LIMIT 1`,
    [digits],
  )

  return result.rows[0] ?? null
}

const findVeiculoByCrm = async (crm) => {
  if (!pool) return null

  const normalizedCrm = normalizeCrm(crm)

  if (!normalizedCrm) {
    return null
  }

  const result = await pool.query(
    `SELECT codigo::text AS codigo
     FROM veiculo
     WHERE UPPER(BTRIM(COALESCE(crm, ''))) = UPPER($1)
     ORDER BY codigo ASC
     LIMIT 1`,
    [normalizedCrm],
  )

  return result.rows[0] ?? null
}

const createError = ({ rowNumber, codigo, os, num_os, field, message, category }) => ({
  rowNumber,
  codigo,
  os,
  num_os,
  field,
  message,
  category,
})

const xmlContent = await readFile(resolvedXmlPath, 'utf8')
const parsed = parser.parse(xmlContent)
const rawRecords = parsed?.dataroot?.OrdemServico
const records = (Array.isArray(rawRecords) ? rawRecords : rawRecords ? [rawRecords] : [])
  .filter((record) => record && typeof record === 'object')

const errors = []
const seenCodes = new Map()

for (const [index, record] of records.entries()) {
  const rowNumber = index + 1
  const codigo = normalizeValue(record?.['Código'])
  const os = normalizeOperationalCode(record?.OS, 255)
  const num_os = extractNumOs(os)
  const vigenciaOs = normalizeXmlDateInput(record?.Vigencia_da_OS)
  const credenciado = normalizeOperationalCode(record?.Credenciado, 255)
  const cnpjCpf = normalizeCnpjCpf(record?.CNPJ_CPF)
  const dreCodigo = normalizeOperationalCode(record?.DRE, 30).toUpperCase()
  const cpfCondutor = normalizeCpf(record?.CPF_condutor)
  const crm = normalizeCrm(record?.CRM)
  const cpfMonitor = normalizeCpf(record?.CPF_monitor)
  const situacao = normalizeSituacao(record?.Situacao_de_OS)
  const dataEncerramento = normalizeXmlDateInput(record?.Data_de_encerramento)
  const anotacao = normalizeValue(record?.['Anotação'])

  if (!codigo) {
    errors.push(createError({ rowNumber, codigo, os, num_os, field: 'Código', message: 'Codigo e obrigatorio.', category: 'structural' }))
  } else if (!isPositiveIntegerString(codigo)) {
    errors.push(createError({ rowNumber, codigo, os, num_os, field: 'Código', message: 'Codigo deve ser um numero inteiro positivo.', category: 'structural' }))
  } else if (seenCodes.has(codigo)) {
    errors.push(createError({ rowNumber, codigo, os, num_os, field: 'Código', message: `Codigo duplicado no XML. Primeira ocorrencia na linha ${seenCodes.get(codigo)}.`, category: 'structural' }))
  } else {
    seenCodes.set(codigo, rowNumber)
  }

  if (!os) {
    errors.push(createError({ rowNumber, codigo, os, num_os, field: 'OS', message: 'Numero da OS e obrigatorio.', category: 'structural' }))
  }

  if (vigenciaOs && !isDateInputValid(vigenciaOs)) {
    errors.push(createError({ rowNumber, codigo, os, num_os, field: 'Vigencia_da_OS', message: 'Vigencia da OS invalida.', category: 'structural' }))
  }

  if (!credenciado && !cnpjCpf) {
    errors.push(createError({ rowNumber, codigo, os, num_os, field: 'Credenciado/CNPJ_CPF', message: 'Credenciado ou CNPJ/CPF e obrigatorio.', category: 'structural' }))
  }

  if (cnpjCpf && !isCnpjCpfValid(cnpjCpf)) {
    errors.push(createError({ rowNumber, codigo, os, num_os, field: 'CNPJ_CPF', message: 'CNPJ/CPF invalido.', category: 'structural' }))
  }

  if (!dreCodigo) {
    errors.push(createError({ rowNumber, codigo, os, num_os, field: 'DRE', message: 'DRE e obrigatoria.', category: 'structural' }))
  }

  if (cpfCondutor && !isCpfValid(cpfCondutor)) {
    errors.push(createError({ rowNumber, codigo, os, num_os, field: 'CPF_condutor', message: 'CPF do condutor invalido.', category: 'structural' }))
  }

  if (crm && !isVehicleCrmValid(crm)) {
    errors.push(createError({ rowNumber, codigo, os, num_os, field: 'CRM', message: 'CRM do veiculo invalido.', category: 'structural' }))
  }

  if (cpfMonitor && !isCpfValid(cpfMonitor)) {
    errors.push(createError({ rowNumber, codigo, os, num_os, field: 'CPF_monitor', message: 'CPF do monitor invalido.', category: 'structural' }))
  }

  if (!situacao) {
    errors.push(createError({ rowNumber, codigo, os, num_os, field: 'Situacao_de_OS', message: 'Situacao da OS invalida.', category: 'structural' }))
  }

  if (dataEncerramento && !isDateInputValid(dataEncerramento)) {
    errors.push(createError({ rowNumber, codigo, os, num_os, field: 'Data_de_encerramento', message: 'Data de encerramento invalida.', category: 'structural' }))
  }

  if (anotacao.length > 1000) {
    errors.push(createError({ rowNumber, codigo, os, num_os, field: 'Anotação', message: 'Anotacao excede o limite de 1000 caracteres.', category: 'structural' }))
  }

  if (shouldCheckDbReferences) {
    const credenciadaItem = cnpjCpf
      ? await findCredenciadaByCnpjCpf(cnpjCpf)
      : await findCredenciadaByName(credenciado)

    if (!credenciadaItem) {
      errors.push(createError({ rowNumber, codigo, os, num_os, field: 'Credenciado/CNPJ_CPF', message: 'Credenciado nao encontrado na tabela credenciada.', category: 'reference' }))
    }

    if (!await findDreByCodigo(dreCodigo)) {
      errors.push(createError({ rowNumber, codigo, os, num_os, field: 'DRE', message: 'DRE nao encontrada.', category: 'reference' }))
    }

    if (cpfCondutor && !await findCondutorByCpf(cpfCondutor)) {
      errors.push(createError({ rowNumber, codigo, os, num_os, field: 'CPF_condutor', message: 'CPF do condutor nao encontrado na tabela condutor.', category: 'reference' }))
    }

    if (crm && !await findVeiculoByCrm(crm)) {
      errors.push(createError({ rowNumber, codigo, os, num_os, field: 'CRM', message: 'CRM nao encontrado na tabela veiculo.', category: 'reference' }))
    }

    if (cpfMonitor && !await findMonitorByCpf(cpfMonitor)) {
      errors.push(createError({ rowNumber, codigo, os, num_os, field: 'CPF_monitor', message: 'CPF do monitor nao encontrado na tabela monitor.', category: 'reference' }))
    }
  }
}

const structuralErrors = errors.filter((item) => item.category === 'structural')
const referenceErrors = errors.filter((item) => item.category === 'reference')

const summary = {
  fileName: path.basename(resolvedXmlPath),
  filePath: resolvedXmlPath,
  total: records.length,
  valid: errors.length === 0,
  structuralValid: structuralErrors.length === 0,
  referenceValid: referenceErrors.length === 0,
  invalidRecords: new Set(errors.map((item) => item.rowNumber)).size,
  errorCount: errors.length,
  structuralErrorCount: structuralErrors.length,
  referenceErrorCount: referenceErrors.length,
  checkedAt: new Date().toISOString(),
  checkedDbReferences: shouldCheckDbReferences,
  errors: errors.slice(0, 500),
}

await mkdir(path.dirname(reportPath), { recursive: true })
await writeFile(reportPath, `${JSON.stringify(summary, null, 2)}\n`, 'utf8')

if (pool) {
  await pool.end()
}

console.log(JSON.stringify({
  fileName: summary.fileName,
  total: summary.total,
  valid: summary.valid,
  structuralValid: summary.structuralValid,
  referenceValid: summary.referenceValid,
  invalidRecords: summary.invalidRecords,
  errorCount: summary.errorCount,
  structuralErrorCount: summary.structuralErrorCount,
  referenceErrorCount: summary.referenceErrorCount,
  reportPath,
}, null, 2))

process.exit(summary.valid ? 0 : 1)
