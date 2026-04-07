import { createServer } from 'node:http'
import { randomBytes, scryptSync, timingSafeEqual } from 'node:crypto'
import { Pool } from 'pg'

const port = Number(process.env.PORT ?? 3001)

const pool = new Pool({
  host: process.env.PGHOST ?? 'localhost',
  port: Number(process.env.PGPORT ?? 5432),
  user: process.env.PGUSER ?? 'postgres',
  password: process.env.PGPASSWORD ?? '12345',
  database: process.env.PGDATABASE ?? 'teg_financ',
})

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
    } catch {
      reject(new Error('JSON invalido no corpo da requisicao.'))
    }
  })

  request.on('error', reject)
})

const normalizeDbValue = (value) => {
  return typeof value === 'string' ? value.trim() : ''
}

const normalizeRequestValue = (value) => {
  return typeof value === 'string' ? value.trim() : ''
}

const normalizeAccessName = (value) => {
  return normalizeRequestValue(value)
    .toUpperCase()
    .replace(/\s+/g, ' ')
}

const isAccessNameValid = (value) => {
  return /^[A-ZÀ-Ý ]{1,50}$/.test(value)
}

const buildGeneratedAccessName = (email, sequenceNumber) => {
  const emailBaseName = normalizeAccessName(email.split('@')[0] ?? '')
    .replace(/[^A-ZÀ-Ý ]/g, '')
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

const getAccessCodigoFromUrl = (url) => {
  const match = url.match(/^\/api\/access\/([^/]+)$/)
  return match ? decodeURIComponent(match[1]) : null
}

const getCondutorCodigoFromUrl = (url) => {
  const match = url.match(/^\/api\/condutor\/([^/]+)$/)
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
    .toUpperCase()
    .replace(/\s+/g, ' ')
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
  return /^[A-ZÀ-Ý ]{1,100}$/.test(value)
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
    return 'Sócio'
  }

  if (normalizedKey === 'funcionario') {
    return 'Funcionário'
  }

  return null
}

const normalizeHistorico = (value) => {
  return normalizeRequestValue(value).slice(0, 200)
}

const isDateInputValid = (value) => {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    return false
  }

  const parsed = new Date(`${value}T00:00:00`)
  return !Number.isNaN(parsed.getTime())
}

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

  if (!normalizedValidadeCurso) {
    return { status: 400, payload: { message: 'Validade do curso e obrigatoria.' } }
  }

  if (!isDateInputValid(normalizedValidadeCurso)) {
    return { status: 400, payload: { message: 'Validade do curso invalida.' } }
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
    'SELECT CAST(codigo AS text) AS codigo, BTRIM(CAST(descricao AS text)) AS descricao FROM dre WHERE codigo = $1 LIMIT 1',
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
            COALESCE(NULLIF(REGEXP_REPLACE(UPPER(SPLIT_PART(email, '@', 1)), '[^A-ZÀ-Ý ]', '', 'g'), ''), 'USUARIO'),
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
  await pool.query('CREATE SEQUENCE IF NOT EXISTS condutor_codigo_seq START WITH 1 INCREMENT BY 1')
  await pool.query('ALTER TABLE condutor ALTER COLUMN codigo SET DEFAULT nextval(\'condutor_codigo_seq\')')
  await pool.query('ALTER SEQUENCE condutor_codigo_seq OWNED BY condutor.codigo')
  await pool.query('SELECT setval(\'condutor_codigo_seq\', GREATEST(COALESCE((SELECT MAX(codigo) FROM condutor), 0), 1), true)')
  await pool.query('CREATE UNIQUE INDEX IF NOT EXISTS login_codigo_unique_idx ON login (codigo)')
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
      'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
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
        `SELECT COUNT(*)::int AS total FROM dre ${whereClause}`,
        values,
      )

      values.push(pageSize)
      values.push(offset)
      const result = await pool.query(
        `SELECT CAST(codigo AS text) AS codigo, BTRIM(CAST(descricao AS text)) AS descricao
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
      const orderByClause = sortBy === 'nome'
        ? `UPPER(BTRIM(nome)) ${sortDirection}, codigo ASC`
        : sortBy === 'email'
          ? `LOWER(TRIM(email)) ${sortDirection}, codigo ASC`
          : `codigo ${sortDirection}`

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
           codigo::text AS codigo,
           BTRIM(condutor) AS condutor,
           BTRIM(cpf_condutor) AS cpf_condutor,
           BTRIM(crmc) AS crmc,
           TO_CHAR(validade_crmc::date, 'YYYY-MM-DD') AS validade_crmc,
           TO_CHAR(validade_curso::date, 'YYYY-MM-DD') AS validade_curso,
           COALESCE(BTRIM(tipo_vinculo), '') AS tipo_vinculo,
           COALESCE(BTRIM(historico), '') AS historico
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
      const codigo = normalizeRequestValue(body.codigo)
      const descricao = normalizeRequestValue(body.descricao)

      if (!codigo) {
        sendJson(response, 400, { message: 'Codigo e obrigatorio.' })
        return
      }

      if (!descricao) {
        sendJson(response, 400, { message: 'Descricao e obrigatoria.' })
        return
      }

      const duplicateCodeResult = await pool.query(
        'SELECT 1 FROM dre WHERE CAST(codigo AS text) = $1 LIMIT 1',
        [codigo],
      )

      if (duplicateCodeResult.rowCount > 0) {
        sendJson(response, 409, { message: 'Codigo ja cadastrado.' })
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

      const insertResult = await pool.query(
        'INSERT INTO dre (codigo, descricao) VALUES ($1, $2) RETURNING CAST(codigo AS text) AS codigo, BTRIM(CAST(descricao AS text)) AS descricao',
        [codigo, descricao],
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
      const validationResult = await validateCondutorPayload({
        codigo: body.codigo,
        condutor: body.condutor,
        cpfCondutor: body.cpfCondutor,
        crmc: body.crmc,
        validadeCrmc: body.validadeCrmc,
        validadeCurso: body.validadeCurso,
        tipoVinculo: body.tipoVinculo,
        historico: body.historico,
      })

      if (validationResult.status !== 200) {
        sendJson(response, validationResult.status, validationResult.payload)
        return
      }

      const insertResult = await pool.query(
        `INSERT INTO condutor (codigo, condutor, cpf_condutor, crmc, validade_crmc, validade_curso, tipo_vinculo, historico)
         VALUES ($1, $2, $3, $4, $5::date, $6::date, NULLIF($7, ''), NULLIF($8, ''))
         RETURNING
           codigo::text AS codigo,
           BTRIM(condutor) AS condutor,
           BTRIM(cpf_condutor) AS cpf_condutor,
           BTRIM(crmc) AS crmc,
           TO_CHAR(validade_crmc::date, 'YYYY-MM-DD') AS validade_crmc,
           TO_CHAR(validade_curso::date, 'YYYY-MM-DD') AS validade_curso,
           COALESCE(BTRIM(tipo_vinculo), '') AS tipo_vinculo,
           COALESCE(BTRIM(historico), '') AS historico`,
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

      sendJson(response, 201, {
        item: insertResult.rows[0],
      })
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Erro ao cadastrar condutor.'

      sendJson(response, 500, { message })
    }

    return
  }

  if (request.method === 'PUT' && getDreCodigoFromUrl(pathname)) {
    try {
      const originalCodigo = getDreCodigoFromUrl(pathname)
      const body = await readJsonBody(request)
      const codigo = normalizeRequestValue(body.codigo)
      const descricao = normalizeRequestValue(body.descricao)

      if (!originalCodigo) {
        sendJson(response, 400, { message: 'Codigo original invalido.' })
        return
      }

      if (!codigo) {
        sendJson(response, 400, { message: 'Codigo e obrigatorio.' })
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

      const duplicateCodeResult = await pool.query(
        'SELECT 1 FROM dre WHERE CAST(codigo AS text) = $1 AND CAST(codigo AS text) <> $2 LIMIT 1',
        [codigo, originalCodigo],
      )

      if (duplicateCodeResult.rowCount > 0) {
        sendJson(response, 409, { message: 'Codigo ja cadastrado.' })
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

      const updateResult = await pool.query(
        'UPDATE dre SET codigo = $1, descricao = $2 WHERE CAST(codigo AS text) = $3 RETURNING CAST(codigo AS text) AS codigo, BTRIM(CAST(descricao AS text)) AS descricao',
        [codigo, descricao, originalCodigo],
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
             historico = NULLIF($8, '')
         WHERE codigo = $9
         RETURNING
           codigo::text AS codigo,
           BTRIM(condutor) AS condutor,
           BTRIM(cpf_condutor) AS cpf_condutor,
           BTRIM(crmc) AS crmc,
           TO_CHAR(validade_crmc::date, 'YYYY-MM-DD') AS validade_crmc,
           TO_CHAR(validade_curso::date, 'YYYY-MM-DD') AS validade_curso,
           COALESCE(BTRIM(tipo_vinculo), '') AS tipo_vinculo,
           COALESCE(BTRIM(historico), '') AS historico`,
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

  sendJson(response, 404, { message: 'Rota nao encontrada.' })
})

ensureDatabaseSchema()
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