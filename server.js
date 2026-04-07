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

const getAccessEmailFromUrl = (url) => {
  const match = url.match(/^\/api\/access\/([^/]+)$/)
  return match ? decodeURIComponent(match[1]) : null
}

const createAccessHashPayload = (password) => {
  const passwordHash = createPasswordHash(password)

  return {
    password: passwordHash,
    descricao: passwordHash,
  }
}

const createAccess = async (email, password) => {
  const existingUser = await pool.query(
    'SELECT 1 FROM login WHERE LOWER(TRIM(email)) = LOWER($1) LIMIT 1',
    [email],
  )

  if (existingUser.rowCount > 0) {
    return { status: 409, payload: { message: 'Email ja cadastrado.' } }
  }

  const passwordPayload = createAccessHashPayload(password)
  await pool.query(
    'INSERT INTO login (email, password, descricao) VALUES ($1, $2, $3)',
    [email, passwordPayload.password, passwordPayload.descricao],
  )

  return {
    status: 201,
    payload: {
      message: 'Acesso cadastrado com sucesso.',
      user: { email },
    },
  }
}

const ensureDatabaseSchema = async () => {
  await pool.query('ALTER TABLE login ADD COLUMN IF NOT EXISTS descricao text')
  await pool.query('CREATE UNIQUE INDEX IF NOT EXISTS login_email_unique_idx ON login (LOWER(TRIM(email)))')
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
      const sortDirection = normalizeRequestValue(requestUrl.searchParams.get('sortDirection') ?? 'asc').toLowerCase() === 'desc'
        ? 'DESC'
        : 'ASC'
      const offset = (page - 1) * pageSize
      const values = []
      const filters = []

      if (search) {
        values.push(`%${search}%`)
        filters.push(`LOWER(TRIM(email)) ILIKE LOWER($${values.length})`)
      }

      const whereClause = filters.length ? `WHERE ${filters.join(' AND ')}` : ''
      const countResult = await pool.query(
        `SELECT COUNT(*)::int AS total FROM login ${whereClause}`,
        values,
      )

      values.push(pageSize)
      values.push(offset)
      const result = await pool.query(
        `SELECT TRIM(email) AS email
         FROM login
         ${whereClause}
         ORDER BY LOWER(TRIM(email)) ${sortDirection}
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
        sortBy: 'email',
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
        'SELECT email, password, descricao FROM login WHERE LOWER(TRIM(email)) = LOWER($1) LIMIT 1',
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
      const email = normalizeRequestValue(body.email)
      const password = normalizeRequestValue(body.password)

      if (!email) {
        sendJson(response, 400, { message: 'Email e obrigatorio.' })
        return
      }

      if (!password) {
        sendJson(response, 400, { message: 'Senha e obrigatoria.' })
        return
      }

      const result = await createAccess(email, password)
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
      const email = normalizeRequestValue(body.email)
      const password = normalizeRequestValue(body.password)

      if (!email) {
        sendJson(response, 400, { message: 'Email e obrigatorio.' })
        return
      }

      if (!password) {
        sendJson(response, 400, { message: 'Senha e obrigatoria.' })
        return
      }

      const result = await createAccess(email, password)
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

  if (request.method === 'PUT' && getAccessEmailFromUrl(pathname)) {
    try {
      const originalEmail = getAccessEmailFromUrl(pathname)
      const body = await readJsonBody(request)
      const email = normalizeRequestValue(body.email)
      const password = normalizeRequestValue(body.password)

      if (!originalEmail) {
        sendJson(response, 400, { message: 'Email original invalido.' })
        return
      }

      if (!email) {
        sendJson(response, 400, { message: 'Email e obrigatorio.' })
        return
      }

      const existingResult = await pool.query(
        'SELECT email, password, descricao FROM login WHERE LOWER(TRIM(email)) = LOWER($1) LIMIT 1',
        [originalEmail],
      )

      if (existingResult.rowCount === 0) {
        sendJson(response, 404, { message: 'Acesso nao encontrado.' })
        return
      }

      const duplicateEmailResult = await pool.query(
        'SELECT 1 FROM login WHERE LOWER(TRIM(email)) = LOWER($1) AND LOWER(TRIM(email)) <> LOWER($2) LIMIT 1',
        [email, originalEmail],
      )

      if (duplicateEmailResult.rowCount > 0) {
        sendJson(response, 409, { message: 'Email ja cadastrado.' })
        return
      }

      const currentUser = existingResult.rows[0]
      const passwordPayload = password
        ? createAccessHashPayload(password)
        : {
            password: currentUser.password,
            descricao: currentUser.descricao,
          }

      const updateResult = await pool.query(
        'UPDATE login SET email = $1, password = $2, descricao = $3 WHERE LOWER(TRIM(email)) = LOWER($4) RETURNING TRIM(email) AS email',
        [email, passwordPayload.password, passwordPayload.descricao, originalEmail],
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

  if (request.method === 'DELETE' && getAccessEmailFromUrl(pathname)) {
    try {
      const email = getAccessEmailFromUrl(pathname)

      if (!email) {
        sendJson(response, 400, { message: 'Email invalido para exclusao.' })
        return
      }

      const deleteResult = await pool.query(
        'DELETE FROM login WHERE LOWER(TRIM(email)) = LOWER($1) RETURNING TRIM(email) AS email',
        [email],
      )

      if (deleteResult.rowCount === 0) {
        sendJson(response, 404, { message: 'Acesso nao encontrado.' })
        return
      }

      sendJson(response, 200, {
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