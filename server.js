import { createServer } from 'node:http'
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

const getDreCodigoFromUrl = (url) => {
  const match = url.match(/^\/api\/dre\/([^/]+)$/)
  return match ? decodeURIComponent(match[1]) : null
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
        'SELECT email, password FROM login WHERE TRIM(email) = $1 LIMIT 1',
        [email],
      )

      if (result.rowCount === 0) {
        sendJson(response, 401, { message: 'Usuario ou senha invalidos.' })
        return
      }

      const dbUser = result.rows[0]
      if (normalizeDbValue(dbUser.password) !== password) {
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

  sendJson(response, 404, { message: 'Rota nao encontrada.' })
})

server.listen(port, () => {
  console.log(`Auth API escutando na porta ${port}`)
})