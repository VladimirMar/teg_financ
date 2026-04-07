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
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
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

const createToken = (email) => {
  return Buffer.from(`${email}:${Date.now()}`).toString('base64url')
}

const server = createServer(async (request, response) => {
  if (request.method === 'OPTIONS') {
    response.writeHead(204, {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Headers': 'Content-Type',
      'Access-Control-Allow-Methods': 'POST, OPTIONS',
    })
    response.end()
    return
  }

  if (request.method === 'POST' && request.url === '/api/auth/login') {
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

  sendJson(response, 404, { message: 'Rota nao encontrada.' })
})

server.listen(port, () => {
  console.log(`Auth API escutando na porta ${port}`)
})