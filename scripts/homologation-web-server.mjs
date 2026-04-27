import { createServer } from 'node:http'
import { readFile, stat } from 'node:fs/promises'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const scriptDirectory = path.dirname(fileURLToPath(import.meta.url))
const projectRoot = path.dirname(scriptDirectory)

const resolveRuntimeRoot = () => {
  const configuredRuntimeRoot = String(process.env.HOMOL_RUNTIME_ROOT ?? '').trim()

  if (configuredRuntimeRoot) {
    return path.resolve(configuredRuntimeRoot)
  }

  return path.basename(projectRoot) === 'runtime' && path.basename(path.dirname(projectRoot)) === '.homolog'
    ? projectRoot
    : path.join(projectRoot, '.homolog', 'runtime')
}

const runtimeRoot = resolveRuntimeRoot()
const port = Number(process.env.HOMOL_WEB_PORT ?? 4173)
const host = process.env.HOMOL_WEB_HOST ?? '10.36.144.147'
const apiTarget = process.env.HOMOL_API_TARGET ?? 'http://127.0.0.1:3002'

const mimeTypes = {
  '.css': 'text/css; charset=utf-8',
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.jpeg': 'image/jpeg',
  '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon',
  '.webp': 'image/webp',
  '.woff': 'font/woff',
  '.woff2': 'font/woff2',
}

const isAllowedStaticPath = (pathname) => {
  return pathname === '/'
    || pathname === '/index.html'
    || pathname === '/favicon.svg'
    || pathname === '/icons.svg'
    || pathname === '/teste-prefeitura.svg'
    || pathname.startsWith('/assets/')
    || pathname.startsWith('/src/')
}

const sendBuffer = (response, statusCode, body, contentType) => {
  response.writeHead(statusCode, {
    'Content-Type': contentType,
    'Cache-Control': 'no-store',
  })
  response.end(body)
}

const sendText = (response, statusCode, message) => {
  sendBuffer(response, statusCode, Buffer.from(message, 'utf8'), 'text/plain; charset=utf-8')
}

const readRequestBody = (request) => new Promise((resolve, reject) => {
  const chunks = []

  request.on('data', (chunk) => {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk))
  })

  request.on('end', () => {
    resolve(Buffer.concat(chunks))
  })

  request.on('error', reject)
})

const proxyRequest = async (request, response, pathnameWithQuery) => {
  const targetUrl = new URL(pathnameWithQuery, apiTarget)
  const headers = new Headers()

  for (const [key, value] of Object.entries(request.headers)) {
    if (value == null) {
      continue
    }

    headers.set(key, Array.isArray(value) ? value.join(', ') : value)
  }

  headers.set('host', targetUrl.host)
  const method = request.method ?? 'GET'
  const body = method === 'GET' || method === 'HEAD'
    ? undefined
    : await readRequestBody(request)

  const upstreamResponse = await fetch(targetUrl, {
    method,
    headers,
    body,
  })

  const responseHeaders = {}
  upstreamResponse.headers.forEach((value, key) => {
    responseHeaders[key] = value
  })

  response.writeHead(upstreamResponse.status, responseHeaders)
  response.end(Buffer.from(await upstreamResponse.arrayBuffer()))
}

const resolveStaticFilePath = async (pathname) => {
  const decodedPath = decodeURIComponent(pathname)
  const normalizedPath = decodedPath === '/'
    ? '/index.html'
    : decodedPath

  if (!isAllowedStaticPath(normalizedPath)) {
    return path.join(runtimeRoot, 'index.html')
  }

  const filePath = path.join(runtimeRoot, normalizedPath.replace(/^\//, ''))
  const normalizedFilePath = path.normalize(filePath)

  if (!normalizedFilePath.startsWith(path.normalize(runtimeRoot))) {
    throw new Error('INVALID_PATH')
  }

  try {
    const fileStat = await stat(normalizedFilePath)

    if (fileStat.isFile()) {
      return normalizedFilePath
    }
  } catch {
    if (!path.extname(normalizedFilePath)) {
      return path.join(runtimeRoot, 'index.html')
    }
  }

  return path.join(runtimeRoot, 'index.html')
}

const server = createServer(async (request, response) => {
  try {
    const requestUrl = new URL(request.url ?? '/', `http://${request.headers.host ?? `${host}:${port}`}`)
    const pathnameWithQuery = `${requestUrl.pathname}${requestUrl.search}`

    if (requestUrl.pathname.startsWith('/api/')) {
      await proxyRequest(request, response, pathnameWithQuery)
      return
    }

    const filePath = await resolveStaticFilePath(requestUrl.pathname)
    const fileContent = await readFile(filePath)
    const contentType = mimeTypes[path.extname(filePath).toLowerCase()] ?? 'application/octet-stream'
    sendBuffer(response, 200, fileContent, contentType)
  } catch (error) {
    if (error instanceof Error && error.message === 'INVALID_PATH') {
      sendText(response, 403, 'Acesso negado.')
      return
    }

    sendText(response, 500, error instanceof Error ? error.message : 'Erro interno.')
  }
})

server.listen(port, host, () => {
  console.log(`Homologacao web disponivel em http://${host}:${port}`)
  console.log(`Proxy /api -> ${apiTarget}`)
})