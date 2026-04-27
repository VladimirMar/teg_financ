import { access } from 'node:fs/promises'
import path from 'node:path'
import { spawn } from 'node:child_process'
import { fileURLToPath } from 'node:url'

const scriptDirectory = path.dirname(fileURLToPath(import.meta.url))
const projectRoot = path.dirname(scriptDirectory)

const resolveRuntimeRoot = async () => {
  const configuredRuntimeRoot = String(process.env.HOMOL_RUNTIME_ROOT ?? '').trim()

  if (configuredRuntimeRoot) {
    return path.resolve(configuredRuntimeRoot)
  }

  try {
    await access(path.join(projectRoot, '.homolog-runtime.json'))
    return projectRoot
  } catch {
    return path.join(projectRoot, '.homolog', 'runtime')
  }
}

const main = async () => {
  const runtimeRoot = await resolveRuntimeRoot()
  const runtimeServerPath = path.join(runtimeRoot, 'server.js')
  await access(runtimeServerPath)

  const child = spawn(process.execPath, [runtimeServerPath], {
    cwd: runtimeRoot,
    stdio: 'inherit',
    env: {
      ...process.env,
      NODE_ENV: process.env.NODE_ENV ?? 'production',
      PGHOST: process.env.PGHOST ?? 'localhost',
      PGPORT: process.env.PGPORT ?? '5432',
      PGUSER: process.env.PGUSER ?? 'postgres',
      PGPASSWORD: process.env.PGPASSWORD ?? '12345',
      PGDATABASE: process.env.PGDATABASE ?? 'teg_financ_homol',
      PORT: process.env.PORT ?? '3002',
    },
  })

  child.on('exit', (code, signal) => {
    if (signal) {
      process.kill(process.pid, signal)
      return
    }

    process.exit(code ?? 0)
  })
}

main().catch((error) => {
  console.error('Execute "npm run homol:build" antes de iniciar a API de homologacao.')
  console.error(error instanceof Error ? error.message : error)
  process.exitCode = 1
})