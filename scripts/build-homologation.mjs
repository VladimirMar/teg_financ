import { cp, mkdir, rm, stat, writeFile } from 'node:fs/promises'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const scriptDirectory = path.dirname(fileURLToPath(import.meta.url))
const projectRoot = path.dirname(scriptDirectory)
const distRoot = path.join(projectRoot, 'dist')
const runtimeRoot = path.join(projectRoot, '.homolog', 'runtime')
const controlRoot = path.join(projectRoot, '.homolog', 'control')

const copyIfPresent = async (sourcePath, targetPath) => {
  try {
    await stat(sourcePath)
  } catch {
    return
  }

  await cp(sourcePath, targetPath, { recursive: true, force: true })
}

const main = async () => {
  await stat(distRoot)
  await rm(runtimeRoot, { recursive: true, force: true })
  await rm(controlRoot, { recursive: true, force: true })
  await mkdir(runtimeRoot, { recursive: true })
  await mkdir(controlRoot, { recursive: true })

  const distEntries = [
    'assets',
    'favicon.svg',
    'icons.svg',
    'index.html',
    'teste-prefeitura.svg',
  ]

  for (const entry of distEntries) {
    const sourcePath = path.join(distRoot, entry)
    const targetPath = path.join(runtimeRoot, entry)
    await copyIfPresent(sourcePath, targetPath)
  }

  const topLevelEntries = [
    'server.js',
    'package.json',
    'README.md',
    '.env.example',
  ]

  for (const entry of topLevelEntries) {
    const sourcePath = path.join(projectRoot, entry)
    const targetPath = path.join(runtimeRoot, entry)
    await copyIfPresent(sourcePath, targetPath)
  }

  const runtimeDirectories = [
    'src',
    'public',
    'documento',
    'importXML',
    'scripts',
  ]

  for (const directoryName of runtimeDirectories) {
    const sourcePath = path.join(projectRoot, directoryName)
    const targetPath = path.join(runtimeRoot, directoryName)
    await copyIfPresent(sourcePath, targetPath)
  }

  await mkdir(path.join(runtimeRoot, '.artifacts'), { recursive: true })
  await copyIfPresent(path.join(projectRoot, 'scripts', 'homologation-control'), controlRoot)
  await writeFile(path.join(runtimeRoot, '.homolog-runtime.json'), `${JSON.stringify({
    environment: 'homolog',
    apiPort: 3002,
    webPort: 4173,
    database: 'teg_financ_homol',
  }, null, 2)}\n`, 'utf8')

  console.log(`Ambiente de homologacao preparado em ${runtimeRoot}`)
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error)
  process.exitCode = 1
})