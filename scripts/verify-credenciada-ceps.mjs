const baseUrl = process.env.API_BASE_URL ?? 'http://localhost:3001'

const fixtures = [
  {
    codigo: '3789',
    cep: '05861-260',
    logradouro: 'RUA COMENDADOR ANTUNES DOS SANTOS, 1809 CASA 189',
    bairro: 'CAPAO REDONDO',
    municipio: 'SÃO PAULO',
    uf: 'SP',
  },
  {
    codigo: '3434',
    cep: '05892-370',
    logradouro: 'R ENG FELIPE GUITON, 162 LOTE 5 QUADRA A',
    bairro: 'JD AMALIA',
    municipio: 'SÃO PAULO',
    uf: 'SP',
  },
]

const requestJson = async (path, options = {}) => {
  const response = await fetch(`${baseUrl}${path}`, {
    ...options,
    headers: {
      Accept: 'application/json',
      ...(options.body ? { 'Content-Type': 'application/json' } : {}),
      ...(options.headers ?? {}),
    },
  })

  const payload = await response.json().catch(() => null)

  if (!response.ok) {
    const message = payload && typeof payload.message === 'string'
      ? payload.message
      : `HTTP ${response.status} em ${path}`

    throw new Error(message)
  }

  return payload
}

const assert = (condition, message) => {
  if (!condition) {
    throw new Error(message)
  }
}

const normalize = (value) => String(value ?? '').trim()

const findCredenciadaByCode = async (codigo) => {
  const response = await requestJson(`/api/credenciada?page=1&pageSize=50&search=${encodeURIComponent(codigo)}`)
  return (response.items ?? []).find((item) => normalize(item.codigo) === normalize(codigo)) ?? null
}

const verifyFixtureCep = async (fixture) => {
  const credenciadaItem = await findCredenciadaByCode(fixture.codigo)
  assert(credenciadaItem, `Credenciada ${fixture.codigo} nao encontrada apos importacao.`)
  assert(normalize(credenciadaItem.cep) === fixture.cep, `Credenciada ${fixture.codigo} nao retornou o CEP esperado.`)

  const cepLookup = await requestJson(`/api/cep/lookup?cep=${encodeURIComponent(fixture.cep)}`)
  const cepItem = cepLookup.item ?? {}

  assert(normalize(cepItem.cep) === fixture.cep, `CEP ${fixture.cep} nao foi encontrado na base local.`)
  assert(normalize(cepItem.logradouro) === fixture.logradouro, `CEP ${fixture.cep} retornou logradouro divergente.`)
  assert(normalize(cepItem.bairro) === fixture.bairro, `CEP ${fixture.cep} retornou bairro divergente.`)
  assert(normalize(cepItem.municipio) === fixture.municipio, `CEP ${fixture.cep} retornou municipio divergente.`)
  assert(normalize(cepItem.uf) === fixture.uf, `CEP ${fixture.cep} retornou UF divergente.`)

  console.log(`- CEP ${fixture.cep} validado para credenciada ${fixture.codigo}`)
}

const main = async () => {
  console.log('Teste dirigido da importacao de Credenciados.xml para a tabela ceps')

  const importResult = await requestJson('/api/credenciada/import-xml', {
    method: 'POST',
    body: JSON.stringify({ fileName: 'Credenciados.xml' }),
  })

  assert(importResult.skipped === 0, 'Importacao de Credenciados.xml retornou recusas inesperadas.')
  assert(importResult.processed === importResult.total, 'Importacao de Credenciados.xml nao processou todos os registros.')
  assert((importResult.inserted + importResult.updated) === importResult.processed, 'Importacao de Credenciados.xml retornou contagem inconsistente.')
  console.log(`- importacao concluida: ${importResult.processed} processado(s), ${importResult.updated} alterado(s), ${importResult.inserted} incluido(s)`)

  for (const fixture of fixtures) {
    await verifyFixtureCep(fixture)
  }

  console.log('Teste dirigido concluido com sucesso.')
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error)
  process.exitCode = 1
})