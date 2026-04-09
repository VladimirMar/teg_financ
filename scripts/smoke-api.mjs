import { mkdir, writeFile } from 'node:fs/promises'
import { dirname } from 'node:path'

const baseUrl = process.env.API_BASE_URL ?? 'http://localhost:3001'
const reportPath = process.env.SMOKE_REPORT_PATH ?? ''
const availableSuites = new Set(['all', 'condutor', 'credenciada', 'veiculo', 'marca-modelo'])
const suite = (process.argv[2] ?? process.env.SMOKE_SUITE ?? 'all').trim().toLowerCase()

const report = {
  requestedSuite: suite,
  status: 'running',
  startedAt: new Date().toISOString(),
  finishedAt: null,
  executedSuites: [],
  failureMessage: '',
}

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

const compareStrings = (left, right) => left.localeCompare(right, 'pt-BR', { sensitivity: 'base' })

const compareNumericStrings = (left, right) => {
  const normalizedLeft = String(left ?? '').trim()
  const normalizedRight = String(right ?? '').trim()
  const leftIsNumeric = /^\d+$/.test(normalizedLeft)
  const rightIsNumeric = /^\d+$/.test(normalizedRight)

  if (leftIsNumeric && rightIsNumeric) {
    return Number(normalizedLeft) - Number(normalizedRight)
  }

  if (leftIsNumeric && !rightIsNumeric) {
    return -1
  }

  if (!leftIsNumeric && rightIsNumeric) {
    return 1
  }

  return compareStrings(normalizedLeft, normalizedRight)
}

const expectSortedBy = (items, fieldName, direction) => {
  for (let index = 1; index < items.length; index += 1) {
    const previousValue = String(items[index - 1]?.[fieldName] ?? '').trim()
    const currentValue = String(items[index]?.[fieldName] ?? '').trim()
    const comparison = compareStrings(previousValue, currentValue)

    if (direction === 'asc') {
      assert(comparison <= 0, `${fieldName} fora de ordem crescente na posicao ${index + 1}.`)
      continue
    }

    assert(comparison >= 0, `${fieldName} fora de ordem decrescente na posicao ${index + 1}.`)
  }
}

const expectSortedByNumericString = (items, fieldName, direction) => {
  for (let index = 1; index < items.length; index += 1) {
    const previousValue = String(items[index - 1]?.[fieldName] ?? '').trim()
    const currentValue = String(items[index]?.[fieldName] ?? '').trim()
    const comparison = compareNumericStrings(previousValue, currentValue)

    if (direction === 'asc') {
      assert(comparison <= 0, `${fieldName} fora de ordem numerica crescente na posicao ${index + 1}.`)
      continue
    }

    assert(comparison >= 0, `${fieldName} fora de ordem numerica decrescente na posicao ${index + 1}.`)
  }
}

const logStep = (message) => {
  console.log(`- ${message}`)
}

const recordSuite = (suiteName) => {
  const suiteReport = {
    name: suiteName,
    status: 'running',
    startedAt: new Date().toISOString(),
    finishedAt: null,
    imports: [],
  }

  report.executedSuites.push(suiteReport)
  return suiteReport
}

const recordImport = (suiteReport, label, payload) => {
  suiteReport.imports.push({
    label,
    fileName: payload.fileName,
    total: payload.total,
    processed: payload.processed,
    inserted: payload.inserted,
    updated: payload.updated,
    skipped: payload.skipped,
    skippedRecords: Array.isArray(payload.skippedRecords)
      ? payload.skippedRecords.map((item) => ({
          index: item.index,
          codigoXml: item.codigoXml,
          message: item.message,
        }))
      : [],
  })
}

const finalizeSuite = (suiteReport, status) => {
  suiteReport.status = status
  suiteReport.finishedAt = new Date().toISOString()
}

const writeReportIfNeeded = async () => {
  if (!reportPath) {
    return
  }

  await mkdir(dirname(reportPath), { recursive: true })
  await writeFile(reportPath, `${JSON.stringify(report, null, 2)}\n`, 'utf8')
}

if (!availableSuites.has(suite)) {
  console.error(`Suite invalida: ${suite}. Use uma destas: ${Array.from(availableSuites).join(', ')}.`)
  process.exit(1)
}

const runCondutorSmoke = async () => {
  console.log('Smoke test da API Condutor')
  const suiteReport = recordSuite('condutor')

  try {
    const listResponse = await requestJson('/api/condutor?page=1&pageSize=5')
    assert(Array.isArray(listResponse.items), 'Listagem de condutor nao retornou items.')
    assert(listResponse.total > 0, 'Listagem de condutor retornou total zerado.')
    logStep(`listagem inicial ok com ${listResponse.total} registro(s)`)

    const ascResponse = await requestJson('/api/condutor?page=1&pageSize=5&sortBy=condutor&sortDirection=asc')
    const descResponse = await requestJson('/api/condutor?page=1&pageSize=5&sortBy=condutor&sortDirection=desc')
    expectSortedBy(ascResponse.items, 'condutor', 'asc')
    expectSortedBy(descResponse.items, 'condutor', 'desc')
    assert(
      String(ascResponse.items[0]?.condutor ?? '') !== String(descResponse.items[0]?.condutor ?? ''),
      'Ordenacao asc/desc de condutor retornou o mesmo primeiro registro.',
    )
    logStep('ordenacao asc/desc por condutor ok')

    const targetResponse = await requestJson('/api/condutor?page=1&pageSize=1&search=9241')
    assert(targetResponse.total === 1, 'Registro 9241 do condutor nao foi encontrado para o teste.')
    const originalItem = targetResponse.items[0]
    const updatedTipoVinculo = 'Cooperado'
    const updatedHistorico = 'SMOKE TEST API CONDUTOR'
    const updatedValidadeCurso = '2030-12-31'

    await requestJson('/api/condutor/9241', {
      method: 'PUT',
      body: JSON.stringify({
        codigo: originalItem.codigo,
        condutor: originalItem.condutor,
        cpfCondutor: originalItem.cpf_condutor,
        crmc: originalItem.crmc,
        validadeCrmc: originalItem.validade_crmc,
        validadeCurso: updatedValidadeCurso,
        tipoVinculo: updatedTipoVinculo,
        historico: updatedHistorico,
      }),
    })

    const updatedResponse = await requestJson('/api/condutor?page=1&pageSize=1&search=9241')
    const updatedItem = updatedResponse.items[0]
    assert(updatedItem.tipo_vinculo === updatedTipoVinculo, 'Alteracao do condutor nao persistiu tipo de vinculo.')
    assert(updatedItem.historico === updatedHistorico, 'Alteracao do condutor nao persistiu historico.')
    assert(updatedItem.validade_curso === updatedValidadeCurso, 'Alteracao do condutor nao persistiu validade do curso.')
    logStep('edicao do registro importado 9241 ok')

    const deleteResponse = await requestJson('/api/condutor/9241', { method: 'DELETE' })
    assert(deleteResponse.deletedCodigo === '9241', 'Exclusao do condutor nao retornou o codigo esperado.')
    const deletedLookup = await requestJson('/api/condutor?page=1&pageSize=1&search=9241')
    assert(deletedLookup.total === 0, 'Registro 9241 ainda foi encontrado apos exclusao.')
    logStep('exclusao do registro importado 9241 ok')

    const validImport = await requestJson('/api/condutor/import-xml', {
      method: 'POST',
      body: JSON.stringify({ fileName: 'Condutor.xml' }),
    })
    recordImport(suiteReport, 'valid-import', validImport)
    assert(validImport.total >= validImport.processed, 'Importacao valida de condutor retornou total inconsistente.')
    assert((validImport.processed + validImport.skipped) === validImport.total, 'Importacao valida de condutor nao fechou a contagem total.')
    assert((validImport.inserted + validImport.updated) === validImport.processed, 'Importacao valida de condutor retornou contagem inconsistente.')
    assert(validImport.skipped >= 0, 'Importacao valida de condutor retornou recusas invalidas.')
    logStep(`importacao valida do condutor: ${validImport.processed} processado(s), ${validImport.updated} alterado(s), ${validImport.inserted} incluido(s), ${validImport.skipped} recusado(s)`)

    const restoredResponse = await requestJson('/api/condutor?page=1&pageSize=1&search=9241')
    assert(restoredResponse.total === 1, 'Registro 9241 nao foi restaurado apos reimportacao valida.')
    const restoredItem = restoredResponse.items[0]
    assert(restoredItem.tipo_vinculo === originalItem.tipo_vinculo, 'Tipo de vinculo original nao foi restaurado apos reimportacao valida.')
    assert(restoredItem.historico === originalItem.historico, 'Historico original nao foi restaurado apos reimportacao valida.')
    assert(restoredItem.validade_curso === originalItem.validade_curso, 'Validade do curso original nao foi restaurada apos reimportacao valida.')
    logStep(`reimportacao valida e restauracao do registro 9241 ok (${validImport.skipped} recusa(s) existente(s) no XML fonte)`)

    const invalidImport = await requestJson('/api/condutor/import-xml', {
      method: 'POST',
      body: JSON.stringify({ fileName: 'Condutor-invalid.xml' }),
    })
    recordImport(suiteReport, 'invalid-import', invalidImport)
    assert(invalidImport.skipped === 2, 'Importacao invalida de condutor nao retornou 2 recusas.')
    assert(invalidImport.processed === 1, 'Importacao invalida de condutor nao retornou 1 registro processado.')
    logStep(`importacao invalida do condutor: ${invalidImport.processed} processado(s), ${invalidImport.skipped} recusado(s)`)

    const rejectionResponse = await requestJson('/api/condutor/import-rejections?page=1&pageSize=20&search=Condutor-invalid.xml')
    const rejectionReasons = rejectionResponse.items
      .filter((item) => item.arquivo_xml === 'Condutor-invalid.xml')
      .map((item) => item.motivo_recusa)

    assert(rejectionReasons.some((reason) => reason.includes('codigo invalido no XML')), 'Recusa por codigo invalido nao encontrada para condutor.')
    assert(rejectionReasons.some((reason) => reason.includes('CPF invalido no XML')), 'Recusa por CPF invalido nao encontrada para condutor.')
    logStep('importacao invalida e painel de recusas do condutor ok')

    finalizeSuite(suiteReport, 'passed')
  } catch (error) {
    finalizeSuite(suiteReport, 'failed')
    throw error
  }
}

const runCredenciadaSmoke = async () => {
  console.log('Smoke test da API Credenciada')
  const suiteReport = recordSuite('credenciada')

  try {
    const listResponse = await requestJson('/api/credenciada?page=1&pageSize=5')
    assert(Array.isArray(listResponse.items), 'Listagem de credenciada nao retornou items.')
    assert(listResponse.total > 0, 'Listagem de credenciada retornou total zerado.')
    logStep(`listagem inicial ok com ${listResponse.total} registro(s)`)

    const ascResponse = await requestJson('/api/credenciada?page=1&pageSize=5&sortBy=credenciado&sortDirection=asc')
    const descResponse = await requestJson('/api/credenciada?page=1&pageSize=5&sortBy=credenciado&sortDirection=desc')
    expectSortedBy(ascResponse.items, 'credenciado', 'asc')
    expectSortedBy(descResponse.items, 'credenciado', 'desc')
    assert(
      String(ascResponse.items[0]?.credenciado ?? '') !== String(descResponse.items[0]?.credenciado ?? ''),
      'Ordenacao asc/desc de credenciada retornou o mesmo primeiro registro.',
    )
    logStep('ordenacao asc/desc por credenciado ok')

    const targetResponse = await requestJson('/api/credenciada?page=1&pageSize=1&search=2277')
    assert(targetResponse.total === 1, 'Registro 2277 da credenciada nao foi encontrado para o teste.')
    const originalItem = targetResponse.items[0]
    const updatedMunicipio = 'OSASCO'
    const updatedRepresentante = 'ROSALI APARECIDA POLI GOMES TESTE API'
    const updatedStatus = 'EM TESTE API'

    await requestJson('/api/credenciada/2277', {
      method: 'PUT',
      body: JSON.stringify({
        codigo: originalItem.codigo,
        credenciado: originalItem.credenciado,
        cnpjCpf: originalItem.cnpj_cpf,
        logradouro: originalItem.logradouro,
        bairro: originalItem.bairro,
        cep: originalItem.cep,
        municipio: updatedMunicipio,
        email: originalItem.email,
        telefone1: originalItem.telefone_01,
        telefone2: originalItem.telefone_02,
        representante: updatedRepresentante,
        cpfRepresentante: originalItem.cpf_representante,
        rgRepresentante: originalItem.rg_representante,
        status: updatedStatus,
      }),
    })

    const updatedResponse = await requestJson('/api/credenciada?page=1&pageSize=1&search=2277')
    const updatedItem = updatedResponse.items[0]
    assert(updatedItem.municipio === updatedMunicipio, 'Alteracao da credenciada nao persistiu municipio.')
    assert(updatedItem.representante === updatedRepresentante, 'Alteracao da credenciada nao persistiu representante.')
    assert(updatedItem.status === updatedStatus, 'Alteracao da credenciada nao persistiu status.')
    logStep('edicao do registro importado 2277 ok')

    const deleteResponse = await requestJson('/api/credenciada/2277', { method: 'DELETE' })
    assert(deleteResponse.deletedCodigo === '2277', 'Exclusao da credenciada nao retornou o codigo esperado.')
    const deletedLookup = await requestJson('/api/credenciada?page=1&pageSize=1&search=2277')
    assert(deletedLookup.total === 0, 'Registro 2277 ainda foi encontrado apos exclusao.')
    logStep('exclusao do registro importado 2277 ok')

    const validImport = await requestJson('/api/credenciada/import-xml', {
      method: 'POST',
      body: JSON.stringify({ fileName: 'Credenciados.xml' }),
    })
    recordImport(suiteReport, 'valid-import', validImport)
    assert(validImport.skipped === 0, 'Importacao valida de credenciada retornou recusas inesperadas.')
    assert(validImport.processed === validImport.total, 'Importacao valida de credenciada nao processou todos os registros do XML.')
    assert((validImport.inserted + validImport.updated) === validImport.processed, 'Importacao valida de credenciada retornou contagem inconsistente.')
    logStep(`importacao valida da credenciada: ${validImport.processed} processado(s), ${validImport.updated} alterado(s), ${validImport.inserted} incluido(s), ${validImport.skipped} recusado(s)`)

    const restoredResponse = await requestJson('/api/credenciada?page=1&pageSize=1&search=2277')
    assert(restoredResponse.total === 1, 'Registro 2277 nao foi restaurado apos reimportacao valida.')
    const restoredItem = restoredResponse.items[0]
    assert(restoredItem.municipio === originalItem.municipio, 'Municipio original nao foi restaurado apos reimportacao valida.')
    assert(restoredItem.representante === originalItem.representante, 'Representante original nao foi restaurado apos reimportacao valida.')
    assert(restoredItem.status === originalItem.status, 'Status original nao foi restaurado apos reimportacao valida.')
    logStep('reimportacao valida e restauracao do registro 2277 ok')

    const invalidImport = await requestJson('/api/credenciada/import-xml', {
      method: 'POST',
      body: JSON.stringify({ fileName: 'Credenciados-invalid.xml' }),
    })
    recordImport(suiteReport, 'invalid-import', invalidImport)
    assert(invalidImport.skipped === 2, 'Importacao invalida de credenciada nao retornou 2 recusas.')
    assert(invalidImport.processed === 1, 'Importacao invalida de credenciada nao retornou 1 registro processado.')
    logStep(`importacao invalida da credenciada: ${invalidImport.processed} processado(s), ${invalidImport.skipped} recusado(s)`)

    const rejectionResponse = await requestJson('/api/credenciada/import-rejections?page=1&pageSize=20&search=Credenciados-invalid.xml')
    const rejectionReasons = rejectionResponse.items
      .filter((item) => item.arquivo_xml === 'Credenciados-invalid.xml')
      .map((item) => item.motivo_recusa)

    assert(rejectionReasons.some((reason) => reason.includes('codigo invalido no XML')), 'Recusa por codigo invalido nao encontrada para credenciada.')
    assert(rejectionReasons.some((reason) => reason.includes('email invalido no XML')), 'Recusa por email invalido nao encontrada para credenciada.')
    logStep('importacao invalida e painel de recusas da credenciada ok')

    finalizeSuite(suiteReport, 'passed')
  } catch (error) {
    finalizeSuite(suiteReport, 'failed')
    throw error
  }
}

const runVeiculoSmoke = async () => {
  console.log('Smoke test da API Veiculo')
  const suiteReport = recordSuite('veiculo')

  try {
    let listResponse = await requestJson('/api/veiculo?page=1&pageSize=5')

    if (listResponse.total === 0) {
      const bootstrapImport = await requestJson('/api/veiculo/import-xml', {
        method: 'POST',
        body: JSON.stringify({ fileName: 'Veiculo.xml' }),
      })
      recordImport(suiteReport, 'bootstrap-import', bootstrapImport)
      listResponse = await requestJson('/api/veiculo?page=1&pageSize=5')
    }

    assert(Array.isArray(listResponse.items), 'Listagem de veiculo nao retornou items.')
    assert(listResponse.total > 0, 'Listagem de veiculo retornou total zerado.')
    logStep(`listagem inicial ok com ${listResponse.total} registro(s)`)

    const ascResponse = await requestJson('/api/veiculo?page=1&pageSize=5&sortBy=placas&sortDirection=asc')
    const descResponse = await requestJson('/api/veiculo?page=1&pageSize=5&sortBy=placas&sortDirection=desc')
    expectSortedBy(ascResponse.items, 'placas', 'asc')
    expectSortedBy(descResponse.items, 'placas', 'desc')
    assert(
      String(ascResponse.items[0]?.placas ?? '') !== String(descResponse.items[0]?.placas ?? ''),
      'Ordenacao asc/desc de veiculo retornou o mesmo primeiro registro.',
    )
    logStep('ordenacao asc/desc por placas ok')

    const targetResponse = await requestJson('/api/veiculo?page=1&pageSize=1&search=4143')
    assert(targetResponse.total === 1, 'Registro 4143 do veiculo nao foi encontrado para o teste.')
    const originalItem = targetResponse.items[0]
    const updatedTipoDeBancada = 'Creche'
    const updatedOsEspecial = 'Sim'
    const updatedMarcaModelo = 'I/M.BENZ311 RIBEIRO MO18 TESTE API'

    await requestJson('/api/veiculo/4143', {
      method: 'PUT',
      body: JSON.stringify({
        codigo: originalItem.codigo,
        crm: originalItem.crm,
        placas: originalItem.placas,
        ano: originalItem.ano,
        capDetran: originalItem.cap_detran,
        capTeg: originalItem.cap_teg,
        capTegCreche: originalItem.cap_teg_creche,
        capAcessivel: originalItem.cap_acessivel,
        valCrm: originalItem.val_crm,
        seguradora: originalItem.seguradora,
        seguroInicio: originalItem.seguro_inicio,
        seguroTermino: originalItem.seguro_termino,
        tipoDeBancada: updatedTipoDeBancada,
        tipoDeVeiculo: originalItem.tipo_de_veiculo,
        marcaModelo: updatedMarcaModelo,
        titular: originalItem.titular,
        cnpjCpf: originalItem.cnpj_cpf,
        valorVeiculo: originalItem.valor_veiculo,
        osEspecial: updatedOsEspecial,
      }),
    })

    const updatedResponse = await requestJson('/api/veiculo?page=1&pageSize=1&search=4143')
    const updatedItem = updatedResponse.items[0]
    assert(updatedItem.tipo_de_bancada === updatedTipoDeBancada, 'Alteracao do veiculo nao persistiu tipo de bancada.')
    assert(updatedItem.os_especial === updatedOsEspecial, 'Alteracao do veiculo nao persistiu OS especial.')
    assert(updatedItem.marca_modelo === updatedMarcaModelo, 'Alteracao do veiculo nao persistiu marca/modelo.')
    logStep('edicao do registro importado 4143 ok')

    const deleteResponse = await requestJson('/api/veiculo/4143', { method: 'DELETE' })
    assert(deleteResponse.deletedCodigo === '4143', 'Exclusao do veiculo nao retornou o codigo esperado.')
    const deletedLookup = await requestJson('/api/veiculo?page=1&pageSize=1&search=4143')
    assert(deletedLookup.total === 0, 'Registro 4143 ainda foi encontrado apos exclusao.')
    logStep('exclusao do registro importado 4143 ok')

    const validImport = await requestJson('/api/veiculo/import-xml', {
      method: 'POST',
      body: JSON.stringify({ fileName: 'Veiculo.xml' }),
    })
    recordImport(suiteReport, 'valid-import', validImport)
    assert(validImport.total >= validImport.processed, 'Importacao valida de veiculo retornou total inconsistente.')
    assert((validImport.processed + validImport.skipped) === validImport.total, 'Importacao valida de veiculo nao fechou a contagem total.')
    assert((validImport.inserted + validImport.updated) === validImport.processed, 'Importacao valida de veiculo retornou contagem inconsistente.')
    logStep(`importacao valida do veiculo: ${validImport.processed} processado(s), ${validImport.updated} alterado(s), ${validImport.inserted} incluido(s), ${validImport.skipped} recusado(s)`) 

    const restoredResponse = await requestJson('/api/veiculo?page=1&pageSize=1&search=4143')
    assert(restoredResponse.total === 1, 'Registro 4143 nao foi restaurado apos reimportacao valida.')
    const restoredItem = restoredResponse.items[0]
    assert(restoredItem.tipo_de_bancada === originalItem.tipo_de_bancada, 'Tipo de bancada original nao foi restaurado apos reimportacao valida.')
    assert(restoredItem.os_especial === originalItem.os_especial, 'OS especial original nao foi restaurado apos reimportacao valida.')
    assert(restoredItem.marca_modelo === originalItem.marca_modelo, 'Marca/modelo original nao foi restaurado apos reimportacao valida.')
    logStep('reimportacao valida e restauracao do registro 4143 ok')

    const invalidImport = await requestJson('/api/veiculo/import-xml', {
      method: 'POST',
      body: JSON.stringify({ fileName: 'Veiculo-invalid.xml' }),
    })
    recordImport(suiteReport, 'invalid-import', invalidImport)
    assert(invalidImport.skipped === 2, 'Importacao invalida de veiculo nao retornou 2 recusas.')
    assert(invalidImport.processed === 1, 'Importacao invalida de veiculo nao retornou 1 registro processado.')
    logStep(`importacao invalida do veiculo: ${invalidImport.processed} processado(s), ${invalidImport.skipped} recusado(s)`) 

    const rejectionResponse = await requestJson('/api/veiculo/import-rejections?page=1&pageSize=20&search=Veiculo-invalid.xml')
    const rejectionReasons = rejectionResponse.items
      .filter((item) => item.arquivo_xml === 'Veiculo-invalid.xml')
      .map((item) => item.motivo_recusa)

    assert(rejectionReasons.some((reason) => reason.includes('codigo invalido no XML')), 'Recusa por codigo invalido nao encontrada para veiculo.')
    assert(rejectionReasons.some((reason) => reason.includes('tipo de bancada invalido no XML') || reason.includes('tipo de veiculo invalido no XML')), 'Recusa por tipo invalido nao encontrada para veiculo.')
    logStep('importacao invalida e painel de recusas do veiculo ok')

    finalizeSuite(suiteReport, 'passed')
  } catch (error) {
    finalizeSuite(suiteReport, 'failed')
    throw error
  }
}

const runMarcaModeloSmoke = async () => {
  console.log('Smoke test da API Marca/Modelo')
  const suiteReport = recordSuite('marca-modelo')

  try {
    const baselineImport = await requestJson('/api/marca-modelo/import-xml', {
      method: 'POST',
      body: JSON.stringify({ fileName: 'marca-modelo.xml' }),
    })
    recordImport(suiteReport, 'baseline-import', baselineImport)

    let listResponse = await requestJson('/api/marca-modelo?page=1&pageSize=5&sortBy=codigo&sortDirection=asc')

    assert(Array.isArray(listResponse.items), 'Listagem de marca/modelo nao retornou items.')
    assert(listResponse.total > 0, 'Listagem de marca/modelo retornou total zerado.')
    logStep(`listagem inicial ok com ${listResponse.total} registro(s)`)

    const ascResponse = await requestJson('/api/marca-modelo?page=1&pageSize=5&sortBy=codigo&sortDirection=asc')
    const descResponse = await requestJson('/api/marca-modelo?page=1&pageSize=5&sortBy=codigo&sortDirection=desc')
    expectSortedByNumericString(ascResponse.items, 'codigo', 'asc')
    expectSortedByNumericString(descResponse.items, 'codigo', 'desc')
    assert(
      String(ascResponse.items[0]?.codigo ?? '') !== String(descResponse.items[0]?.codigo ?? ''),
      'Ordenacao asc/desc de marca/modelo retornou o mesmo primeiro registro.',
    )
    logStep('ordenacao numerica asc/desc por codigo ok')

    const targetResponse = await requestJson('/api/marca-modelo?page=1&pageSize=1&search=1')
    assert(targetResponse.total >= 1, 'Registro 1 de marca/modelo nao foi encontrado para o teste.')
    const originalItem = targetResponse.items.find((item) => item.codigo === '1') ?? targetResponse.items[0]
    assert(originalItem.codigo === '1', 'O smoke de marca/modelo exige o registro codigo 1.')
    const updatedDescricao = 'AGRALE/8.5NEOBUS THUNDER TESTE API'

    await requestJson('/api/marca-modelo/1', {
      method: 'PUT',
      body: JSON.stringify({
        codigo: originalItem.codigo,
        descricao: updatedDescricao,
      }),
    })

    const updatedResponse = await requestJson('/api/marca-modelo?page=1&pageSize=1&search=THUNDER TESTE API')
    assert(updatedResponse.total === 1, 'Alteracao de marca/modelo nao foi localizada apos edicao.')
    assert(updatedResponse.items[0]?.descricao === updatedDescricao, 'Alteracao de marca/modelo nao persistiu descricao.')
    logStep('edicao do registro importado 1 ok')

    const deleteResponse = await requestJson('/api/marca-modelo/1', { method: 'DELETE' })
    assert(deleteResponse.deletedCodigo === '1', 'Exclusao de marca/modelo nao retornou o codigo esperado.')
    const deletedLookup = await requestJson('/api/marca-modelo?page=1&pageSize=5&search=AGRALE/8.5NEOBUS THUNDER TESTE API')
    assert(deletedLookup.total === 0, 'Registro 1 ainda foi encontrado apos exclusao.')
    logStep('exclusao do registro importado 1 ok')

    const validImport = await requestJson('/api/marca-modelo/import-xml', {
      method: 'POST',
      body: JSON.stringify({ fileName: 'marca-modelo.xml' }),
    })
    recordImport(suiteReport, 'valid-import', validImport)
    assert(validImport.total >= validImport.processed, 'Importacao valida de marca/modelo retornou total inconsistente.')
    assert(validImport.processed > 0, 'Importacao valida de marca/modelo nao processou registros.')
    assert((validImport.inserted + validImport.updated) === validImport.processed, 'Importacao valida de marca/modelo retornou contagem inconsistente.')
    logStep(`importacao valida de marca/modelo: ${validImport.processed} processado(s), ${validImport.updated} alterado(s), ${validImport.inserted} incluido(s)`) 

    const restoredResponse = await requestJson('/api/marca-modelo?page=1&pageSize=5&search=AGRALE/8.5NEOBUS THUNDER')
    const restoredItem = restoredResponse.items.find((item) => item.codigo === '1')
    assert(Boolean(restoredItem), 'Registro 1 nao foi restaurado apos reimportacao valida.')
    assert(restoredItem?.descricao === originalItem.descricao, 'Descricao original nao foi restaurada apos reimportacao valida.')
    logStep('reimportacao valida e restauracao do registro 1 ok')

    finalizeSuite(suiteReport, 'passed')
  } catch (error) {
    finalizeSuite(suiteReport, 'failed')
    throw error
  }
}

try {
  if (suite === 'all' || suite === 'condutor') {
    await runCondutorSmoke()
  }

  if (suite === 'all') {
    console.log('')
  }

  if (suite === 'all' || suite === 'credenciada') {
    await runCredenciadaSmoke()
  }

  if (suite === 'all') {
    console.log('')
  }

  if (suite === 'all' || suite === 'veiculo') {
    await runVeiculoSmoke()
  }

  if (suite === 'all') {
    console.log('')
  }

  if (suite === 'all' || suite === 'marca-modelo') {
    await runMarcaModeloSmoke()
  }

  report.status = 'passed'
  console.log(`Smoke test concluido com sucesso (${suite}).`)
} catch (error) {
  report.status = 'failed'
  report.failureMessage = error instanceof Error ? error.message : String(error)
  console.error('Smoke test falhou.')
  console.error(error instanceof Error ? error.message : error)
  process.exitCode = 1
} finally {
  report.finishedAt = new Date().toISOString()
  await writeReportIfNeeded()
}
