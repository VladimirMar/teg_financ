export type MarcaModeloItem = {
  codigo: string
  descricao: string
}

export type MarcaModeloSaveItem = {
  codigo?: string
  descricao: string
}

type MarcaModeloListResponse = {
  items: MarcaModeloItem[]
  total: number
  page: number
  pageSize: number
  totalPages: number
  sortBy: 'codigo' | 'descricao'
  sortDirection: 'asc' | 'desc'
}

type MarcaModeloCreateResponse = {
  item: MarcaModeloItem
}

type MarcaModeloDeleteResponse = {
  deletedCodigo: string
}

export type MarcaModeloListParams = {
  search?: string
  page?: number
  pageSize?: number
  sortBy?: 'codigo' | 'descricao'
  sortDirection?: 'asc' | 'desc'
}

export type MarcaModeloListResult = {
  items: MarcaModeloItem[]
  total: number
  page: number
  pageSize: number
  totalPages: number
  sortBy: 'codigo' | 'descricao'
  sortDirection: 'asc' | 'desc'
}

const getMarcaModeloUrl = () => {
  return import.meta.env.VITE_MARCA_MODELO_URL?.trim() || '/api/marca-modelo'
}

const getMarcaModeloItemUrl = (codigo: string) => {
  return `${getMarcaModeloUrl()}/${encodeURIComponent(codigo)}`
}

const parseJsonSafely = (value: string) => {
  if (!value) {
    return {}
  }

  try {
    return JSON.parse(value) as Record<string, unknown>
  } catch {
    return { message: value }
  }
}

const getErrorMessage = (payload: Record<string, unknown>) => {
  return typeof payload.message === 'string' && payload.message.trim()
    ? payload.message
    : 'Falha ao processar dados da tabela marca/modelo.'
}

export async function listMarcaModeloItemsPaginated(params: MarcaModeloListParams): Promise<MarcaModeloListResult> {
  const queryParams = new URLSearchParams()

  if (params.search?.trim()) {
    queryParams.set('search', params.search.trim())
  }

  if (params.page) {
    queryParams.set('page', String(params.page))
  }

  if (params.pageSize) {
    queryParams.set('pageSize', String(params.pageSize))
  }

  if (params.sortBy) {
    queryParams.set('sortBy', params.sortBy)
  }

  if (params.sortDirection) {
    queryParams.set('sortDirection', params.sortDirection)
  }

  const requestUrl = queryParams.size ? `${getMarcaModeloUrl()}?${queryParams.toString()}` : getMarcaModeloUrl()
  const response = await fetch(requestUrl, {
    method: 'GET',
    headers: {
      Accept: 'application/json',
    },
  })

  const responseText = await response.text()
  const payload = parseJsonSafely(responseText)

  if (!response.ok) {
    throw new Error(getErrorMessage(payload))
  }

  const result = payload as MarcaModeloListResponse
  return {
    items: result.items ?? [],
    total: result.total ?? 0,
    page: result.page ?? 1,
    pageSize: result.pageSize ?? params.pageSize ?? 5,
    totalPages: result.totalPages ?? 1,
    sortBy: result.sortBy ?? params.sortBy ?? 'codigo',
    sortDirection: result.sortDirection ?? params.sortDirection ?? 'asc',
  }
}

export async function createMarcaModeloItem(item: MarcaModeloSaveItem): Promise<MarcaModeloItem> {
  const response = await fetch(getMarcaModeloUrl(), {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Accept: 'application/json',
    },
    body: JSON.stringify(item),
  })

  const responseText = await response.text()
  const payload = parseJsonSafely(responseText)

  if (!response.ok) {
    throw new Error(getErrorMessage(payload))
  }

  return (payload as MarcaModeloCreateResponse).item
}

export async function updateMarcaModeloItem(originalCodigo: string, item: MarcaModeloSaveItem): Promise<MarcaModeloItem> {
  const response = await fetch(getMarcaModeloItemUrl(originalCodigo), {
    method: 'PUT',
    headers: {
      'Content-Type': 'application/json',
      Accept: 'application/json',
    },
    body: JSON.stringify(item),
  })

  const responseText = await response.text()
  const payload = parseJsonSafely(responseText)

  if (!response.ok) {
    throw new Error(getErrorMessage(payload))
  }

  return (payload as MarcaModeloCreateResponse).item
}

export async function deleteMarcaModeloItem(codigo: string): Promise<string> {
  const response = await fetch(getMarcaModeloItemUrl(codigo), {
    method: 'DELETE',
    headers: {
      Accept: 'application/json',
    },
  })

  const responseText = await response.text()
  const payload = parseJsonSafely(responseText)

  if (!response.ok) {
    throw new Error(getErrorMessage(payload))
  }

  return (payload as MarcaModeloDeleteResponse).deletedCodigo
}