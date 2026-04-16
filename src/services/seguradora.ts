export type SeguradoraItem = {
  codigo: string
  controle: string
  descricao: string
}

export type SeguradoraSaveItem = {
  codigo?: string
  controle: string
  descricao: string
}

type SeguradoraListResponse = {
  items: SeguradoraItem[]
  total: number
  page: number
  pageSize: number
  totalPages: number
  sortBy: 'codigo' | 'controle' | 'descricao'
  sortDirection: 'asc' | 'desc'
}

type SeguradoraCreateResponse = {
  item: SeguradoraItem
}

type SeguradoraDeleteResponse = {
  deletedCodigo: string
}

export type SeguradoraListParams = {
  search?: string
  page?: number
  pageSize?: number
  sortBy?: 'codigo' | 'controle' | 'descricao'
  sortDirection?: 'asc' | 'desc'
}

export type SeguradoraListResult = {
  items: SeguradoraItem[]
  total: number
  page: number
  pageSize: number
  totalPages: number
  sortBy: 'codigo' | 'controle' | 'descricao'
  sortDirection: 'asc' | 'desc'
}

const getSeguradoraUrl = () => {
  return import.meta.env.VITE_SEGURADORA_URL?.trim() || '/api/seguradora'
}

const getSeguradoraItemUrl = (codigo: string) => {
  return `${getSeguradoraUrl()}/${encodeURIComponent(codigo)}`
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
    : 'Falha ao processar dados de seguradoras.'
}

export async function listSeguradoraItemsPaginated(params: SeguradoraListParams): Promise<SeguradoraListResult> {
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

  const requestUrl = queryParams.size ? `${getSeguradoraUrl()}?${queryParams.toString()}` : getSeguradoraUrl()
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

  const result = payload as SeguradoraListResponse
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

export async function createSeguradoraItem(item: SeguradoraSaveItem): Promise<SeguradoraItem> {
  const response = await fetch(getSeguradoraUrl(), {
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

  return (payload as SeguradoraCreateResponse).item
}

export async function updateSeguradoraItem(originalCodigo: string, item: SeguradoraSaveItem): Promise<SeguradoraItem> {
  const response = await fetch(getSeguradoraItemUrl(originalCodigo), {
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

  return (payload as SeguradoraCreateResponse).item
}

export async function deleteSeguradoraItem(codigo: string): Promise<string> {
  const response = await fetch(getSeguradoraItemUrl(codigo), {
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

  return (payload as SeguradoraDeleteResponse).deletedCodigo
}