export type TitularItem = {
  codigo: string
  cnpj_cpf: string
  titular: string
}

type TitularListResponse = {
  items: TitularItem[]
  total: number
  page: number
  pageSize: number
  totalPages: number
  sortBy: 'codigo' | 'cnpj_cpf' | 'titular'
  sortDirection: 'asc' | 'desc'
}

type TitularCreateResponse = {
  item: TitularItem
}

type TitularDeleteResponse = {
  deletedCodigo: string
}

export type TitularListParams = {
  search?: string
  page?: number
  pageSize?: number
  sortBy?: 'codigo' | 'cnpj_cpf' | 'titular'
  sortDirection?: 'asc' | 'desc'
}

export type TitularListResult = {
  items: TitularItem[]
  total: number
  page: number
  pageSize: number
  totalPages: number
  sortBy: 'codigo' | 'cnpj_cpf' | 'titular'
  sortDirection: 'asc' | 'desc'
}

const getTitularUrl = () => {
  return import.meta.env.VITE_TITULAR_URL?.trim() || '/api/titular'
}

const getTitularItemUrl = (codigo: string) => {
  return `${getTitularUrl()}/${encodeURIComponent(codigo)}`
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
    : 'Falha ao processar dados da tabela titular do CRM.'
}

export async function listTitularItemsPaginated(params: TitularListParams): Promise<TitularListResult> {
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

  const requestUrl = queryParams.size ? `${getTitularUrl()}?${queryParams.toString()}` : getTitularUrl()
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

  const result = payload as TitularListResponse
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

export async function createTitularItem(item: TitularItem): Promise<TitularItem> {
  const response = await fetch(getTitularUrl(), {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Accept: 'application/json',
    },
    body: JSON.stringify({
      codigo: item.codigo,
      cnpjCpf: item.cnpj_cpf,
      titular: item.titular,
    }),
  })

  const responseText = await response.text()
  const payload = parseJsonSafely(responseText)

  if (!response.ok) {
    throw new Error(getErrorMessage(payload))
  }

  return (payload as TitularCreateResponse).item
}

export async function updateTitularItem(originalCodigo: string, item: TitularItem): Promise<TitularItem> {
  const response = await fetch(getTitularItemUrl(originalCodigo), {
    method: 'PUT',
    headers: {
      'Content-Type': 'application/json',
      Accept: 'application/json',
    },
    body: JSON.stringify({
      codigo: item.codigo,
      cnpjCpf: item.cnpj_cpf,
      titular: item.titular,
    }),
  })

  const responseText = await response.text()
  const payload = parseJsonSafely(responseText)

  if (!response.ok) {
    throw new Error(getErrorMessage(payload))
  }

  return (payload as TitularCreateResponse).item
}

export async function deleteTitularItem(codigo: string): Promise<string> {
  const response = await fetch(getTitularItemUrl(codigo), {
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

  return (payload as TitularDeleteResponse).deletedCodigo
}