export type ModalidadeItem = {
  codigo: string
  descricao: string
}

export type ModalidadeSaveItem = {
  codigo?: string
  descricao: string
}

type ModalidadeListResponse = {
  items: ModalidadeItem[]
  total: number
  page: number
  pageSize: number
  totalPages: number
  sortBy: 'codigo' | 'descricao'
  sortDirection: 'asc' | 'desc'
}

type ModalidadeCreateResponse = {
  item: ModalidadeItem
}

type ModalidadeDeleteResponse = {
  deletedCodigo: string
}

export type ModalidadeListParams = {
  search?: string
  page?: number
  pageSize?: number
  sortBy?: 'codigo' | 'descricao'
  sortDirection?: 'asc' | 'desc'
}

export type ModalidadeListResult = {
  items: ModalidadeItem[]
  total: number
  page: number
  pageSize: number
  totalPages: number
  sortBy: 'codigo' | 'descricao'
  sortDirection: 'asc' | 'desc'
}

const getModalidadeUrl = () => {
  return import.meta.env.VITE_MODALIDADE_URL?.trim() || '/api/modalidade'
}

const getModalidadeItemUrl = (codigo: string) => {
  return `${getModalidadeUrl()}/${encodeURIComponent(codigo)}`
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
    : 'Falha ao processar dados da modalidade.'
}

export async function listModalidadeItemsPaginated(params: ModalidadeListParams): Promise<ModalidadeListResult> {
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

  const requestUrl = queryParams.size ? `${getModalidadeUrl()}?${queryParams.toString()}` : getModalidadeUrl()
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

  const result = payload as ModalidadeListResponse
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

export async function createModalidadeItem(item: ModalidadeSaveItem): Promise<ModalidadeItem> {
  const response = await fetch(getModalidadeUrl(), {
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

  return (payload as ModalidadeCreateResponse).item
}

export async function updateModalidadeItem(originalCodigo: string, item: ModalidadeSaveItem): Promise<ModalidadeItem> {
  const response = await fetch(getModalidadeItemUrl(originalCodigo), {
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

  return (payload as ModalidadeCreateResponse).item
}

export async function deleteModalidadeItem(codigo: string): Promise<string> {
  const response = await fetch(getModalidadeItemUrl(codigo), {
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

  return (payload as ModalidadeDeleteResponse).deletedCodigo
}