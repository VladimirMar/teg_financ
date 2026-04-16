export type DreItem = {
  codigo: string
  sigla: string
  descricao: string
}

export type DreSaveItem = {
  codigo?: string
  sigla: string
  descricao: string
}

type DreListResponse = {
  items: DreItem[]
  total: number
  page: number
  pageSize: number
  totalPages: number
  sortBy: 'codigo' | 'descricao'
  sortDirection: 'asc' | 'desc'
}

type DreCreateResponse = {
  item: DreItem
}

type DreDeleteResponse = {
  deletedCodigo: string
}

export type DreListParams = {
  search?: string
  page?: number
  pageSize?: number
  sortBy?: 'codigo' | 'descricao'
  sortDirection?: 'asc' | 'desc'
}

export type DreListResult = {
  items: DreItem[]
  total: number
  page: number
  pageSize: number
  totalPages: number
  sortBy: 'codigo' | 'descricao'
  sortDirection: 'asc' | 'desc'
}

const getDreUrl = () => {
  return import.meta.env.VITE_DRE_URL?.trim() || '/api/dre'
}

const getDreItemUrl = (codigo: string) => {
  return `${getDreUrl()}/${encodeURIComponent(codigo)}`
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
    : 'Falha ao processar dados da DRE.'
}

export async function listDreItems(): Promise<DreItem[]> {
  const response = await fetch(getDreUrl(), {
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

  return (payload as DreListResponse).items ?? []
}

export async function listDreItemsPaginated(params: DreListParams): Promise<DreListResult> {
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

  const requestUrl = queryParams.size ? `${getDreUrl()}?${queryParams.toString()}` : getDreUrl()
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

  const result = payload as DreListResponse
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

export async function createDreItem(item: DreSaveItem): Promise<DreItem> {
  const response = await fetch(getDreUrl(), {
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

  return (payload as DreCreateResponse).item
}

export async function updateDreItem(originalCodigo: string, item: DreSaveItem): Promise<DreItem> {
  const response = await fetch(getDreItemUrl(originalCodigo), {
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

  return (payload as DreCreateResponse).item
}

export async function deleteDreItem(codigo: string): Promise<string> {
  const response = await fetch(getDreItemUrl(codigo), {
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

  return (payload as DreDeleteResponse).deletedCodigo
}