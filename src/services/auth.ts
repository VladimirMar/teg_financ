type LoginCredentials = {
  email: string
  password: string
}

type AuthPayload = Record<string, unknown>

export type AuthResult = {
  token: string | null
  user: unknown
  payload: AuthPayload
}

const getAuthUrl = () => {
  const authUrl = import.meta.env.VITE_AUTH_URL?.trim()

  if (!authUrl) {
    throw new Error('Defina VITE_AUTH_URL para autenticar no backend.')
  }

  return authUrl
}

const parseJsonSafely = (value: string) => {
  if (!value) {
    return {}
  }

  try {
    return JSON.parse(value) as AuthPayload
  } catch {
    return { message: value }
  }
}

const getString = (value: unknown) => {
  return typeof value === 'string' && value.trim() ? value : null
}

const extractToken = (payload: AuthPayload) => {
  const candidates = [
    payload.token,
    payload.accessToken,
    payload.access_token,
    payload.jwt,
    payload.authToken,
  ]

  for (const candidate of candidates) {
    const token = getString(candidate)

    if (token) {
      return token
    }
  }

  const data = payload.data
  if (data && typeof data === 'object') {
    const nestedToken = getString((data as AuthPayload).token)
    if (nestedToken) {
      return nestedToken
    }
  }

  return null
}

const extractUser = (payload: AuthPayload) => {
  if (payload.user !== undefined) {
    return payload.user
  }

  if (payload.data && typeof payload.data === 'object') {
    return (payload.data as AuthPayload).user ?? null
  }

  return null
}

const extractErrorMessage = (payload: AuthPayload) => {
  const candidates = [
    payload.message,
    payload.error,
    payload.detail,
    payload.title,
  ]

  for (const candidate of candidates) {
    const message = getString(candidate)

    if (message) {
      return message
    }
  }

  if (payload.errors && typeof payload.errors === 'object') {
    const firstError = Object.values(payload.errors).find((value) => {
      return typeof value === 'string' || Array.isArray(value)
    })

    if (typeof firstError === 'string' && firstError.trim()) {
      return firstError
    }

    if (Array.isArray(firstError) && typeof firstError[0] === 'string') {
      return firstError[0]
    }
  }

  return null
}

export async function authenticate(credentials: LoginCredentials): Promise<AuthResult> {
  const response = await fetch(getAuthUrl(), {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Accept: 'application/json',
    },
    body: JSON.stringify(credentials),
  })

  const responseText = await response.text()
  const payload = parseJsonSafely(responseText)

  if (!response.ok) {
    throw new Error(extractErrorMessage(payload) ?? 'Nao foi possivel autenticar.')
  }

  return {
    token: extractToken(payload),
    user: extractUser(payload),
    payload,
  }
}