import { useEffect, useState } from 'react'
import type { FormEvent } from 'react'
import './App.css'
import { authenticate } from './services/auth'

type StatusTone = 'idle' | 'error' | 'success'

type StoredSession = {
  email: string
  token: string | null
  user: unknown
  payload: Record<string, unknown>
  authenticatedAt: string
}

const SESSION_STORAGE_KEY = 'tegfinanc.auth'

function SchoolBusArt() {
  return (
    <svg
      className="bus-illustration"
      viewBox="0 0 640 360"
      role="img"
      aria-label="Ilustracao de onibus escolar"
    >
      <defs>
        <linearGradient id="busBody" x1="0%" y1="0%" x2="100%" y2="100%">
          <stop offset="0%" stopColor="#f4be2a" />
          <stop offset="100%" stopColor="#df8e1d" />
        </linearGradient>
        <linearGradient id="skyGlow" x1="0%" y1="0%" x2="0%" y2="100%">
          <stop offset="0%" stopColor="#f8fbff" stopOpacity="0.9" />
          <stop offset="100%" stopColor="#cfe7f7" stopOpacity="0.2" />
        </linearGradient>
      </defs>

      <rect x="24" y="24" width="592" height="312" rx="36" fill="url(#skyGlow)" />
      <circle cx="132" cy="104" r="34" fill="#fff3c4" />
      <path d="M70 276h500" stroke="#86af78" strokeWidth="10" strokeLinecap="round" />
      <path d="M78 294h482" stroke="#648d5d" strokeWidth="18" strokeLinecap="round" />

      <g transform="translate(88 98)">
        <rect x="20" y="36" width="398" height="118" rx="26" fill="url(#busBody)" />
        <path
          d="M74 10h222c30 0 58 17 72 44l17 32H33l16-30C55 28 83 10 114 10Z"
          fill="url(#busBody)"
        />
        <rect x="88" y="35" width="256" height="18" rx="9" fill="#45413a" opacity="0.18" />
        <rect x="58" y="64" width="54" height="44" rx="8" fill="#d8eefc" />
        <rect x="122" y="64" width="54" height="44" rx="8" fill="#d8eefc" />
        <rect x="186" y="64" width="54" height="44" rx="8" fill="#d8eefc" />
        <rect x="250" y="64" width="54" height="44" rx="8" fill="#d8eefc" />
        <rect x="314" y="64" width="54" height="44" rx="8" fill="#d8eefc" />
        <rect x="22" y="64" width="26" height="78" rx="8" fill="#45413a" opacity="0.9" />
        <rect x="383" y="66" width="26" height="66" rx="8" fill="#7d4d1f" opacity="0.72" />
        <path d="M42 122h334" stroke="#4d3a16" strokeWidth="6" strokeLinecap="round" />
        <circle cx="104" cy="164" r="32" fill="#2f3b45" />
        <circle cx="104" cy="164" r="15" fill="#b9c6cf" />
        <circle cx="344" cy="164" r="32" fill="#2f3b45" />
        <circle cx="344" cy="164" r="15" fill="#b9c6cf" />
        <circle cx="50" cy="120" r="7" fill="#f97316" />
        <circle cx="391" cy="120" r="7" fill="#ef4444" />
      </g>
    </svg>
  )
}

function getStoredSession() {
  const storedValue = sessionStorage.getItem(SESSION_STORAGE_KEY)

  if (!storedValue) {
    return null
  }

  try {
    return JSON.parse(storedValue) as StoredSession
  } catch {
    sessionStorage.removeItem(SESSION_STORAGE_KEY)
    return null
  }
}

function App() {
  const [email, setEmail] = useState('')
  const [password, setPassword] = useState('')
  const [emailError, setEmailError] = useState('')
  const [passwordError, setPasswordError] = useState('')
  const [statusMessage, setStatusMessage] = useState('')
  const [statusTone, setStatusTone] = useState<StatusTone>('idle')
  const [isSubmitting, setIsSubmitting] = useState(false)
  const [session, setSession] = useState<StoredSession | null>(null)

  useEffect(() => {
    setSession(getStoredSession())
  }, [])

  const validateEmail = (value: string) => {
    return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(value)
  }

  const handleSubmit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault()

    const trimmedEmail = email.trim()
    const trimmedPassword = password.trim()

    let hasError = false

    if (!trimmedEmail) {
      setEmailError('Informe o email.')
      hasError = true
    } else if (!validateEmail(trimmedEmail)) {
      setEmailError('Digite um email valido.')
      hasError = true
    } else {
      setEmailError('')
    }

    if (!trimmedPassword) {
      setPasswordError('Informe a senha.')
      hasError = true
    } else {
      setPasswordError('')
    }

    if (hasError) {
      setStatusTone('error')
      setStatusMessage('Corrija os campos destacados para continuar.')
      return
    }

    setStatusMessage('Autenticando...')
    setStatusTone('idle')
    setIsSubmitting(true)

    try {
      const result = await authenticate({
        email: trimmedEmail,
        password: trimmedPassword,
      })

      const nextSession: StoredSession = {
        email: trimmedEmail,
        token: result.token,
        user: result.user,
        payload: result.payload,
        authenticatedAt: new Date().toISOString(),
      }

      sessionStorage.setItem(SESSION_STORAGE_KEY, JSON.stringify(nextSession))
      setSession(nextSession)
      setStatusTone('success')
      setStatusMessage('Login realizado com sucesso.')
      setPassword('')
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Falha inesperada ao autenticar.'

      setStatusTone('error')
      setStatusMessage(message)
    } finally {
      setIsSubmitting(false)
    }
  }

  const handleCancel = () => {
    setEmail('')
    setPassword('')
    setEmailError('')
    setPasswordError('')
    setStatusMessage('')
    setStatusTone('idle')
  }

  const handleLogout = () => {
    sessionStorage.removeItem(SESSION_STORAGE_KEY)
    setSession(null)
    setPassword('')
    setStatusTone('idle')
    setStatusMessage('Sessao encerrada.')
  }

  if (!session) {
    return (
      <main className="login-page">
        <section className="login-panel" aria-labelledby="login-title">
          <div className="login-copy">
            <p className="login-kicker">TEG Financ</p>
            <h1 id="login-title">Acesse o painel administrativo</h1>
            <p className="login-description">
              Informe email e senha para validar o acesso. Quando a autenticacao
              for aprovada, a aplicacao abre esta area administrativa.
            </p>
          </div>

          <form className="login-card" onSubmit={handleSubmit} noValidate>
            <label className="field-group" htmlFor="email">
              <span>Email</span>
              <input
                id="email"
                name="email"
                type="email"
                placeholder="nome@empresa.com"
                value={email}
                onChange={(event) => setEmail(event.target.value)}
                disabled={isSubmitting}
                aria-invalid={Boolean(emailError)}
                aria-describedby={emailError ? 'email-error' : undefined}
              />
              {emailError ? (
                <strong id="email-error" className="field-error">
                  {emailError}
                </strong>
              ) : null}
            </label>

            <label className="field-group" htmlFor="password">
              <span>Senha</span>
              <input
                id="password"
                name="password"
                type="password"
                placeholder="Digite sua senha"
                value={password}
                onChange={(event) => setPassword(event.target.value)}
                disabled={isSubmitting}
                aria-invalid={Boolean(passwordError)}
                aria-describedby={passwordError ? 'password-error' : undefined}
              />
              {passwordError ? (
                <strong id="password-error" className="field-error">
                  {passwordError}
                </strong>
              ) : null}
            </label>

            <div className="button-row">
              <button type="submit" className="primary-button" disabled={isSubmitting}>
                {isSubmitting ? 'Confirmando...' : 'Confirmar'}
              </button>
              <button
                type="button"
                className="secondary-button"
                onClick={handleCancel}
                disabled={isSubmitting}
              >
                Cancelar
              </button>
            </div>

            <p className={`status-message status-${statusTone}`} aria-live="polite">
              {statusMessage}
            </p>

            <p className="auth-hint">
              Endpoint configurado por <strong>VITE_AUTH_URL</strong>.
            </p>
          </form>
        </section>
      </main>
    )
  }

  return (
    <main className="dashboard-page">
      <aside className="sidebar-menu" aria-label="Menu principal">
        <div>
          <p className="sidebar-brand">TEG Financ</p>
          <h1 className="sidebar-title">Painel Escolar</h1>
        </div>

        <nav>
          <ul className="menu-list">
            <li className="menu-item menu-item-active">Inicio</li>
            <li className="menu-item">Rotas</li>
            <li className="menu-item">Alunos</li>
            <li className="menu-item">Motoristas</li>
            <li className="menu-item">Relatorios</li>
          </ul>
        </nav>

        <div className="sidebar-footer">
          <p>Usuario autenticado</p>
          <strong>{session.email}</strong>
          <button type="button" className="logout-button" onClick={handleLogout}>
            Sair
          </button>
        </div>
      </aside>

      <section className="content-panel" aria-labelledby="content-title">
        <div className="content-copy">
          <p className="content-kicker">Centro de monitoramento</p>
          <h2 id="content-title">Frota de onibus escolar</h2>
          <p className="content-description">
            Area central dedicada a visualizacao do transporte escolar com foco
            nas rotas, status da frota e organizacao do atendimento diario.
          </p>
        </div>

        <div className="bus-stage">
          <SchoolBusArt />
        </div>
      </section>
    </main>
  )
}

export default App
