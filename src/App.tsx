import { useDeferredValue, useEffect, useState } from 'react'
import type { FormEvent } from 'react'
import './App.css'
import { authenticate } from './services/auth'
import { createDreItem, deleteDreItem, listDreItemsPaginated, updateDreItem } from './services/dre'
import type { DreItem } from './services/dre'

type StatusTone = 'idle' | 'error' | 'success'
type ActiveView = 'inicio' | 'dre'
type DreSortField = 'codigo' | 'descricao'
type DreSortDirection = 'asc' | 'desc'

type StoredSession = {
  email: string
  displayName: string
  token: string | null
  user: unknown
  payload: Record<string, unknown>
  authenticatedAt: string
}

const SESSION_STORAGE_KEY = 'tegfinanc.auth'
const DRE_PAGE_SIZE = 5

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

function getUserDisplayName(user: unknown, fallbackEmail: string) {
  if (user && typeof user === 'object') {
    const candidateName = 'name' in user ? user.name : null
    if (typeof candidateName === 'string' && candidateName.trim()) {
      return candidateName.trim()
    }

    const candidateEmail = 'email' in user ? user.email : null
    if (typeof candidateEmail === 'string' && candidateEmail.trim()) {
      return candidateEmail.trim()
    }
  }

  return fallbackEmail
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
  const [activeView, setActiveView] = useState<ActiveView>('inicio')
  const [dreItems, setDreItems] = useState<DreItem[]>([])
  const [dreCodigo, setDreCodigo] = useState('')
  const [dreDescricao, setDreDescricao] = useState('')
  const [dreCodigoError, setDreCodigoError] = useState('')
  const [dreDescricaoError, setDreDescricaoError] = useState('')
  const [dreStatusMessage, setDreStatusMessage] = useState('')
  const [dreStatusTone, setDreStatusTone] = useState<StatusTone>('idle')
  const [isLoadingDre, setIsLoadingDre] = useState(false)
  const [isSavingDre, setIsSavingDre] = useState(false)
  const [isDeletingDre, setIsDeletingDre] = useState(false)
  const [isDreFormVisible, setIsDreFormVisible] = useState(false)
  const [editingDreCodigo, setEditingDreCodigo] = useState<string | null>(null)
  const [dreSearch, setDreSearch] = useState('')
  const [drePage, setDrePage] = useState(1)
  const [dreTotalItems, setDreTotalItems] = useState(0)
  const [dreTotalPages, setDreTotalPages] = useState(1)
  const [dreSortBy, setDreSortBy] = useState<DreSortField>('codigo')
  const [dreSortDirection, setDreSortDirection] = useState<DreSortDirection>('asc')
  const deferredDreSearch = useDeferredValue(dreSearch)

  useEffect(() => {
    setSession(getStoredSession())
  }, [])

  const loadDreItems = async (pageToLoad: number) => {
    setIsLoadingDre(true)
    setDreStatusMessage('Carregando registros da DRE...')
    setDreStatusTone('idle')

    try {
      const result = await listDreItemsPaginated({
        search: deferredDreSearch,
        page: pageToLoad,
        pageSize: DRE_PAGE_SIZE,
        sortBy: dreSortBy,
        sortDirection: dreSortDirection,
      })

      setDreItems(result.items)
      setDreTotalItems(result.total)
      setDreTotalPages(result.totalPages)
      setDrePage(result.page)
      setDreSortBy(result.sortBy)
      setDreSortDirection(result.sortDirection)
      setDreStatusMessage(result.items.length ? '' : 'Nenhum registro encontrado na tabela DRE.')
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Falha ao carregar os registros da DRE.'

      setDreStatusTone('error')
      setDreStatusMessage(message)
    } finally {
      setIsLoadingDre(false)
    }
  }

  useEffect(() => {
    if (!session || activeView !== 'dre') {
      return
    }

    void loadDreItems(drePage)
  }, [activeView, deferredDreSearch, drePage, dreSortBy, dreSortDirection, session])

  useEffect(() => {
    setDrePage(1)
  }, [deferredDreSearch])

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
        displayName: getUserDisplayName(result.user, trimmedEmail),
        token: result.token,
        user: result.user,
        payload: result.payload,
        authenticatedAt: new Date().toISOString(),
      }

      sessionStorage.setItem(SESSION_STORAGE_KEY, JSON.stringify(nextSession))
      setSession(nextSession)
      setStatusTone('success')
      setStatusMessage(`Login realizado com sucesso para ${nextSession.displayName}.`)
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
    setStatusMessage('Fechando a aplicacao...')
    setStatusTone('idle')

    window.open('', '_self')
    window.close()
    window.location.replace('about:blank')
  }

  const handleLogout = () => {
    sessionStorage.removeItem(SESSION_STORAGE_KEY)
    setSession(null)
    setPassword('')
    setStatusTone('idle')
    setStatusMessage('Sessao encerrada.')
    setActiveView('inicio')
  }

  const resetDreForm = () => {
    setDreCodigo('')
    setDreDescricao('')
    setDreCodigoError('')
    setDreDescricaoError('')
    setEditingDreCodigo(null)
  }

  const handleStartInsertDre = () => {
    resetDreForm()
    setDreStatusTone('idle')
    setDreStatusMessage('')
    setIsDreFormVisible(true)
  }

  const handleFilterDreSubmit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault()
    setDrePage(1)
    setDreStatusMessage('Aplicando filtro da DRE...')
    setDreStatusTone('idle')
  }

  const handleClearDreFilter = () => {
    setDreSearch('')
    setDrePage(1)
  }

  const handleSortDre = (field: DreSortField) => {
    setDrePage(1)
    setDreSortBy((currentField) => {
      if (currentField === field) {
        setDreSortDirection((currentDirection) => currentDirection === 'asc' ? 'desc' : 'asc')
        return currentField
      }

      setDreSortDirection('asc')
      return field
    })
  }

  const getSortIndicator = (field: DreSortField) => {
    if (dreSortBy !== field) {
      return '↕'
    }

    return dreSortDirection === 'asc' ? '↑' : '↓'
  }

  const handleStartEditDre = (item: DreItem) => {
    setEditingDreCodigo(item.codigo)
    setDreCodigo(item.codigo)
    setDreDescricao(item.descricao)
    setDreCodigoError('')
    setDreDescricaoError('')
    setDreStatusTone('idle')
    setDreStatusMessage(`Alterando registro ${item.codigo}.`)
    setIsDreFormVisible(true)
  }

  const handleCancelDreForm = () => {
    resetDreForm()
    setIsDreFormVisible(false)
    setDreStatusTone('idle')
    setDreStatusMessage('')
  }

  const handleCreateDre = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault()

    const normalizedCodigo = dreCodigo.trim()
    const normalizedDescricao = dreDescricao.trim()
    const editingCodigo = editingDreCodigo
    let hasError = false

    setDreCodigoError('')
    setDreDescricaoError('')

    if (!normalizedCodigo) {
      setDreCodigoError('Codigo e obrigatorio.')
      hasError = true
    }

    if (!normalizedDescricao) {
      setDreDescricaoError('Descricao e obrigatoria.')
      hasError = true
    }

    if (hasError) {
      setDreStatusTone('error')
      setDreStatusMessage('Corrija os campos da DRE para continuar.')
      return
    }

    setIsSavingDre(true)
    setDreStatusTone('idle')
    setDreStatusMessage(editingCodigo ? 'Alterando registro da DRE...' : 'Gravando registro da DRE...')

    try {
      const savedItem = editingCodigo
        ? await updateDreItem(editingCodigo, {
            codigo: normalizedCodigo,
            descricao: normalizedDescricao,
          })
        : await createDreItem({
            codigo: normalizedCodigo,
            descricao: normalizedDescricao,
          })

      void savedItem
      resetDreForm()
      setIsDreFormVisible(false)
      setDreStatusTone('success')
      setDreStatusMessage(editingCodigo ? 'Registro da DRE alterado com sucesso.' : 'Registro da DRE cadastrado com sucesso.')
      await loadDreItems(editingCodigo ? drePage : 1)
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Falha ao cadastrar registro da DRE.'

      setDreStatusTone('error')
      setDreStatusMessage(message)
    } finally {
      setIsSavingDre(false)
    }
  }

  const handleDeleteDre = async (item: DreItem) => {
    const confirmed = window.confirm(`Excluir o registro ${item.codigo} - ${item.descricao}?`)

    if (!confirmed) {
      return
    }

    setIsDeletingDre(true)
    setDreStatusTone('idle')
    setDreStatusMessage(`Excluindo registro ${item.codigo}...`)

    try {
      const deletedCodigo = await deleteDreItem(item.codigo)
      setDreItems((currentItems) => currentItems.filter((currentItem) => currentItem.codigo !== deletedCodigo))

      if (editingDreCodigo === item.codigo) {
        resetDreForm()
        setIsDreFormVisible(false)
      }

      setDreStatusTone('success')
      setDreStatusMessage('Registro da DRE excluido com sucesso.')
      const nextPage = dreItems.length === 1 && drePage > 1 ? drePage - 1 : drePage
      await loadDreItems(nextPage)
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Falha ao excluir registro da DRE.'

      setDreStatusTone('error')
      setDreStatusMessage(message)
    } finally {
      setIsDeletingDre(false)
    }
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

  const canGoToPreviousDrePage = drePage > 1
  const canGoToNextDrePage = drePage < dreTotalPages

  return (
    <main className="dashboard-page">
      <aside className="sidebar-menu" aria-label="Menu principal">
        <div>
          <p className="sidebar-brand">TEG Financ</p>
          <h1 className="sidebar-title">Painel Escolar</h1>
        </div>

        <nav>
          <ul className="menu-list">
            <li
              className={`menu-item ${activeView === 'inicio' ? 'menu-item-active' : ''}`}
              onClick={() => setActiveView('inicio')}
            >
              Inicio
            </li>
            <li
              className={`menu-item ${activeView === 'dre' ? 'menu-item-active' : ''}`}
              onClick={() => setActiveView('dre')}
            >
              DRE
            </li>
            <li className="menu-item">Rotas</li>
            <li className="menu-item">Alunos</li>
            <li className="menu-item">Motoristas</li>
            <li className="menu-item">Relatorios</li>
          </ul>
        </nav>

        <div className="sidebar-footer">
          <p>Usuario autenticado</p>
          <strong>{session.displayName}</strong>
          <button type="button" className="logout-button" onClick={handleLogout}>
            Sair
          </button>
        </div>
      </aside>

      <section className="content-panel" aria-labelledby="content-title">
        {activeView === 'inicio' ? (
          <>
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
          </>
        ) : (
          <>
            <div className="content-copy">
              <p className="content-kicker">Cadastro administrativo</p>
              <h2 id="content-title">Tabela DRE</h2>
              <p className="content-description">
                Cadastre e consulte os registros da tabela DRE. Os campos codigo e
                descricao sao obrigatorios e nao podem se repetir.
              </p>
            </div>

            <div className="dre-layout">
              <div className="dre-toolbar">
                <button
                  type="button"
                  className="primary-button dre-insert-button"
                  onClick={handleStartInsertDre}
                  disabled={isSavingDre || isDeletingDre}
                >
                  Inserir registro
                </button>

                <form className="dre-filter-form" onSubmit={handleFilterDreSubmit}>
                  <input
                    className="dre-filter-input"
                    type="text"
                    placeholder="Filtrar por codigo ou descricao"
                    value={dreSearch}
                    onChange={(event) => setDreSearch(event.target.value)}
                  />
                  <button type="submit" className="secondary-button dre-filter-button">
                    Filtrar
                  </button>
                  <button type="button" className="secondary-button dre-filter-button" onClick={handleClearDreFilter}>
                    Limpar
                  </button>
                </form>
              </div>

              {isDreFormVisible ? (
                <form className="dre-card dre-form" onSubmit={handleCreateDre} noValidate>
                  <h3>{editingDreCodigo ? 'Alterar registro' : 'Novo registro'}</h3>

                  <label className="field-group" htmlFor="dre-codigo">
                    <span>Codigo</span>
                    <input
                      id="dre-codigo"
                      name="codigo"
                      type="text"
                      value={dreCodigo}
                      onChange={(event) => setDreCodigo(event.target.value)}
                      disabled={isSavingDre}
                      aria-invalid={Boolean(dreCodigoError)}
                    />
                    {dreCodigoError ? <strong className="field-error">{dreCodigoError}</strong> : null}
                  </label>

                  <label className="field-group" htmlFor="dre-descricao">
                    <span>Descricao</span>
                    <input
                      id="dre-descricao"
                      name="descricao"
                      type="text"
                      value={dreDescricao}
                      onChange={(event) => setDreDescricao(event.target.value)}
                      disabled={isSavingDre}
                      aria-invalid={Boolean(dreDescricaoError)}
                    />
                    {dreDescricaoError ? <strong className="field-error">{dreDescricaoError}</strong> : null}
                  </label>

                  <div className="button-row dre-button-row">
                    <button type="submit" className="primary-button" disabled={isSavingDre}>
                      {isSavingDre ? 'Salvando...' : editingDreCodigo ? 'Salvar alteracao' : 'Salvar DRE'}
                    </button>
                    <button type="button" className="secondary-button" onClick={handleCancelDreForm} disabled={isSavingDre}>
                      Cancelar
                    </button>
                  </div>
                </form>
              ) : null}

              <div className="dre-card dre-list-card">
                <div className="dre-list-header">
                  <h3>Registros cadastrados</h3>
                  <span>
                    {isLoadingDre ? 'Atualizando...' : `${dreTotalItems} item(ns) encontrados`}
                  </span>
                </div>

                <div className="dre-table-wrapper">
                  <table className="dre-table">
                    <thead>
                      <tr>
                        <th>
                          <button type="button" className="dre-sort-button" onClick={() => handleSortDre('codigo')}>
                            Codigo <span>{getSortIndicator('codigo')}</span>
                          </button>
                        </th>
                        <th>
                          <button type="button" className="dre-sort-button" onClick={() => handleSortDre('descricao')}>
                            Descricao <span>{getSortIndicator('descricao')}</span>
                          </button>
                        </th>
                        <th className="dre-actions-column">Acoes</th>
                      </tr>
                    </thead>
                    <tbody>
                      {dreItems.map((item) => (
                        <tr key={item.codigo}>
                          <td>{item.codigo}</td>
                          <td>{item.descricao}</td>
                          <td>
                            <div className="dre-row-actions">
                              <button type="button" className="row-action-button row-action-edit" onClick={() => handleStartEditDre(item)}>
                                Alterar
                              </button>
                              <button type="button" className="row-action-button row-action-delete" onClick={() => handleDeleteDre(item)}>
                                Excluir
                              </button>
                            </div>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>

                  {!isLoadingDre && dreItems.length === 0 ? (
                    <p className="empty-state">Nenhum registro da DRE encontrado.</p>
                  ) : null}
                </div>

                <p className={`status-message status-${dreStatusTone}`} aria-live="polite">
                  {dreStatusMessage}
                </p>

                <div className="dre-pagination">
                  <button
                    type="button"
                    className="secondary-button dre-pagination-button"
                    onClick={() => setDrePage((currentPage) => currentPage - 1)}
                    disabled={!canGoToPreviousDrePage || isLoadingDre}
                  >
                    Anterior
                  </button>
                  <span className="dre-pagination-info">
                    Pagina {drePage} de {dreTotalPages}
                  </span>
                  <button
                    type="button"
                    className="secondary-button dre-pagination-button"
                    onClick={() => setDrePage((currentPage) => currentPage + 1)}
                    disabled={!canGoToNextDrePage || isLoadingDre}
                  >
                    Proxima
                  </button>
                </div>
              </div>
            </div>
          </>
        )}
      </section>
    </main>
  )
}

export default App
