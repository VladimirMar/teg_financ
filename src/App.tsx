import { useCallback, useDeferredValue, useEffect, useState } from 'react'
import type { FormEvent } from 'react'
import './App.css'
import { authenticate } from './services/auth'
import { createDreItem, deleteDreItem, listDreItemsPaginated, updateDreItem } from './services/dre'
import type { DreItem } from './services/dre'
import { createModalidadeItem, deleteModalidadeItem, listModalidadeItemsPaginated, updateModalidadeItem } from './services/modalidade'
import type { ModalidadeItem } from './services/modalidade'
import { createTitularItem, deleteTitularItem, listTitularItemsPaginated, updateTitularItem } from './services/titular'
import type { TitularItem } from './services/titular'
import { createMarcaModeloItem, deleteMarcaModeloItem, listMarcaModeloItemsPaginated, updateMarcaModeloItem } from './services/marcaModelo'
import type { MarcaModeloItem } from './services/marcaModelo'
import { createSeguradoraItem, deleteSeguradoraItem, listSeguradoraItemsPaginated, updateSeguradoraItem } from './services/seguradora'
import type { SeguradoraItem } from './services/seguradora'

type StatusTone = 'idle' | 'error' | 'success'
type ActiveView = 'inicio' | 'dre' | 'modalidade' | 'titular' | 'marcaModelo' | 'seguradora' | 'troca' | 'acesso' | 'loginDre' | 'condutor' | 'monitor' | 'credenciada' | 'veiculo' | 'ordemServico'
type DreSortField = 'codigo' | 'descricao'
type DreSortDirection = 'asc' | 'desc'
type TitularSortField = 'codigo' | 'cnpj_cpf' | 'titular'
type MarcaModeloSortField = 'codigo' | 'descricao'
type SeguradoraSortField = 'codigo' | 'controle' | 'descricao'

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

function formatCpfOrCnpj(value: string) {
  const digits = value.replace(/\D/g, '').slice(0, 14)

  if (digits.length <= 3) {
    return digits
  }

  if (digits.length <= 6) {
    return `${digits.slice(0, 3)}.${digits.slice(3)}`
  }

  if (digits.length <= 9) {
    return `${digits.slice(0, 3)}.${digits.slice(3, 6)}.${digits.slice(6)}`
  }

  if (digits.length <= 11) {
    return `${digits.slice(0, 3)}.${digits.slice(3, 6)}.${digits.slice(6, 9)}-${digits.slice(9, 11)}`
  }

  if (digits.length <= 12) {
    return `${digits.slice(0, 2)}.${digits.slice(2, 5)}.${digits.slice(5, 8)}/${digits.slice(8)}`
  }

  return `${digits.slice(0, 2)}.${digits.slice(2, 5)}.${digits.slice(5, 8)}/${digits.slice(8, 12)}-${digits.slice(12, 14)}`
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
  const [modalidadeItems, setModalidadeItems] = useState<ModalidadeItem[]>([])
  const [modalidadeCodigo, setModalidadeCodigo] = useState('')
  const [modalidadeDescricao, setModalidadeDescricao] = useState('')
  const [modalidadeCodigoError, setModalidadeCodigoError] = useState('')
  const [modalidadeDescricaoError, setModalidadeDescricaoError] = useState('')
  const [modalidadeStatusMessage, setModalidadeStatusMessage] = useState('')
  const [modalidadeStatusTone, setModalidadeStatusTone] = useState<StatusTone>('idle')
  const [isLoadingModalidade, setIsLoadingModalidade] = useState(false)
  const [isSavingModalidade, setIsSavingModalidade] = useState(false)
  const [isDeletingModalidade, setIsDeletingModalidade] = useState(false)
  const [isModalidadeFormVisible, setIsModalidadeFormVisible] = useState(false)
  const [editingModalidadeCodigo, setEditingModalidadeCodigo] = useState<string | null>(null)
  const [modalidadeSearch, setModalidadeSearch] = useState('')
  const [modalidadePage, setModalidadePage] = useState(1)
  const [modalidadeTotalItems, setModalidadeTotalItems] = useState(0)
  const [modalidadeTotalPages, setModalidadeTotalPages] = useState(1)
  const [modalidadeSortBy, setModalidadeSortBy] = useState<DreSortField>('codigo')
  const [modalidadeSortDirection, setModalidadeSortDirection] = useState<DreSortDirection>('asc')
  const deferredModalidadeSearch = useDeferredValue(modalidadeSearch)
  const [titularItems, setTitularItems] = useState<TitularItem[]>([])
  const [titularCodigo, setTitularCodigo] = useState('')
  const [titularCnpjCpf, setTitularCnpjCpf] = useState('')
  const [titularNome, setTitularNome] = useState('')
  const [titularCodigoError, setTitularCodigoError] = useState('')
  const [titularCnpjCpfError, setTitularCnpjCpfError] = useState('')
  const [titularNomeError, setTitularNomeError] = useState('')
  const [titularStatusMessage, setTitularStatusMessage] = useState('')
  const [titularStatusTone, setTitularStatusTone] = useState<StatusTone>('idle')
  const [isLoadingTitular, setIsLoadingTitular] = useState(false)
  const [isSavingTitular, setIsSavingTitular] = useState(false)
  const [isDeletingTitular, setIsDeletingTitular] = useState(false)
  const [isTitularFormVisible, setIsTitularFormVisible] = useState(false)
  const [editingTitularCodigo, setEditingTitularCodigo] = useState<string | null>(null)
  const [titularSearch, setTitularSearch] = useState('')
  const [titularPage, setTitularPage] = useState(1)
  const [titularTotalItems, setTitularTotalItems] = useState(0)
  const [titularTotalPages, setTitularTotalPages] = useState(1)
  const [titularSortBy, setTitularSortBy] = useState<TitularSortField>('codigo')
  const [titularSortDirection, setTitularSortDirection] = useState<DreSortDirection>('asc')
  const deferredTitularSearch = useDeferredValue(titularSearch)
  const [marcaModeloItems, setMarcaModeloItems] = useState<MarcaModeloItem[]>([])
  const [marcaModeloCodigo, setMarcaModeloCodigo] = useState('')
  const [marcaModeloDescricao, setMarcaModeloDescricao] = useState('')
  const [marcaModeloCodigoError, setMarcaModeloCodigoError] = useState('')
  const [marcaModeloDescricaoError, setMarcaModeloDescricaoError] = useState('')
  const [marcaModeloStatusMessage, setMarcaModeloStatusMessage] = useState('')
  const [marcaModeloStatusTone, setMarcaModeloStatusTone] = useState<StatusTone>('idle')
  const [isLoadingMarcaModelo, setIsLoadingMarcaModelo] = useState(false)
  const [isSavingMarcaModelo, setIsSavingMarcaModelo] = useState(false)
  const [isDeletingMarcaModelo, setIsDeletingMarcaModelo] = useState(false)
  const [isMarcaModeloFormVisible, setIsMarcaModeloFormVisible] = useState(false)
  const [editingMarcaModeloCodigo, setEditingMarcaModeloCodigo] = useState<string | null>(null)
  const [marcaModeloSearch, setMarcaModeloSearch] = useState('')
  const [marcaModeloPage, setMarcaModeloPage] = useState(1)
  const [marcaModeloTotalItems, setMarcaModeloTotalItems] = useState(0)
  const [marcaModeloTotalPages, setMarcaModeloTotalPages] = useState(1)
  const [marcaModeloSortBy, setMarcaModeloSortBy] = useState<MarcaModeloSortField>('codigo')
  const [marcaModeloSortDirection, setMarcaModeloSortDirection] = useState<DreSortDirection>('asc')
  const deferredMarcaModeloSearch = useDeferredValue(marcaModeloSearch)
  const [seguradoraItems, setSeguradoraItems] = useState<SeguradoraItem[]>([])
  const [seguradoraCodigo, setSeguradoraCodigo] = useState('')
  const [seguradoraControle, setSeguradoraControle] = useState('')
  const [seguradoraLista, setSeguradoraLista] = useState('')
  const [seguradoraCodigoError, setSeguradoraCodigoError] = useState('')
  const [seguradoraControleError, setSeguradoraControleError] = useState('')
  const [seguradoraListaError, setSeguradoraListaError] = useState('')
  const [seguradoraStatusMessage, setSeguradoraStatusMessage] = useState('')
  const [seguradoraStatusTone, setSeguradoraStatusTone] = useState<StatusTone>('idle')
  const [isLoadingSeguradora, setIsLoadingSeguradora] = useState(false)
  const [isSavingSeguradora, setIsSavingSeguradora] = useState(false)
  const [isDeletingSeguradora, setIsDeletingSeguradora] = useState(false)
  const [isSeguradoraFormVisible, setIsSeguradoraFormVisible] = useState(false)
  const [editingSeguradoraCodigo, setEditingSeguradoraCodigo] = useState<string | null>(null)
  const [seguradoraSearch, setSeguradoraSearch] = useState('')
  const [seguradoraPage, setSeguradoraPage] = useState(1)
  const [seguradoraTotalItems, setSeguradoraTotalItems] = useState(0)
  const [seguradoraTotalPages, setSeguradoraTotalPages] = useState(1)
  const [seguradoraSortBy, setSeguradoraSortBy] = useState<SeguradoraSortField>('codigo')
  const [seguradoraSortDirection, setSeguradoraSortDirection] = useState<DreSortDirection>('asc')
  const deferredSeguradoraSearch = useDeferredValue(seguradoraSearch)

  useEffect(() => {
    setSession(getStoredSession())
  }, [])

  const loadDreItems = useCallback(async (pageToLoad: number) => {
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
  }, [deferredDreSearch, dreSortBy, dreSortDirection])

  const loadModalidadeItems = useCallback(async (pageToLoad: number) => {
    setIsLoadingModalidade(true)
    setModalidadeStatusMessage('Carregando registros de modalidade...')
    setModalidadeStatusTone('idle')

    try {
      const result = await listModalidadeItemsPaginated({
        search: deferredModalidadeSearch,
        page: pageToLoad,
        pageSize: DRE_PAGE_SIZE,
        sortBy: modalidadeSortBy,
        sortDirection: modalidadeSortDirection,
      })

      setModalidadeItems(result.items)
      setModalidadeTotalItems(result.total)
      setModalidadeTotalPages(result.totalPages)
      setModalidadePage(result.page)
      setModalidadeSortBy(result.sortBy)
      setModalidadeSortDirection(result.sortDirection)
      setModalidadeStatusMessage(result.items.length ? '' : 'Nenhum registro encontrado na tabela Modalidade.')
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Falha ao carregar os registros de modalidade.'

      setModalidadeStatusTone('error')
      setModalidadeStatusMessage(message)
    } finally {
      setIsLoadingModalidade(false)
    }
  }, [deferredModalidadeSearch, modalidadeSortBy, modalidadeSortDirection])

  const loadSeguradoraItems = useCallback(async (pageToLoad: number) => {
    setIsLoadingSeguradora(true)
    setSeguradoraStatusMessage('Carregando registros de seguradoras...')
    setSeguradoraStatusTone('idle')

    try {
      const result = await listSeguradoraItemsPaginated({
        search: deferredSeguradoraSearch,
        page: pageToLoad,
        pageSize: DRE_PAGE_SIZE,
        sortBy: seguradoraSortBy,
        sortDirection: seguradoraSortDirection,
      })

      setSeguradoraItems(result.items)
      setSeguradoraTotalItems(result.total)
      setSeguradoraTotalPages(result.totalPages)
      setSeguradoraPage(result.page)
      setSeguradoraSortBy(result.sortBy)
      setSeguradoraSortDirection(result.sortDirection)
      setSeguradoraStatusMessage(result.items.length ? '' : 'Nenhum registro encontrado na tabela seguradora.')
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Falha ao carregar os registros de seguradoras.'

      setSeguradoraStatusTone('error')
      setSeguradoraStatusMessage(message)
    } finally {
      setIsLoadingSeguradora(false)
    }
  }, [deferredSeguradoraSearch, seguradoraSortBy, seguradoraSortDirection])

  const loadTitularItems = useCallback(async (pageToLoad: number) => {
    setIsLoadingTitular(true)
    setTitularStatusMessage('Carregando registros de titulares do CRM...')
    setTitularStatusTone('idle')

    try {
      const result = await listTitularItemsPaginated({
        search: deferredTitularSearch,
        page: pageToLoad,
        pageSize: DRE_PAGE_SIZE,
        sortBy: titularSortBy,
        sortDirection: titularSortDirection,
      })

      setTitularItems(result.items)
      setTitularTotalItems(result.total)
      setTitularTotalPages(result.totalPages)
      setTitularPage(result.page)
      setTitularSortBy(result.sortBy)
      setTitularSortDirection(result.sortDirection)
      setTitularStatusMessage(result.items.length ? '' : 'Nenhum registro encontrado na tabela titular do CRM.')
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Falha ao carregar os registros de titulares do CRM.'

      setTitularStatusTone('error')
      setTitularStatusMessage(message)
    } finally {
      setIsLoadingTitular(false)
    }
  }, [deferredTitularSearch, titularSortBy, titularSortDirection])

  const loadMarcaModeloItems = useCallback(async (pageToLoad: number) => {
    setIsLoadingMarcaModelo(true)
    setMarcaModeloStatusMessage('Carregando registros de marca/modelo...')
    setMarcaModeloStatusTone('idle')

    try {
      const result = await listMarcaModeloItemsPaginated({
        search: deferredMarcaModeloSearch,
        page: pageToLoad,
        pageSize: DRE_PAGE_SIZE,
        sortBy: marcaModeloSortBy,
        sortDirection: marcaModeloSortDirection,
      })

      setMarcaModeloItems(result.items)
      setMarcaModeloTotalItems(result.total)
      setMarcaModeloTotalPages(result.totalPages)
      setMarcaModeloPage(result.page)
      setMarcaModeloSortBy(result.sortBy)
      setMarcaModeloSortDirection(result.sortDirection)
      setMarcaModeloStatusMessage(result.items.length ? '' : 'Nenhum registro encontrado na tabela marca/modelo.')
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Falha ao carregar os registros de marca/modelo.'

      setMarcaModeloStatusTone('error')
      setMarcaModeloStatusMessage(message)
    } finally {
      setIsLoadingMarcaModelo(false)
    }
  }, [deferredMarcaModeloSearch, marcaModeloSortBy, marcaModeloSortDirection])

  useEffect(() => {
    if (!session || activeView !== 'dre') {
      return
    }

    void loadDreItems(drePage)
  }, [activeView, drePage, loadDreItems, session])

  useEffect(() => {
    if (!session || activeView !== 'modalidade') {
      return
    }

    void loadModalidadeItems(modalidadePage)
  }, [activeView, loadModalidadeItems, modalidadePage, session])

  useEffect(() => {
    if (!session || activeView !== 'seguradora') {
      return
    }

    void loadSeguradoraItems(seguradoraPage)
  }, [activeView, loadSeguradoraItems, seguradoraPage, session])

  useEffect(() => {
    if (!session || activeView !== 'marcaModelo') {
      return
    }

    void loadMarcaModeloItems(marcaModeloPage)
  }, [activeView, loadMarcaModeloItems, marcaModeloPage, session])

  useEffect(() => {
    setDrePage(1)
  }, [deferredDreSearch])

  useEffect(() => {
    setModalidadePage(1)
  }, [deferredModalidadeSearch])

  useEffect(() => {
    if (!session || activeView !== 'titular') {
      return
    }

    void loadTitularItems(titularPage)
  }, [activeView, loadTitularItems, session, titularPage])

  useEffect(() => {
    setTitularPage(1)
  }, [deferredTitularSearch])

  useEffect(() => {
    setSeguradoraPage(1)
  }, [deferredSeguradoraSearch])

  useEffect(() => {
    setMarcaModeloPage(1)
  }, [deferredMarcaModeloSearch])

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

  const resetModalidadeForm = () => {
    setModalidadeCodigo('')
    setModalidadeDescricao('')
    setModalidadeCodigoError('')
    setModalidadeDescricaoError('')
    setEditingModalidadeCodigo(null)
  }

  const handleStartInsertDre = () => {
    resetDreForm()
    setDreStatusTone('idle')
    setDreStatusMessage('')
    setIsDreFormVisible(true)
  }

  const handleStartInsertModalidade = () => {
    resetModalidadeForm()
    setModalidadeStatusTone('idle')
    setModalidadeStatusMessage('')
    setIsModalidadeFormVisible(true)
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

  const handleFilterModalidadeSubmit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault()
    setModalidadePage(1)
    setModalidadeStatusMessage('Aplicando filtro de modalidade...')
    setModalidadeStatusTone('idle')
  }

  const handleClearModalidadeFilter = () => {
    setModalidadeSearch('')
    setModalidadePage(1)
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

  const handleSortModalidade = (field: DreSortField) => {
    setModalidadePage(1)
    setModalidadeSortBy((currentField) => {
      if (currentField === field) {
        setModalidadeSortDirection((currentDirection) => currentDirection === 'asc' ? 'desc' : 'asc')
        return currentField
      }

      setModalidadeSortDirection('asc')
      return field
    })
  }

  const getModalidadeSortIndicator = (field: DreSortField) => {
    if (modalidadeSortBy !== field) {
      return '↕'
    }

    return modalidadeSortDirection === 'asc' ? '↑' : '↓'
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

  const handleStartEditModalidade = (item: ModalidadeItem) => {
    setEditingModalidadeCodigo(item.codigo)
    setModalidadeCodigo(item.codigo)
    setModalidadeDescricao(item.descricao)
    setModalidadeCodigoError('')
    setModalidadeDescricaoError('')
    setModalidadeStatusTone('idle')
    setModalidadeStatusMessage(`Alterando registro ${item.codigo}.`)
    setIsModalidadeFormVisible(true)
  }

  const handleCancelModalidadeForm = () => {
    resetModalidadeForm()
    setIsModalidadeFormVisible(false)
    setModalidadeStatusTone('idle')
    setModalidadeStatusMessage('')
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

  const handleCreateModalidade = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault()

    const normalizedCodigo = modalidadeCodigo.trim()
    const normalizedDescricao = modalidadeDescricao.trim()
    const editingCodigo = editingModalidadeCodigo
    let hasError = false

    setModalidadeCodigoError('')
    setModalidadeDescricaoError('')

    if (!normalizedCodigo) {
      setModalidadeCodigoError('Codigo e obrigatorio.')
      hasError = true
    }

    if (!normalizedDescricao) {
      setModalidadeDescricaoError('Descricao e obrigatoria.')
      hasError = true
    }

    if (hasError) {
      setModalidadeStatusTone('error')
      setModalidadeStatusMessage('Corrija os campos da modalidade para continuar.')
      return
    }

    setIsSavingModalidade(true)
    setModalidadeStatusTone('idle')
    setModalidadeStatusMessage(editingCodigo ? 'Alterando registro da modalidade...' : 'Gravando registro da modalidade...')

    try {
      const savedItem = editingCodigo
        ? await updateModalidadeItem(editingCodigo, {
            codigo: normalizedCodigo,
            descricao: normalizedDescricao,
          })
        : await createModalidadeItem({
            codigo: normalizedCodigo,
            descricao: normalizedDescricao,
          })

      void savedItem
      resetModalidadeForm()
      setIsModalidadeFormVisible(false)
      setModalidadeStatusTone('success')
      setModalidadeStatusMessage(editingCodigo ? 'Registro da modalidade alterado com sucesso.' : 'Registro da modalidade cadastrado com sucesso.')
      await loadModalidadeItems(editingCodigo ? modalidadePage : 1)
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Falha ao cadastrar registro da modalidade.'

      setModalidadeStatusTone('error')
      setModalidadeStatusMessage(message)
    } finally {
      setIsSavingModalidade(false)
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

  const handleDeleteModalidade = async (item: ModalidadeItem) => {
    const confirmed = window.confirm(`Excluir o registro ${item.codigo} - ${item.descricao}?`)

    if (!confirmed) {
      return
    }

    setIsDeletingModalidade(true)
    setModalidadeStatusTone('idle')
    setModalidadeStatusMessage(`Excluindo registro ${item.codigo}...`)

    try {
      const deletedCodigo = await deleteModalidadeItem(item.codigo)
      setModalidadeItems((currentItems) => currentItems.filter((currentItem) => currentItem.codigo !== deletedCodigo))

      if (editingModalidadeCodigo === item.codigo) {
        resetModalidadeForm()
        setIsModalidadeFormVisible(false)
      }

      setModalidadeStatusTone('success')
      setModalidadeStatusMessage('Registro da modalidade excluido com sucesso.')
      const nextPage = modalidadeItems.length === 1 && modalidadePage > 1 ? modalidadePage - 1 : modalidadePage
      await loadModalidadeItems(nextPage)
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Falha ao excluir registro da modalidade.'

      setModalidadeStatusTone('error')
      setModalidadeStatusMessage(message)
    } finally {
      setIsDeletingModalidade(false)
    }
  }

  const resetTitularForm = () => {
    setTitularCodigo('')
    setTitularCnpjCpf('')
    setTitularNome('')
    setTitularCodigoError('')
    setTitularCnpjCpfError('')
    setTitularNomeError('')
    setEditingTitularCodigo(null)
  }

  const handleStartInsertTitular = () => {
    resetTitularForm()
    setTitularStatusTone('idle')
    setTitularStatusMessage('')
    setIsTitularFormVisible(true)
  }

  const handleFilterTitularSubmit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault()
    setTitularPage(1)
    setTitularStatusMessage('Aplicando filtro de titular do CRM...')
    setTitularStatusTone('idle')
  }

  const handleClearTitularFilter = () => {
    setTitularSearch('')
    setTitularPage(1)
  }

  const handleSortTitular = (field: TitularSortField) => {
    setTitularPage(1)
    setTitularSortBy((currentField) => {
      if (currentField === field) {
        setTitularSortDirection((currentDirection) => currentDirection === 'asc' ? 'desc' : 'asc')
        return currentField
      }

      setTitularSortDirection('asc')
      return field
    })
  }

  const getTitularSortIndicator = (field: TitularSortField) => {
    if (titularSortBy !== field) {
      return '↕'
    }

    return titularSortDirection === 'asc' ? '↑' : '↓'
  }

  const handleStartEditTitular = (item: TitularItem) => {
    setEditingTitularCodigo(item.codigo)
    setTitularCodigo(item.codigo)
    setTitularCnpjCpf(formatCpfOrCnpj(item.cnpj_cpf))
    setTitularNome(item.titular)
    setTitularCodigoError('')
    setTitularCnpjCpfError('')
    setTitularNomeError('')
    setTitularStatusTone('idle')
    setTitularStatusMessage(`Alterando registro ${item.codigo}.`)
    setIsTitularFormVisible(true)
  }

  const handleCancelTitularForm = () => {
    resetTitularForm()
    setIsTitularFormVisible(false)
    setTitularStatusTone('idle')
    setTitularStatusMessage('')
  }

  const handleCreateTitular = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault()

    const normalizedCodigo = titularCodigo.trim()
    const normalizedCnpjCpf = titularCnpjCpf.trim()
    const normalizedTitular = titularNome.trim()
    const editingCodigo = editingTitularCodigo
    let hasError = false

    setTitularCodigoError('')
    setTitularCnpjCpfError('')
    setTitularNomeError('')

    if (!normalizedCodigo) {
      setTitularCodigoError('Codigo e obrigatorio.')
      hasError = true
    }

    if (!normalizedCnpjCpf) {
      setTitularCnpjCpfError('CNPJ/CPF e obrigatorio.')
      hasError = true
    }

    if (!normalizedTitular) {
      setTitularNomeError('Titular do CRM e obrigatorio.')
      hasError = true
    }

    if (hasError) {
      setTitularStatusTone('error')
      setTitularStatusMessage('Corrija os campos de titular do CRM para continuar.')
      return
    }

    setIsSavingTitular(true)
    setTitularStatusTone('idle')
    setTitularStatusMessage(editingCodigo ? 'Alterando registro de titular do CRM...' : 'Gravando registro de titular do CRM...')

    try {
      const savedItem = editingCodigo
        ? await updateTitularItem(editingCodigo, {
            codigo: normalizedCodigo,
            cnpj_cpf: normalizedCnpjCpf,
            titular: normalizedTitular,
          })
        : await createTitularItem({
            codigo: normalizedCodigo,
            cnpj_cpf: normalizedCnpjCpf,
            titular: normalizedTitular,
          })

      void savedItem
      resetTitularForm()
      setIsTitularFormVisible(false)
      setTitularStatusTone('success')
      setTitularStatusMessage(editingCodigo ? 'Registro de titular do CRM alterado com sucesso.' : 'Registro de titular do CRM cadastrado com sucesso.')
      await loadTitularItems(editingCodigo ? titularPage : 1)
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Falha ao cadastrar titular do CRM.'

      setTitularStatusTone('error')
      setTitularStatusMessage(message)
    } finally {
      setIsSavingTitular(false)
    }
  }

  const handleDeleteTitular = async (item: TitularItem) => {
    const confirmed = window.confirm(`Excluir o registro ${item.codigo} - ${item.titular}?`)

    if (!confirmed) {
      return
    }

    setIsDeletingTitular(true)
    setTitularStatusTone('idle')
    setTitularStatusMessage(`Excluindo registro ${item.codigo}...`)

    try {
      const deletedCodigo = await deleteTitularItem(item.codigo)
      setTitularItems((currentItems) => currentItems.filter((currentItem) => currentItem.codigo !== deletedCodigo))

      if (editingTitularCodigo === item.codigo) {
        resetTitularForm()
        setIsTitularFormVisible(false)
      }

      setTitularStatusTone('success')
      setTitularStatusMessage('Registro de titular do CRM excluido com sucesso.')
      const nextPage = titularItems.length === 1 && titularPage > 1 ? titularPage - 1 : titularPage
      await loadTitularItems(nextPage)
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Falha ao excluir titular do CRM.'

      setTitularStatusTone('error')
      setTitularStatusMessage(message)
    } finally {
      setIsDeletingTitular(false)
    }
  }

  const resetMarcaModeloForm = () => {
    setMarcaModeloCodigo('')
    setMarcaModeloDescricao('')
    setMarcaModeloCodigoError('')
    setMarcaModeloDescricaoError('')
    setEditingMarcaModeloCodigo(null)
  }

  const handleStartInsertMarcaModelo = () => {
    resetMarcaModeloForm()
    setMarcaModeloStatusTone('idle')
    setMarcaModeloStatusMessage('')
    setIsMarcaModeloFormVisible(true)
  }

  const handleFilterMarcaModeloSubmit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault()
    setMarcaModeloPage(1)
    setMarcaModeloStatusMessage('Aplicando filtro de marca/modelo...')
    setMarcaModeloStatusTone('idle')
  }

  const handleClearMarcaModeloFilter = () => {
    setMarcaModeloSearch('')
    setMarcaModeloPage(1)
  }

  const handleSortMarcaModelo = (field: MarcaModeloSortField) => {
    setMarcaModeloPage(1)
    setMarcaModeloSortBy((currentField) => {
      if (currentField === field) {
        setMarcaModeloSortDirection((currentDirection) => currentDirection === 'asc' ? 'desc' : 'asc')
        return currentField
      }

      setMarcaModeloSortDirection('asc')
      return field
    })
  }

  const getMarcaModeloSortIndicator = (field: MarcaModeloSortField) => {
    if (marcaModeloSortBy !== field) {
      return '↕'
    }

    return marcaModeloSortDirection === 'asc' ? '↑' : '↓'
  }

  const handleStartEditMarcaModelo = (item: MarcaModeloItem) => {
    setEditingMarcaModeloCodigo(item.codigo)
    setMarcaModeloCodigo(item.codigo)
    setMarcaModeloDescricao(item.descricao)
    setMarcaModeloCodigoError('')
    setMarcaModeloDescricaoError('')
    setMarcaModeloStatusTone('idle')
    setMarcaModeloStatusMessage(`Alterando registro ${item.codigo}.`)
    setIsMarcaModeloFormVisible(true)
  }

  const handleCancelMarcaModeloForm = () => {
    resetMarcaModeloForm()
    setIsMarcaModeloFormVisible(false)
    setMarcaModeloStatusTone('idle')
    setMarcaModeloStatusMessage('')
  }

  const handleCreateMarcaModelo = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault()

    const normalizedCodigo = marcaModeloCodigo.trim()
    const normalizedDescricao = marcaModeloDescricao.trim()
    const editingCodigo = editingMarcaModeloCodigo
    let hasError = false

    setMarcaModeloCodigoError('')
    setMarcaModeloDescricaoError('')

    if (!normalizedCodigo) {
      setMarcaModeloCodigoError('Codigo e obrigatorio.')
      hasError = true
    }

    if (!normalizedDescricao) {
      setMarcaModeloDescricaoError('Descricao e obrigatoria.')
      hasError = true
    }

    if (hasError) {
      setMarcaModeloStatusTone('error')
      setMarcaModeloStatusMessage('Corrija os campos de marca/modelo para continuar.')
      return
    }

    setIsSavingMarcaModelo(true)
    setMarcaModeloStatusTone('idle')
    setMarcaModeloStatusMessage(editingCodigo ? 'Alterando registro de marca/modelo...' : 'Gravando registro de marca/modelo...')

    try {
      const savedItem = editingCodigo
        ? await updateMarcaModeloItem(editingCodigo, {
            codigo: normalizedCodigo,
            descricao: normalizedDescricao,
          })
        : await createMarcaModeloItem({
            codigo: normalizedCodigo,
            descricao: normalizedDescricao,
          })

      void savedItem
      resetMarcaModeloForm()
      setIsMarcaModeloFormVisible(false)
      setMarcaModeloStatusTone('success')
      setMarcaModeloStatusMessage(editingCodigo ? 'Registro de marca/modelo alterado com sucesso.' : 'Registro de marca/modelo cadastrado com sucesso.')
      await loadMarcaModeloItems(editingCodigo ? marcaModeloPage : 1)
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Falha ao cadastrar registro de marca/modelo.'

      setMarcaModeloStatusTone('error')
      setMarcaModeloStatusMessage(message)
    } finally {
      setIsSavingMarcaModelo(false)
    }
  }

  const handleDeleteMarcaModelo = async (item: MarcaModeloItem) => {
    const confirmed = window.confirm(`Excluir o registro ${item.codigo} - ${item.descricao}?`)

    if (!confirmed) {
      return
    }

    setIsDeletingMarcaModelo(true)
    setMarcaModeloStatusTone('idle')
    setMarcaModeloStatusMessage(`Excluindo registro ${item.codigo}...`)

    try {
      const deletedCodigo = await deleteMarcaModeloItem(item.codigo)
      setMarcaModeloItems((currentItems) => currentItems.filter((currentItem) => currentItem.codigo !== deletedCodigo))

      if (editingMarcaModeloCodigo === item.codigo) {
        resetMarcaModeloForm()
        setIsMarcaModeloFormVisible(false)
      }

      setMarcaModeloStatusTone('success')
      setMarcaModeloStatusMessage('Registro de marca/modelo excluido com sucesso.')
      const nextPage = marcaModeloItems.length === 1 && marcaModeloPage > 1 ? marcaModeloPage - 1 : marcaModeloPage
      await loadMarcaModeloItems(nextPage)
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Falha ao excluir registro de marca/modelo.'

      setMarcaModeloStatusTone('error')
      setMarcaModeloStatusMessage(message)
    } finally {
      setIsDeletingMarcaModelo(false)
    }
  }

  const resetSeguradoraForm = () => {
    setSeguradoraCodigo('')
    setSeguradoraControle('')
    setSeguradoraLista('')
    setSeguradoraCodigoError('')
    setSeguradoraControleError('')
    setSeguradoraListaError('')
    setEditingSeguradoraCodigo(null)
  }

  const handleStartInsertSeguradora = () => {
    resetSeguradoraForm()
    setSeguradoraStatusTone('idle')
    setSeguradoraStatusMessage('')
    setIsSeguradoraFormVisible(true)
  }

  const handleFilterSeguradoraSubmit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault()
    setSeguradoraPage(1)
    setSeguradoraStatusMessage('Aplicando filtro de seguradoras...')
    setSeguradoraStatusTone('idle')
  }

  const handleClearSeguradoraFilter = () => {
    setSeguradoraSearch('')
    setSeguradoraPage(1)
  }

  const handleSortSeguradora = (field: SeguradoraSortField) => {
    setSeguradoraPage(1)
    setSeguradoraSortBy((currentField) => {
      if (currentField === field) {
        setSeguradoraSortDirection((currentDirection) => currentDirection === 'asc' ? 'desc' : 'asc')
        return currentField
      }

      setSeguradoraSortDirection('asc')
      return field
    })
  }

  const getSeguradoraSortIndicator = (field: SeguradoraSortField) => {
    if (seguradoraSortBy !== field) {
      return '↕'
    }

    return seguradoraSortDirection === 'asc' ? '↑' : '↓'
  }

  const handleStartEditSeguradora = (item: SeguradoraItem) => {
    setEditingSeguradoraCodigo(item.codigo)
    setSeguradoraCodigo(item.codigo)
    setSeguradoraControle(item.controle)
    setSeguradoraLista(item.descricao)
    setSeguradoraCodigoError('')
    setSeguradoraControleError('')
    setSeguradoraListaError('')
    setSeguradoraStatusTone('idle')
    setSeguradoraStatusMessage(`Alterando registro ${item.codigo}.`)
    setIsSeguradoraFormVisible(true)
  }

  const handleCancelSeguradoraForm = () => {
    resetSeguradoraForm()
    setIsSeguradoraFormVisible(false)
    setSeguradoraStatusTone('idle')
    setSeguradoraStatusMessage('')
  }

  const handleCreateSeguradora = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault()

    const editingCodigo = editingSeguradoraCodigo
    const normalizedCodigo = editingCodigo ?? seguradoraCodigo.trim()
    const normalizedControle = seguradoraControle.trim()
    const normalizedLista = seguradoraLista.trim()
    let hasError = false

    setSeguradoraCodigoError('')
    setSeguradoraControleError('')
    setSeguradoraListaError('')

    if (!normalizedCodigo) {
      setSeguradoraCodigoError('Codigo e obrigatorio.')
      hasError = true
    }

    if (!normalizedControle) {
      setSeguradoraControleError('Controle e obrigatorio.')
      hasError = true
    }

    if (!normalizedLista) {
      setSeguradoraListaError('Descricao e obrigatoria.')
      hasError = true
    }

    if (hasError) {
      setSeguradoraStatusTone('error')
      setSeguradoraStatusMessage('Corrija os campos de seguradora para continuar.')
      return
    }

    setIsSavingSeguradora(true)
    setSeguradoraStatusTone('idle')
    setSeguradoraStatusMessage(editingCodigo ? 'Alterando registro de seguradora...' : 'Gravando registro de seguradora...')

    try {
      const savedItem = editingCodigo
        ? await updateSeguradoraItem(editingCodigo, {
            codigo: normalizedCodigo,
            controle: normalizedControle,
            descricao: normalizedLista,
          })
        : await createSeguradoraItem({
            codigo: normalizedCodigo,
            controle: normalizedControle,
            descricao: normalizedLista,
          })

      void savedItem
      resetSeguradoraForm()
      setIsSeguradoraFormVisible(false)
      setSeguradoraStatusTone('success')
      setSeguradoraStatusMessage(editingCodigo ? 'Registro de seguradora alterado com sucesso.' : 'Registro de seguradora cadastrado com sucesso.')
      await loadSeguradoraItems(editingCodigo ? seguradoraPage : 1)
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Falha ao cadastrar registro de seguradora.'

      setSeguradoraStatusTone('error')
      setSeguradoraStatusMessage(message)
    } finally {
      setIsSavingSeguradora(false)
    }
  }

  const handleDeleteSeguradora = async (item: SeguradoraItem) => {
    const confirmed = window.confirm(`Excluir o registro ${item.codigo} - ${item.descricao}?`)

    if (!confirmed) {
      return
    }

    setIsDeletingSeguradora(true)
    setSeguradoraStatusTone('idle')
    setSeguradoraStatusMessage(`Excluindo registro ${item.codigo}...`)

    try {
      const deletedCodigo = await deleteSeguradoraItem(item.codigo)
      setSeguradoraItems((currentItems) => currentItems.filter((currentItem) => currentItem.codigo !== deletedCodigo))

      if (editingSeguradoraCodigo === item.codigo) {
        resetSeguradoraForm()
        setIsSeguradoraFormVisible(false)
      }

      setSeguradoraStatusTone('success')
      setSeguradoraStatusMessage('Registro de seguradora excluido com sucesso.')
      const nextPage = seguradoraItems.length === 1 && seguradoraPage > 1 ? seguradoraPage - 1 : seguradoraPage
      await loadSeguradoraItems(nextPage)
    } catch (error) {
      const message = error instanceof Error
        ? error.message
        : 'Falha ao excluir registro de seguradora.'

      setSeguradoraStatusTone('error')
      setSeguradoraStatusMessage(message)
    } finally {
      setIsDeletingSeguradora(false)
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
  const canGoToPreviousModalidadePage = modalidadePage > 1
  const canGoToNextModalidadePage = modalidadePage < modalidadeTotalPages
  const canGoToPreviousTitularPage = titularPage > 1
  const canGoToNextTitularPage = titularPage < titularTotalPages
  const canGoToPreviousMarcaModeloPage = marcaModeloPage > 1
  const canGoToNextMarcaModeloPage = marcaModeloPage < marcaModeloTotalPages
  const canGoToPreviousSeguradoraPage = seguradoraPage > 1
  const canGoToNextSeguradoraPage = seguradoraPage < seguradoraTotalPages

  return (
    <main className="dashboard-page">
      <aside className="sidebar-menu" aria-label="Menu principal">
        <div>
          <p className="sidebar-brand">TEG Financ</p>
          <h1 className="sidebar-title">Painel Financeiro</h1>
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
            <li
              className={`menu-item ${activeView === 'modalidade' ? 'menu-item-active' : ''}`}
              onClick={() => setActiveView('modalidade')}
            >
              Modalidade
            </li>
            <li
              className={`menu-item ${activeView === 'titular' ? 'menu-item-active' : ''}`}
              onClick={() => setActiveView('titular')}
            >
              Titular do CRM
            </li>
            <li
              className={`menu-item ${activeView === 'marcaModelo' ? 'menu-item-active' : ''}`}
              onClick={() => setActiveView('marcaModelo')}
            >
              Marca/Modelo
            </li>
            <li
              className={`menu-item ${activeView === 'seguradora' ? 'menu-item-active' : ''}`}
              onClick={() => setActiveView('seguradora')}
            >
              Seguradoras
            </li>
            <li
              className={`menu-item ${activeView === 'troca' ? 'menu-item-active' : ''}`}
              onClick={() => setActiveView('troca')}
            >
              Tipo de Troca
            </li>
            <li
              className={`menu-item ${activeView === 'acesso' ? 'menu-item-active' : ''}`}
              onClick={() => setActiveView('acesso')}
            >
              Controle de acesso
            </li>
            <li
              className={`menu-item ${activeView === 'loginDre' ? 'menu-item-active' : ''}`}
              onClick={() => setActiveView('loginDre')}
            >
              Login x DRE
            </li>
            <li
              className={`menu-item ${activeView === 'condutor' ? 'menu-item-active' : ''}`}
              onClick={() => setActiveView('condutor')}
            >
              Condutor
            </li>
            <li
              className={`menu-item ${activeView === 'monitor' ? 'menu-item-active' : ''}`}
              onClick={() => setActiveView('monitor')}
            >
              Monitor
            </li>
            <li
              className={`menu-item ${activeView === 'credenciada' ? 'menu-item-active' : ''}`}
              onClick={() => setActiveView('credenciada')}
            >
              Credenciada
            </li>
            <li
              className={`menu-item ${activeView === 'veiculo' ? 'menu-item-active' : ''}`}
              onClick={() => setActiveView('veiculo')}
            >
              Veiculo
            </li>
            <li
              className={`menu-item ${activeView === 'ordemServico' ? 'menu-item-active' : ''}`}
              onClick={() => setActiveView('ordemServico')}
            >
              OrdemServico
            </li>
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
        ) : activeView === 'dre' ? (
          <>
            <div className="content-copy">
              <p className="content-kicker">Cadastro administrativo</p>
              <h2 id="content-title">Tabela DRE</h2>
              <p className="content-description">
                Cadastre e consulte os registros da tabela DRE. Os campos codigo e
                descricao sao obrigatorios e nao podem se repetir.
              </p>
            </div>

            <div className="management-layout">
              <div className="management-toolbar">
                <button
                  type="button"
                  className="primary-button dre-insert-button"
                  onClick={handleStartInsertDre}
                  disabled={isSavingDre || isDeletingDre}
                >
                  Inserir registro
                </button>

                <form className="management-filter-form" onSubmit={handleFilterDreSubmit}>
                  <input
                    className="management-filter-input"
                    type="text"
                    placeholder="Filtrar por codigo ou descricao"
                    value={dreSearch}
                    onChange={(event) => setDreSearch(event.target.value)}
                  />
                  <button type="submit" className="secondary-button management-filter-button">
                    Filtrar
                  </button>
                  <button type="button" className="secondary-button management-filter-button" onClick={handleClearDreFilter}>
                    Limpar
                  </button>
                </form>
              </div>

              {isDreFormVisible ? (
                <form className="management-card management-form dre-form" onSubmit={handleCreateDre} noValidate>
                  <h2>{editingDreCodigo ? 'Alterar registro' : 'Novo registro'}</h2>

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

              <div className="management-card management-grid-card dre-list-card">
                <div className="management-grid-header">
                  <h2>Registros cadastrados</h2>
                  <span>
                    {isLoadingDre ? 'Atualizando...' : `${dreTotalItems} item(ns) encontrados`}
                  </span>
                </div>

                <div className="management-grid-wrapper">
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
                    <p className="management-empty-state">Nenhum registro da DRE encontrado.</p>
                  ) : null}
                </div>

                <p className={`status-message status-${dreStatusTone}`} aria-live="polite">
                  {dreStatusMessage}
                </p>

                <div className="management-pagination">
                  <button
                    type="button"
                    className="secondary-button management-pagination-button"
                    onClick={() => setDrePage((currentPage) => currentPage - 1)}
                    disabled={!canGoToPreviousDrePage || isLoadingDre}
                  >
                    Anterior
                  </button>
                  <span className="management-pagination-info">
                    Pagina {drePage} de {dreTotalPages}
                  </span>
                  <button
                    type="button"
                    className="secondary-button management-pagination-button"
                    onClick={() => setDrePage((currentPage) => currentPage + 1)}
                    disabled={!canGoToNextDrePage || isLoadingDre}
                  >
                    Proxima
                  </button>
                </div>
              </div>
            </div>
          </>
        ) : activeView === 'modalidade' ? (
          <>
            <div className="content-copy">
              <p className="content-kicker">Cadastro administrativo</p>
              <h2 id="content-title">Tabela Modalidade</h2>
              <p className="content-description">
                Cadastre e consulte os registros da tabela Modalidade. Os campos codigo e
                descricao sao obrigatorios e nao podem se repetir.
              </p>
            </div>

            <div className="management-layout">
              <div className="management-toolbar">
                <button
                  type="button"
                  className="primary-button dre-insert-button"
                  onClick={handleStartInsertModalidade}
                  disabled={isSavingModalidade || isDeletingModalidade}
                >
                  Inserir registro
                </button>

                <form className="management-filter-form" onSubmit={handleFilterModalidadeSubmit}>
                  <input
                    className="management-filter-input"
                    type="text"
                    placeholder="Filtrar por codigo ou descricao"
                    value={modalidadeSearch}
                    onChange={(event) => setModalidadeSearch(event.target.value)}
                  />
                  <button type="submit" className="secondary-button management-filter-button">
                    Filtrar
                  </button>
                  <button type="button" className="secondary-button management-filter-button" onClick={handleClearModalidadeFilter}>
                    Limpar
                  </button>
                </form>
              </div>

              {isModalidadeFormVisible ? (
                <form className="management-card management-form dre-form" onSubmit={handleCreateModalidade} noValidate>
                  <h2>{editingModalidadeCodigo ? 'Alterar registro' : 'Novo registro'}</h2>

                  <label className="field-group" htmlFor="modalidade-codigo">
                    <span>Codigo</span>
                    <input
                      id="modalidade-codigo"
                      name="codigo"
                      type="text"
                      value={modalidadeCodigo}
                      onChange={(event) => setModalidadeCodigo(event.target.value)}
                      disabled={isSavingModalidade}
                      aria-invalid={Boolean(modalidadeCodigoError)}
                    />
                    {modalidadeCodigoError ? <strong className="field-error">{modalidadeCodigoError}</strong> : null}
                  </label>

                  <label className="field-group" htmlFor="modalidade-descricao">
                    <span>Descricao</span>
                    <input
                      id="modalidade-descricao"
                      name="descricao"
                      type="text"
                      value={modalidadeDescricao}
                      onChange={(event) => setModalidadeDescricao(event.target.value)}
                      disabled={isSavingModalidade}
                      aria-invalid={Boolean(modalidadeDescricaoError)}
                    />
                    {modalidadeDescricaoError ? <strong className="field-error">{modalidadeDescricaoError}</strong> : null}
                  </label>

                  <div className="button-row dre-button-row">
                    <button type="submit" className="primary-button" disabled={isSavingModalidade}>
                      {isSavingModalidade ? 'Salvando...' : editingModalidadeCodigo ? 'Salvar alteracao' : 'Salvar Modalidade'}
                    </button>
                    <button type="button" className="secondary-button" onClick={handleCancelModalidadeForm} disabled={isSavingModalidade}>
                      Cancelar
                    </button>
                  </div>
                </form>
              ) : null}

              <div className="management-card management-grid-card dre-list-card">
                <div className="management-grid-header">
                  <h2>Registros cadastrados</h2>
                  <span>
                    {isLoadingModalidade ? 'Atualizando...' : `${modalidadeTotalItems} item(ns) encontrados`}
                  </span>
                </div>

                <div className="management-grid-wrapper">
                  <table className="dre-table">
                    <thead>
                      <tr>
                        <th>
                          <button type="button" className="dre-sort-button" onClick={() => handleSortModalidade('codigo')}>
                            Codigo <span>{getModalidadeSortIndicator('codigo')}</span>
                          </button>
                        </th>
                        <th>
                          <button type="button" className="dre-sort-button" onClick={() => handleSortModalidade('descricao')}>
                            Descricao <span>{getModalidadeSortIndicator('descricao')}</span>
                          </button>
                        </th>
                        <th className="dre-actions-column">Acoes</th>
                      </tr>
                    </thead>
                    <tbody>
                      {modalidadeItems.map((item) => (
                        <tr key={item.codigo}>
                          <td>{item.codigo}</td>
                          <td>{item.descricao}</td>
                          <td>
                            <div className="dre-row-actions">
                              <button type="button" className="row-action-button row-action-edit" onClick={() => handleStartEditModalidade(item)}>
                                Alterar
                              </button>
                              <button type="button" className="row-action-button row-action-delete" onClick={() => handleDeleteModalidade(item)}>
                                Excluir
                              </button>
                            </div>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>

                  {!isLoadingModalidade && modalidadeItems.length === 0 ? (
                    <p className="management-empty-state">Nenhum registro de modalidade encontrado.</p>
                  ) : null}
                </div>

                <p className={`status-message status-${modalidadeStatusTone}`} aria-live="polite">
                  {modalidadeStatusMessage}
                </p>

                <div className="management-pagination">
                  <button
                    type="button"
                    className="secondary-button management-pagination-button"
                    onClick={() => setModalidadePage((currentPage) => currentPage - 1)}
                    disabled={!canGoToPreviousModalidadePage || isLoadingModalidade}
                  >
                    Anterior
                  </button>
                  <span className="management-pagination-info">
                    Pagina {modalidadePage} de {modalidadeTotalPages}
                  </span>
                  <button
                    type="button"
                    className="secondary-button management-pagination-button"
                    onClick={() => setModalidadePage((currentPage) => currentPage + 1)}
                    disabled={!canGoToNextModalidadePage || isLoadingModalidade}
                  >
                    Proxima
                  </button>
                </div>
              </div>
            </div>
          </>
        ) : activeView === 'titular' ? (
          <>
            <div className="content-copy">
              <p className="content-kicker">Cadastro administrativo</p>
              <h2 id="content-title">Tabela Titular do CRM</h2>
              <p className="content-description">
                Cadastre e consulte os registros de titulares do CRM carregados inicialmente a partir do XML. A tela segue o mesmo layout e funcionalidades da DRE, com filtro, ordenacao, paginacao e CRUD completo.
              </p>
            </div>

            <div className="management-layout">
              <div className="management-toolbar">
                <button
                  type="button"
                  className="primary-button dre-insert-button"
                  onClick={handleStartInsertTitular}
                  disabled={isSavingTitular || isDeletingTitular}
                >
                  Inserir registro
                </button>

                <form className="management-filter-form" onSubmit={handleFilterTitularSubmit}>
                  <input
                    className="management-filter-input"
                    type="text"
                    placeholder="Filtrar por codigo, CNPJ/CPF ou titular do CRM"
                    value={titularSearch}
                    onChange={(event) => setTitularSearch(event.target.value)}
                  />
                  <button type="submit" className="secondary-button management-filter-button">
                    Filtrar
                  </button>
                  <button type="button" className="secondary-button management-filter-button" onClick={handleClearTitularFilter}>
                    Limpar
                  </button>
                </form>
              </div>

              {isTitularFormVisible ? (
                <form className="management-card management-form dre-form" onSubmit={handleCreateTitular} noValidate>
                  <h2>{editingTitularCodigo ? 'Alterar registro' : 'Novo registro'}</h2>

                  <label className="field-group" htmlFor="titular-codigo">
                    <span>Codigo</span>
                    <input
                      id="titular-codigo"
                      name="codigo"
                      type="text"
                      value={titularCodigo}
                      onChange={(event) => setTitularCodigo(event.target.value)}
                      disabled={isSavingTitular || Boolean(editingTitularCodigo)}
                      readOnly={Boolean(editingTitularCodigo)}
                      aria-invalid={Boolean(titularCodigoError)}
                    />
                    {titularCodigoError ? <strong className="field-error">{titularCodigoError}</strong> : null}
                  </label>

                  <label className="field-group" htmlFor="titular-cnpj-cpf">
                    <span>CNPJ/CPF</span>
                    <input
                      id="titular-cnpj-cpf"
                      name="cnpj-cpf"
                      type="text"
                      inputMode="numeric"
                      placeholder="000.000.000-00 ou 00.000.000/0000-00"
                      maxLength={18}
                      value={titularCnpjCpf}
                      onChange={(event) => setTitularCnpjCpf(formatCpfOrCnpj(event.target.value))}
                      disabled={isSavingTitular}
                      aria-invalid={Boolean(titularCnpjCpfError)}
                    />
                    {titularCnpjCpfError ? <strong className="field-error">{titularCnpjCpfError}</strong> : null}
                  </label>

                  <label className="field-group" htmlFor="titular-nome">
                    <span>Titular do CRM</span>
                    <input
                      id="titular-nome"
                      name="titular"
                      type="text"
                      value={titularNome}
                      onChange={(event) => setTitularNome(event.target.value)}
                      disabled={isSavingTitular}
                      aria-invalid={Boolean(titularNomeError)}
                    />
                    {titularNomeError ? <strong className="field-error">{titularNomeError}</strong> : null}
                  </label>

                  <div className="button-row dre-button-row">
                    <button type="submit" className="primary-button" disabled={isSavingTitular}>
                      {isSavingTitular ? 'Salvando...' : editingTitularCodigo ? 'Salvar alteracao' : 'Salvar titular do CRM'}
                    </button>
                    <button type="button" className="secondary-button" onClick={handleCancelTitularForm} disabled={isSavingTitular}>
                      Cancelar
                    </button>
                  </div>
                </form>
              ) : null}

              <div className="management-card management-grid-card dre-list-card">
                <div className="management-grid-header">
                  <h2>Registros cadastrados</h2>
                  <span>
                    {isLoadingTitular ? 'Atualizando...' : `${titularTotalItems} item(ns) encontrados`}
                  </span>
                </div>

                <div className="management-grid-wrapper">
                  <table className="dre-table">
                    <thead>
                      <tr>
                        <th>
                          <button type="button" className="dre-sort-button" onClick={() => handleSortTitular('codigo')}>
                            Codigo <span>{getTitularSortIndicator('codigo')}</span>
                          </button>
                        </th>
                        <th>
                          <button type="button" className="dre-sort-button" onClick={() => handleSortTitular('cnpj_cpf')}>
                            CNPJ/CPF <span>{getTitularSortIndicator('cnpj_cpf')}</span>
                          </button>
                        </th>
                        <th>
                          <button type="button" className="dre-sort-button" onClick={() => handleSortTitular('titular')}>
                            Titular do CRM <span>{getTitularSortIndicator('titular')}</span>
                          </button>
                        </th>
                        <th className="dre-actions-column">Acoes</th>
                      </tr>
                    </thead>
                    <tbody>
                      {titularItems.map((item) => (
                        <tr key={item.codigo}>
                          <td>{item.codigo}</td>
                          <td>{item.cnpj_cpf}</td>
                          <td>{item.titular}</td>
                          <td>
                            <div className="dre-row-actions">
                              <button type="button" className="row-action-button row-action-edit" onClick={() => handleStartEditTitular(item)}>
                                Alterar
                              </button>
                              <button type="button" className="row-action-button row-action-delete" onClick={() => handleDeleteTitular(item)}>
                                Excluir
                              </button>
                            </div>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>

                  {!isLoadingTitular && titularItems.length === 0 ? (
                    <p className="management-empty-state">Nenhum registro de titular do CRM encontrado.</p>
                  ) : null}
                </div>

                <p className={`status-message status-${titularStatusTone}`} aria-live="polite">
                  {titularStatusMessage}
                </p>

                <div className="management-pagination">
                  <button
                    type="button"
                    className="secondary-button management-pagination-button"
                    onClick={() => setTitularPage((currentPage) => currentPage - 1)}
                    disabled={!canGoToPreviousTitularPage || isLoadingTitular}
                  >
                    Anterior
                  </button>
                  <span className="management-pagination-info">
                    Pagina {titularPage} de {titularTotalPages}
                  </span>
                  <button
                    type="button"
                    className="secondary-button management-pagination-button"
                    onClick={() => setTitularPage((currentPage) => currentPage + 1)}
                    disabled={!canGoToNextTitularPage || isLoadingTitular}
                  >
                    Proxima
                  </button>
                </div>
              </div>
            </div>
          </>
        ) : activeView === 'marcaModelo' ? (
          <>
            <div className="content-copy">
              <p className="content-kicker">Cadastro administrativo</p>
              <h2 id="content-title">Tabela Marca/Modelo</h2>
              <p className="content-description">
                Cadastre e consulte os registros da tabela de marca/modelo importada do XML. Os campos codigo e descricao sao obrigatorios e nao podem se repetir.
              </p>
            </div>

            <div className="management-layout">
              <div className="management-toolbar">
                <button
                  type="button"
                  className="primary-button dre-insert-button"
                  onClick={handleStartInsertMarcaModelo}
                  disabled={isSavingMarcaModelo || isDeletingMarcaModelo}
                >
                  Inserir registro
                </button>

                <form className="management-filter-form" onSubmit={handleFilterMarcaModeloSubmit}>
                  <input
                    className="management-filter-input"
                    type="text"
                    placeholder="Filtrar por codigo ou descricao"
                    value={marcaModeloSearch}
                    onChange={(event) => setMarcaModeloSearch(event.target.value)}
                  />
                  <button type="submit" className="secondary-button management-filter-button">
                    Filtrar
                  </button>
                  <button type="button" className="secondary-button management-filter-button" onClick={handleClearMarcaModeloFilter}>
                    Limpar
                  </button>
                </form>
              </div>

              {isMarcaModeloFormVisible ? (
                <form className="management-card management-form dre-form" onSubmit={handleCreateMarcaModelo} noValidate>
                  <h2>{editingMarcaModeloCodigo ? 'Alterar registro' : 'Novo registro'}</h2>

                  <label className="field-group" htmlFor="marca-modelo-codigo">
                    <span>Codigo</span>
                    <input
                      id="marca-modelo-codigo"
                      name="codigo"
                      type="text"
                      value={marcaModeloCodigo}
                      onChange={(event) => setMarcaModeloCodigo(event.target.value)}
                      disabled={isSavingMarcaModelo}
                      aria-invalid={Boolean(marcaModeloCodigoError)}
                    />
                    {marcaModeloCodigoError ? <strong className="field-error">{marcaModeloCodigoError}</strong> : null}
                  </label>

                  <label className="field-group" htmlFor="marca-modelo-descricao">
                    <span>Descricao</span>
                    <input
                      id="marca-modelo-descricao"
                      name="descricao"
                      type="text"
                      value={marcaModeloDescricao}
                      onChange={(event) => setMarcaModeloDescricao(event.target.value)}
                      disabled={isSavingMarcaModelo}
                      aria-invalid={Boolean(marcaModeloDescricaoError)}
                    />
                    {marcaModeloDescricaoError ? <strong className="field-error">{marcaModeloDescricaoError}</strong> : null}
                  </label>

                  <div className="button-row dre-button-row">
                    <button type="submit" className="primary-button" disabled={isSavingMarcaModelo}>
                      {isSavingMarcaModelo ? 'Salvando...' : editingMarcaModeloCodigo ? 'Salvar alteracao' : 'Salvar marca/modelo'}
                    </button>
                    <button type="button" className="secondary-button" onClick={handleCancelMarcaModeloForm} disabled={isSavingMarcaModelo}>
                      Cancelar
                    </button>
                  </div>
                </form>
              ) : null}

              <div className="management-card management-grid-card dre-list-card">
                <div className="management-grid-header">
                  <h2>Registros cadastrados</h2>
                  <span>
                    {isLoadingMarcaModelo ? 'Atualizando...' : `${marcaModeloTotalItems} item(ns) encontrados`}
                  </span>
                </div>

                <div className="management-grid-wrapper">
                  <table className="dre-table">
                    <thead>
                      <tr>
                        <th>
                          <button type="button" className="dre-sort-button" onClick={() => handleSortMarcaModelo('codigo')}>
                            Codigo <span>{getMarcaModeloSortIndicator('codigo')}</span>
                          </button>
                        </th>
                        <th>
                          <button type="button" className="dre-sort-button" onClick={() => handleSortMarcaModelo('descricao')}>
                            Descricao <span>{getMarcaModeloSortIndicator('descricao')}</span>
                          </button>
                        </th>
                        <th className="dre-actions-column">Acoes</th>
                      </tr>
                    </thead>
                    <tbody>
                      {marcaModeloItems.map((item) => (
                        <tr key={item.codigo}>
                          <td>{item.codigo}</td>
                          <td>{item.descricao}</td>
                          <td>
                            <div className="dre-row-actions">
                              <button type="button" className="row-action-button row-action-edit" onClick={() => handleStartEditMarcaModelo(item)}>
                                Alterar
                              </button>
                              <button type="button" className="row-action-button row-action-delete" onClick={() => handleDeleteMarcaModelo(item)}>
                                Excluir
                              </button>
                            </div>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>

                  {!isLoadingMarcaModelo && marcaModeloItems.length === 0 ? (
                    <p className="management-empty-state">Nenhum registro de marca/modelo encontrado.</p>
                  ) : null}
                </div>

                <p className={`status-message status-${marcaModeloStatusTone}`} aria-live="polite">
                  {marcaModeloStatusMessage}
                </p>

                <div className="management-pagination">
                  <button
                    type="button"
                    className="secondary-button management-pagination-button"
                    onClick={() => setMarcaModeloPage((currentPage) => currentPage - 1)}
                    disabled={!canGoToPreviousMarcaModeloPage || isLoadingMarcaModelo}
                  >
                    Anterior
                  </button>
                  <span className="management-pagination-info">
                    Pagina {marcaModeloPage} de {marcaModeloTotalPages}
                  </span>
                  <button
                    type="button"
                    className="secondary-button management-pagination-button"
                    onClick={() => setMarcaModeloPage((currentPage) => currentPage + 1)}
                    disabled={!canGoToNextMarcaModeloPage || isLoadingMarcaModelo}
                  >
                    Proxima
                  </button>
                </div>
              </div>
            </div>
          </>
        ) : activeView === 'seguradora' ? (
          <>
            <div className="content-copy">
              <p className="content-kicker">Cadastro administrativo</p>
              <h2 id="content-title">Tabela Seguradoras</h2>
              <p className="content-description">
                Cadastre e consulte os registros da tabela de seguradoras carregada inicialmente a partir do XML. Os campos codigo, controle e descricao sao obrigatorios, e apenas o codigo e tratado como chave unica.
              </p>
            </div>

            <div className="management-layout">
              <div className="management-toolbar">
                <button
                  type="button"
                  className="primary-button dre-insert-button"
                  onClick={handleStartInsertSeguradora}
                  disabled={isSavingSeguradora || isDeletingSeguradora}
                >
                  Inserir registro
                </button>

                <form className="management-filter-form" onSubmit={handleFilterSeguradoraSubmit}>
                  <input
                    className="management-filter-input"
                    type="text"
                    placeholder="Filtrar por codigo, controle ou descricao"
                    value={seguradoraSearch}
                    onChange={(event) => setSeguradoraSearch(event.target.value)}
                  />
                  <button type="submit" className="secondary-button management-filter-button">
                    Filtrar
                  </button>
                  <button type="button" className="secondary-button management-filter-button" onClick={handleClearSeguradoraFilter}>
                    Limpar
                  </button>
                </form>
              </div>

              {isSeguradoraFormVisible ? (
                <form className="management-card management-form dre-form" onSubmit={handleCreateSeguradora} noValidate>
                  <h2>{editingSeguradoraCodigo ? 'Alterar registro' : 'Novo registro'}</h2>

                  <label className="field-group" htmlFor="seguradora-codigo">
                    <span>Codigo</span>
                    <input
                      id="seguradora-codigo"
                      name="codigo"
                      type="text"
                      value={seguradoraCodigo}
                      onChange={(event) => setSeguradoraCodigo(event.target.value)}
                      disabled={isSavingSeguradora || Boolean(editingSeguradoraCodigo)}
                      readOnly={Boolean(editingSeguradoraCodigo)}
                      aria-invalid={Boolean(seguradoraCodigoError)}
                    />
                    {seguradoraCodigoError ? <strong className="field-error">{seguradoraCodigoError}</strong> : null}
                  </label>

                  <label className="field-group" htmlFor="seguradora-controle">
                    <span>Controle</span>
                    <input
                      id="seguradora-controle"
                      name="controle"
                      type="text"
                      value={seguradoraControle}
                      onChange={(event) => setSeguradoraControle(event.target.value)}
                      disabled={isSavingSeguradora}
                      aria-invalid={Boolean(seguradoraControleError)}
                    />
                    {seguradoraControleError ? <strong className="field-error">{seguradoraControleError}</strong> : null}
                  </label>

                  <label className="field-group" htmlFor="seguradora-lista">
                    <span>Descricao</span>
                    <input
                      id="seguradora-lista"
                      name="lista"
                      type="text"
                      value={seguradoraLista}
                      onChange={(event) => setSeguradoraLista(event.target.value)}
                      disabled={isSavingSeguradora}
                      aria-invalid={Boolean(seguradoraListaError)}
                    />
                    {seguradoraListaError ? <strong className="field-error">{seguradoraListaError}</strong> : null}
                  </label>

                  <div className="button-row dre-button-row">
                    <button type="submit" className="primary-button" disabled={isSavingSeguradora}>
                      {isSavingSeguradora ? 'Salvando...' : editingSeguradoraCodigo ? 'Salvar alteracao' : 'Salvar seguradora'}
                    </button>
                    <button type="button" className="secondary-button" onClick={handleCancelSeguradoraForm} disabled={isSavingSeguradora}>
                      Cancelar
                    </button>
                  </div>
                </form>
              ) : null}

              <div className="management-card management-grid-card dre-list-card">
                <div className="management-grid-header">
                  <h2>Registros cadastrados</h2>
                  <span>
                    {isLoadingSeguradora ? 'Atualizando...' : `${seguradoraTotalItems} item(ns) encontrados`}
                  </span>
                </div>

                <div className="management-grid-wrapper">
                  <table className="dre-table">
                    <thead>
                      <tr>
                        <th>
                          <button type="button" className="dre-sort-button" onClick={() => handleSortSeguradora('codigo')}>
                            Codigo <span>{getSeguradoraSortIndicator('codigo')}</span>
                          </button>
                        </th>
                        <th>
                          <button type="button" className="dre-sort-button" onClick={() => handleSortSeguradora('controle')}>
                            Controle <span>{getSeguradoraSortIndicator('controle')}</span>
                          </button>
                        </th>
                        <th>
                          <button type="button" className="dre-sort-button" onClick={() => handleSortSeguradora('descricao')}>
                            Descricao <span>{getSeguradoraSortIndicator('descricao')}</span>
                          </button>
                        </th>
                        <th className="dre-actions-column">Acoes</th>
                      </tr>
                    </thead>
                    <tbody>
                      {seguradoraItems.map((item) => (
                        <tr key={item.codigo}>
                          <td>{item.codigo}</td>
                          <td>{item.controle}</td>
                          <td>{item.descricao}</td>
                          <td>
                            <div className="dre-row-actions">
                              <button type="button" className="row-action-button row-action-edit" onClick={() => handleStartEditSeguradora(item)}>
                                Alterar
                              </button>
                              <button type="button" className="row-action-button row-action-delete" onClick={() => handleDeleteSeguradora(item)}>
                                Excluir
                              </button>
                            </div>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>

                  {!isLoadingSeguradora && seguradoraItems.length === 0 ? (
                    <p className="management-empty-state">Nenhum registro de seguradora encontrado.</p>
                  ) : null}
                </div>

                <p className={`status-message status-${seguradoraStatusTone}`} aria-live="polite">
                  {seguradoraStatusMessage}
                </p>

                <div className="management-pagination">
                  <button
                    type="button"
                    className="secondary-button management-pagination-button"
                    onClick={() => setSeguradoraPage((currentPage) => currentPage - 1)}
                    disabled={!canGoToPreviousSeguradoraPage || isLoadingSeguradora}
                  >
                    Anterior
                  </button>
                  <span className="management-pagination-info">
                    Pagina {seguradoraPage} de {seguradoraTotalPages}
                  </span>
                  <button
                    type="button"
                    className="secondary-button management-pagination-button"
                    onClick={() => setSeguradoraPage((currentPage) => currentPage + 1)}
                    disabled={!canGoToNextSeguradoraPage || isLoadingSeguradora}
                  >
                    Proxima
                  </button>
                </div>
              </div>
            </div>
          </>
        ) : activeView === 'troca' ? (
          <>
            <div className="content-copy">
              <p className="content-kicker">Cadastro operacional</p>
              <h2 id="content-title">Tabela Tipo de Troca</h2>
              <p className="content-description">
                Consulte, inclua, altere e exclua os tipos de troca carregados inicialmente a partir do XML operacional.
              </p>
            </div>

            <div className="access-embed-card">
              <iframe
                className="access-embed-frame"
                src="/src/troca.html"
                title="Cadastro de tipo de troca"
              />
            </div>
          </>
        ) : activeView === 'acesso' ? (
          <>
            <div className="content-copy">
              <p className="content-kicker">Seguranca administrativa</p>
              <h2 id="content-title">Controle de acesso</h2>
              <p className="content-description">
                Acesse o grid de cadastro, consulta, alteracao e exclusao de usuarios
                diretamente pelo menu lateral da tela administrativa.
              </p>
            </div>

            <div className="access-embed-card">
              <iframe
                className="access-embed-frame"
                src="/src/cadastroAcesso.html"
                title="Controle de acesso"
              />
            </div>
          </>
        ) : activeView === 'loginDre' ? (
          <>
            <div className="content-copy">
              <p className="content-kicker">Relacionamento administrativo</p>
              <h2 id="content-title">Login x DRE</h2>
              <p className="content-description">
                Consulte e mantenha os relacionamentos entre usuarios e DRE com selecao por codigo.
              </p>
            </div>

            <div className="access-embed-card">
              <iframe
                className="access-embed-frame"
                src="/src/loginDre.html"
                title="Login x DRE"
              />
            </div>
          </>
        ) : activeView === 'condutor' ? (
          <>
            <div className="content-copy">
              <p className="content-kicker">Cadastro operacional</p>
              <h2 id="content-title">Tabela Condutor</h2>
              <p className="content-description">
                Consulte, inclua e altere os registros de condutores no mesmo padrao do controle de acesso.
              </p>
            </div>

            <div className="access-embed-card">
              <iframe
                className="access-embed-frame"
                src="/src/condutor.html"
                title="Cadastro de condutor"
              />
            </div>
          </>
        ) : activeView === 'monitor' ? (
          <>
            <div className="content-copy">
              <p className="content-kicker">Cadastro operacional</p>
              <h2 id="content-title">Tabela Monitor</h2>
              <p className="content-description">
                Consulte, inclua, altere e importe os registros de monitores no mesmo padrao operacional da tela de condutor.
              </p>
            </div>

            <div className="access-embed-card">
              <iframe
                className="access-embed-frame"
                src="/src/monitor.html"
                title="Cadastro de monitor"
              />
            </div>
          </>
        ) : activeView === 'veiculo' ? (
          <>
            <div className="content-copy">
              <p className="content-kicker">Cadastro operacional</p>
              <h2 id="content-title">Tabela Veiculo</h2>
              <p className="content-description">
                Consulte, inclua, altere e importe os registros de veiculos a partir do XML no mesmo padrao operacional da tela de monitor.
              </p>
            </div>

            <div className="access-embed-card">
              <iframe
                className="access-embed-frame"
                src="/src/veiculo.html"
                title="Cadastro de veiculo"
              />
            </div>
          </>
        ) : activeView === 'ordemServico' ? (
          <>
            <div className="content-copy">
              <p className="content-kicker">Cadastro operacional</p>
              <h2 id="content-title">OrdemServico</h2>
              <p className="content-description">
                Consulte, inclua, altere e importe Ordens de Servico com busca relacional de credenciada, DRE, condutor, preposto, veiculo, monitor e tipo de troca.
              </p>
            </div>

            <div className="access-embed-card">
              <iframe
                className="access-embed-frame"
                src="/src/ordemServico.html"
                title="OrdemServico"
              />
            </div>
          </>
        ) : (
          <>
            <div className="content-copy">
              <p className="content-kicker">Cadastro operacional</p>
              <h2 id="content-title">Tabela Credenciada</h2>
              <p className="content-description">
                Consulte, inclua, altere e importe os registros de credenciadas a partir do XML no mesmo padrao operacional da tela de condutor.
              </p>
            </div>

            <div className="access-embed-card">
              <iframe
                className="access-embed-frame"
                src="/src/credenciada.html"
                title="Cadastro de credenciada"
              />
            </div>
          </>
        )}
      </section>
    </main>
  )
}

export default App
