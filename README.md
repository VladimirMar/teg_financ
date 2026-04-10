## OrdemServico

Validar o XML offline antes do POST:

```bash
npm run validate:ordem-servico:xml
```

O script lê `importXML/OrdemServico.xml` e grava o resumo em `importXML/ordem_servico_validation_summary.json`.

Para incluir checagem opcional de integridade referencial com PostgreSQL antes do POST:

```bash
CHECK_DB_REFERENCES=true npm run validate:ordem-servico:xml
```

Nesse modo, o validador também antecipa ausências em `credenciada`, `dre`, `condutor`, `veiculo` e `monitor`.

Importar com saída em arquivo, sem depender da janela interativa:

```bash
npm run import:ordem-servico
```

O script chama `POST /api/ordem-servico/import-xml` e grava o retorno completo em `importXML/ordem_servico_import_summary.json`.

Importar em background com log incremental local:

```bash
npm run import:ordem-servico:background
```

Esse runner grava o resumo JSON em `importXML/ordem_servico_import_summary.json` e escreve batidas de andamento em `importXML/ordem_servico_import.log` enquanto aguarda a resposta do endpoint.

Pipeline unico de validacao + importacao:

```bash
npm run pipeline:ordem-servico
```

O pipeline roda a validacao primeiro e so dispara a importacao quando o XML passa. O consolidado final fica em `importXML/ordem_servico_pipeline_summary.json`.

Para aceitar pendencias referenciais parciais ate um limite configuravel, mantendo bloqueio para erro estrutural:

```bash
CHECK_DB_REFERENCES=true PIPELINE_MAX_REFERENCE_ERRORS=10 npm run pipeline:ordem-servico
```

Nesse modo, o pipeline continua quando `structuralValid=true` e `referenceErrorCount <= PIPELINE_MAX_REFERENCE_ERRORS`.

Variáveis opcionais:

```bash
API_BASE_URL=http://localhost:3001
ORDEM_SERVICO_XML_FILE=OrdemServico.xml
ORDEM_SERVICO_VALIDATION_REPORT_PATH=importXML/ordem_servico_validation_summary.json
ORDEM_SERVICO_IMPORT_REPORT_PATH=importXML/ordem_servico_import_summary.json
ORDEM_SERVICO_IMPORT_LOG_PATH=importXML/ordem_servico_import.log
ORDEM_SERVICO_IMPORT_HEARTBEAT_SECONDS=15
ORDEM_SERVICO_PIPELINE_REPORT_PATH=importXML/ordem_servico_pipeline_summary.json
CHECK_DB_REFERENCES=true
PIPELINE_MAX_REFERENCE_ERRORS=0
```
# TEG Financ

Aplicacao React + Vite com API Node para operacoes administrativas, CRUD e importacao XML.

## Comandos

- `npm run api`: sobe a API em `http://localhost:3001`.
- `npm run dev -- --host 0.0.0.0`: sobe o frontend Vite.
- `npm run build`: gera o build de producao.
- `npm run lint`: executa o lint.
- `npm run smoke:api`: executa o smoke automatizado completo da API para `Condutor`, `Credenciada`, `Veiculo` e `Marca/Modelo`.
- `npm run smoke:api:condutor`: executa apenas a suite de `Condutor`.
- `npm run smoke:api:credenciada`: executa apenas a suite de `Credenciada`.
- `npm run smoke:api:veiculo`: executa apenas a suite de `Veiculo`.
- `npm run smoke:api:marca-modelo`: executa apenas a suite de `Marca/Modelo`.

## Smoke Test

O smoke valida os fluxos principais das APIs de `Condutor`, `Credenciada`, `Veiculo` e `Marca/Modelo`:

- listagem e ordenacao
- edicao e exclusao de registro importado
- restauracao por reimportacao do XML valido
- importacao invalida e consulta das recusas

No VS Code, as tasks disponiveis sao:

- `smoke api`
- `smoke api condutor`
- `smoke api credenciada`

Para `Veiculo` e `Marca/Modelo`, a execucao pode ser feita pelos scripts `npm run smoke:api:veiculo` e `npm run smoke:api:marca-modelo`.

## CI

O workflow de GitHub Actions executa as suites de smoke da API com Postgres em jobs separados para `Condutor` e `Credenciada`.

Ele tambem aceita execucao manual por `workflow_dispatch`, com selecao de `all`, `condutor`, `credenciada`, `veiculo` ou `marca-modelo`.

Quando uma suite falha, o workflow publica um artifact com:

- log da API
- log completo do smoke
- relatorio JSON com os logs de importacao
- resumo em Markdown da falha
