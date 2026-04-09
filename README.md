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
