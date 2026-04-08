# Checklist De Homologacao Da Tela Credenciada

## Preparacao

- [ ] Executar `npm run api`.
- [ ] Executar `npm run dev -- --host 0.0.0.0`.
- [ ] Abrir `http://localhost:5173/`, autenticar e acessar a tela `Credenciada`.

## Checklist Essencial

- [ ] Confirmar que o grid abre com total de registros e paginacao ativa.
- [ ] Validar ordenacao por `Credenciada` em ordem ascendente e descendente.
- [ ] Filtrar por `2277`, confirmar um unico resultado e limpar o filtro.
- [ ] Alterar o registro importado `2277`, salvar e confirmar a persistencia no grid.
- [ ] Excluir o registro importado `2277` e confirmar que ele nao aparece mais na busca.
- [ ] Reimportar `Credenciados.xml` e confirmar restauracao do registro `2277`.
- [ ] Importar `Credenciados-invalid.xml` e confirmar abertura do painel `Registros nao importados`.
- [ ] Confirmar `2` recusas esperadas: codigo invalido na linha `2` e email invalido na linha `3`.

## Resultado Esperado Atual

- [ ] Importacao valida com `2938` processados, `2937` atualizados ou equivalentes, `1` inserido quando houver restauracao de registro excluido, e `0` recusas.
- [ ] Importacao invalida com `1` processado e `2` recusas.

## Arquivos De Apoio

- XML valido: `importXML/Credenciados.xml`
- XML invalido: `importXML/Credenciados-invalid.xml`