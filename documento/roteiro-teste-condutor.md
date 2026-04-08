# Checklist De Homologacao Da Tela Condutor

## Preparacao

- [ ] Executar `npm run api`.
- [ ] Executar `npm run dev -- --host 0.0.0.0`.
- [ ] Abrir `http://localhost:5173/`, autenticar e acessar a tela `Condutor`.

## Checklist Essencial

- [ ] Confirmar que o grid abre com total de registros e paginacao ativa.
- [ ] Validar ordenacao por `Condutor` em ordem ascendente e descendente.
- [ ] Filtrar por `9241`, confirmar um unico resultado e limpar o filtro.
- [ ] Alterar o registro importado `9241`, salvar e confirmar a persistencia no grid.
- [ ] Excluir o registro importado `9241` e confirmar que ele nao aparece mais na busca.
- [ ] Reimportar `Condutor.xml` e confirmar restauracao do registro `9241`.
- [ ] Importar `Condutor-invalid.xml` e confirmar abertura do painel `Registros nao importados`.
- [ ] Confirmar `2` recusas esperadas: codigo invalido na linha `2` e CPF invalido na linha `3`.

## Resultado Esperado Atual

- [ ] Importacao valida concluida com restauracao do registro excluido quando aplicavel e com as recusas ja existentes no `Condutor.xml` refletidas pela API.
- [ ] Importacao invalida com `1` processado e `2` recusas.

## Arquivos De Apoio

- XML valido: `importXML/Condutor.xml`
- XML invalido: `importXML/Condutor-invalid.xml`