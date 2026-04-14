const pg = require('pg');

const api = 'http://localhost:3001';
const { Pool } = pg;
const pool = new Pool({
  host: 'localhost',
  port: 5432,
  user: 'postgres',
  password: '12345',
  database: 'teg_financ',
});

const today = new Date();
const formatDate = (d) => d.toISOString().slice(0, 10);
const addDays = (n) => {
  const d = new Date(today);
  d.setDate(d.getDate() + n);
  return formatDate(d);
};

(async () => {
  const osResp = await fetch(`${api}/api/ordem-servico?page=1&pageSize=200`);
  const osJson = await osResp.json();
  const base = (osJson.items || []).find((item) => item.cpf_condutor && item.cpf_monitor && item.crm && item.cnpj_cpf && item.dre_codigo);

  const condResp = await fetch(`${api}/api/condutor?page=1&pageSize=50&sortBy=condutor&sortDirection=asc`);
  const condJson = await condResp.json();
  const novoCondutor = (condJson.items || []).find((item) => item.cpf_condutor && item.cpf_condutor !== base?.cpf_condutor);

  if (!base || !novoCondutor) {
    throw new Error('Base de teste não encontrada.');
  }

  const termo = `2026/${String(Date.now()).slice(-7)}`;
  const revisao = '-S/R';
  const payloadBase = {
    codigoAccess: '',
    termoAdesao: termo,
    numOs: '001',
    revisao,
    vigenciaOs: addDays(1),
    credenciado: base.credenciado,
    cnpjCpf: base.cnpj_cpf,
    dreCodigo: base.dre_codigo,
    modalidadeDescricao: base.modalidade_descricao,
    cpfCondutor: base.cpf_condutor,
    dataAdmissaoCondutor: addDays(-5),
    cpfPreposto: '',
    prepostoInicio: '',
    prepostoDias: '',
    crm: base.crm,
    cpfMonitor: base.cpf_monitor,
    dataAdmissaoMonitor: addDays(-5),
    situacao: 'Ativo',
    tipoTroca: '',
    dataEncerramento: '',
    anotacao: 'TESTE VINCULO CONDUTOR',
    uniaoTermos: '',
  };

  let createdCodigo = null;

  try {
    const createResp = await fetch(`${api}/api/ordem-servico`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
      body: JSON.stringify(payloadBase),
    });
    const createJson = await createResp.json();
    if (!createResp.ok) {
      throw new Error(`create: ${JSON.stringify(createJson)}`);
    }
    createdCodigo = createJson.item.codigo;

    const q1 = await pool.query(
      "select termo_adesao, num_os, revisao, credenciada_codigo, to_char(data_admissao_condutor,'YYYY-MM-DD') as data_admissao_condutor, condutor_codigo from vinculo_condutor where termo_adesao = $1 and num_os = $2 and revisao = $3",
      [termo, '001', revisao],
    );

    const updateCondutorPayload = {
      ...payloadBase,
      codigo: createdCodigo,
      cpfCondutor: novoCondutor.cpf_condutor,
      dataAdmissaoCondutor: addDays(-4),
    };

    const updateCondutorResp = await fetch(`${api}/api/ordem-servico/${createdCodigo}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
      body: JSON.stringify(updateCondutorPayload),
    });
    const updateCondutorJson = await updateCondutorResp.json();
    if (!updateCondutorResp.ok) {
      throw new Error(`update-condutor: ${JSON.stringify(updateCondutorJson)}`);
    }

    const q2 = await pool.query(
      "select termo_adesao, num_os, revisao, credenciada_codigo, to_char(data_admissao_condutor,'YYYY-MM-DD') as data_admissao_condutor, condutor_codigo from vinculo_condutor where termo_adesao = $1 and num_os = $2 and revisao = $3",
      [termo, '001', revisao],
    );

    const updateDataPayload = {
      ...updateCondutorPayload,
      dataAdmissaoCondutor: addDays(-2),
    };

    const updateDataResp = await fetch(`${api}/api/ordem-servico/${createdCodigo}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
      body: JSON.stringify(updateDataPayload),
    });
    const updateDataJson = await updateDataResp.json();
    if (!updateDataResp.ok) {
      throw new Error(`update-data: ${JSON.stringify(updateDataJson)}`);
    }

    const q3 = await pool.query(
      "select termo_adesao, num_os, revisao, credenciada_codigo, to_char(data_admissao_condutor,'YYYY-MM-DD') as data_admissao_condutor, condutor_codigo from vinculo_condutor where termo_adesao = $1 and num_os = $2 and revisao = $3",
      [termo, '001', revisao],
    );

    console.log(JSON.stringify({
      aposCriar: q1.rows,
      aposAlterarCondutor: q2.rows,
      aposAlterarSomenteData: q3.rows,
      condutorOriginalCpf: base.cpf_condutor,
      novoCondutorCpf: novoCondutor.cpf_condutor,
      dataEsperadaFinal: addDays(-2),
    }, null, 2));
  } finally {
    if (createdCodigo) {
      await fetch(`${api}/api/ordem-servico/${createdCodigo}`, {
        method: 'DELETE',
        headers: { Accept: 'application/json' },
      });
    }
    await pool.end();
  }
})().catch(async (error) => {
  console.error(error.message);
  try { await pool.end(); } catch {}
  process.exit(1);
});
