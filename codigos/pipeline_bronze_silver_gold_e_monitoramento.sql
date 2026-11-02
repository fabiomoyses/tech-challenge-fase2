-- ============================================================================
-- 1. ingestao bronze: extracao dos dados brutos oficiais para o cloud storage
-- ============================================================================

EXPORT DATA OPTIONS(
  uri='gs://bronze-alfabetizacao-fabio-2026/batch_dados_oficiais/data_ingestao=2026-10-30/alunos_*.csv',
  format='CSV', header=true, overwrite=true
) AS SELECT * FROM `basedosdados.br_inep_avaliacao_alfabetizacao.alunos`;

EXPORT DATA OPTIONS(
  uri='gs://bronze-alfabetizacao-fabio-2026/batch_dados_oficiais/data_ingestao=2026-10-30/municipio_*.csv',
  format='CSV', header=true, overwrite=true
) AS SELECT * FROM `basedosdados.br_inep_avaliacao_alfabetizacao.municipio`;

EXPORT DATA OPTIONS(
  uri='gs://bronze-alfabetizacao-fabio-2026/batch_dados_oficiais/data_ingestao=2026-10-30/uf_*.csv',
  format='CSV', header=true, overwrite=true
) AS SELECT * FROM `basedosdados.br_inep_avaliacao_alfabetizacao.uf`;

EXPORT DATA OPTIONS(
  uri='gs://bronze-alfabetizacao-fabio-2026/batch_dados_oficiais/data_ingestao=2026-10-30/meta_alfabetizacao_brasil_*.csv',
  format='CSV', header=true, overwrite=true
) AS SELECT * FROM `basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_brasil`;

EXPORT DATA OPTIONS(
  uri='gs://bronze-alfabetizacao-fabio-2026/batch_dados_oficiais/data_ingestao=2026-10-30/meta_alfabetizacao_uf_*.csv',
  format='CSV', header=true, overwrite=true
) AS SELECT * FROM `basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_uf`;

EXPORT DATA OPTIONS(
  uri='gs://bronze-alfabetizacao-fabio-2026/batch_dados_oficiais/data_ingestao=2026-10-30/dicionario_*.csv',
  format='CSV', header=true, overwrite=true
) AS SELECT * FROM `basedosdados.br_inep_avaliacao_alfabetizacao.dicionario`;


-- ============================================================================
-- 2. camada silver (batch): carga dos arquivos do storage para o bigquery
-- ============================================================================

LOAD DATA OVERWRITE `centering-brook-513216-a6.silver_alfabetizacao.alunos_limpo`
FROM FILES (
  format = 'CSV',
  uris = ['gs://bronze-alfabetizacao-fabio-2026/batch_dados_oficiais/data_ingestao=2026-10-30/alunos_*.csv']
);

LOAD DATA OVERWRITE `centering-brook-513216-a6.silver_alfabetizacao.municipio_limpo`
FROM FILES (
  format = 'CSV',
  uris = ['gs://bronze-alfabetizacao-fabio-2026/batch_dados_oficiais/data_ingestao=2026-10-30/municipio_*.csv']
);

LOAD DATA OVERWRITE `centering-brook-513216-a6.silver_alfabetizacao.uf_limpo`
FROM FILES (
  format = 'CSV',
  uris = ['gs://bronze-alfabetizacao-fabio-2026/batch_dados_oficiais/data_ingestao=2026-10-30/uf_*.csv']
);

LOAD DATA OVERWRITE `centering-brook-513216-a6.silver_alfabetizacao.meta_alfabetizacao_brasil_limpo`
FROM FILES (
  format = 'CSV',
  uris = ['gs://bronze-alfabetizacao-fabio-2026/batch_dados_oficiais/data_ingestao=2026-10-30/meta_alfabetizacao_brasil_*.csv']
);

LOAD DATA OVERWRITE `centering-brook-513216-a6.silver_alfabetizacao.meta_alfabetizacao_uf_limpo`
FROM FILES (
  format = 'CSV',
  uris = ['gs://bronze-alfabetizacao-fabio-2026/batch_dados_oficiais/data_ingestao=2026-10-30/meta_alfabetizacao_uf_*.csv']
);

LOAD DATA OVERWRITE `centering-brook-513216-a6.silver_alfabetizacao.dicionario_limpo`
FROM FILES (
  format = 'CSV',
  uris = ['gs://bronze-alfabetizacao-fabio-2026/batch_dados_oficiais/data_ingestao=2026-10-30/dicionario_*.csv']
);

LOAD DATA OVERWRITE `centering-brook-513216-a6.silver_alfabetizacao.meta_alfabetizacao_municipio_limpo`
FROM FILES (
  format = 'CSV',
  uris = ['gs://bronze-alfabetizacao-fabio-2026/batch_dados_oficiais/data_ingestao=2026-10-30/meta_alfabetizacao_municipio.csv'],
  skip_leading_rows = 1,
  field_delimiter = ','
);


-- ============================================================================
-- 3. camada silver (streaming e controle): eventos simulados e auditoria
-- ============================================================================

CREATE OR REPLACE TABLE `centering-brook-513216-a6.silver_alfabetizacao.streaming_avaliacoes_simuladas` (
    ano INT64,
    id_municipio STRING,
    taxa_alfabetizacao FLOAT64,
    data_evento DATE,
    origem STRING,
    timestamp_producao_evento TIMESTAMP,
    timestamp_ingestao_gcp TIMESTAMP
);

INSERT INTO `centering-brook-513216-a6.silver_alfabetizacao.streaming_avaliacoes_simuladas`
(ano, id_municipio, taxa_alfabetizacao, data_evento, origem, timestamp_producao_evento, timestamp_ingestao_gcp)
VALUES
(2026, '3550308', 95.5, CURRENT_DATE(), 'SIMULACAO_STREAMING_ESCOLAS', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 45 SECOND), CURRENT_TIMESTAMP()),
(2026, '3304557', 88.2, CURRENT_DATE(), 'SIMULACAO_STREAMING_ESCOLAS', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 120 SECOND), CURRENT_TIMESTAMP());

CREATE TABLE IF NOT EXISTS `centering-brook-513216-a6.silver_alfabetizacao.log_qualidade` (
    data_execucao TIMESTAMP,
    nome_tabela STRING,
    tipo_regra STRING,
    status STRING,
    mensagem STRING
);

-- recria a tabela de metricas com o schema correto
DROP TABLE IF EXISTS `centering-brook-513216-a6.silver_alfabetizacao.metricas_execucao_pipeline`;

CREATE TABLE `centering-brook-513216-a6.silver_alfabetizacao.metricas_execucao_pipeline` (
    data_hora_execucao TIMESTAMP,
    etapa_pipeline STRING,
    linhas_entrada INT64,
    linhas_saida INT64,
    linhas_descartadas INT64,
    latencia_media_segundos FLOAT64
);


-- ============================================================================
-- 4. camada gold: modelagem analitica, validacao cruzada e uniao hibrida
-- ============================================================================

CREATE OR REPLACE TABLE `centering-brook-513216-a6.gold_alfabetizacao.validacao_cruzada_alunos` AS
SELECT 
    a.ano,
    a.id_municipio,
    a.rede,
    SUM(CASE WHEN a.alfabetizado = 1 THEN a.peso_aluno ELSE 0 END) / NULLIF(SUM(a.peso_aluno), 0) AS taxa_recalculada,
    m.taxa_alfabetizacao AS taxa_oficial
FROM `centering-brook-513216-a6.silver_alfabetizacao.alunos_limpo` a
LEFT JOIN `centering-brook-513216-a6.silver_alfabetizacao.municipio_limpo` m
    ON a.ano = m.ano 
    AND a.id_municipio = m.id_municipio 
    AND a.rede = m.rede
WHERE a.peso_aluno IS NOT NULL
GROUP BY 
    a.ano, 
    a.id_municipio, 
    a.rede,
    m.taxa_alfabetizacao;

CREATE OR REPLACE TABLE `centering-brook-513216-a6.gold_alfabetizacao.analise_metas_uf_brasil` AS
SELECT 
    u.ano,
    u.sigla_uf,
    u.rede,
    u.taxa_alfabetizacao AS taxa_uf_realizada,
    m_uf.meta_alfabetizacao_2030 AS meta_estadual_2030,
    m_br.meta_alfabetizacao_2030 AS meta_nacional_2030
FROM `centering-brook-513216-a6.silver_alfabetizacao.uf_limpo` u
LEFT JOIN `centering-brook-513216-a6.silver_alfabetizacao.meta_alfabetizacao_uf_limpo` m_uf
    ON u.sigla_uf = m_uf.sigla_uf
CROSS JOIN `centering-brook-513216-a6.silver_alfabetizacao.meta_alfabetizacao_brasil_limpo` m_br;

-- tabela gold principal com validacao bloqueante antes da publicacao
CREATE OR REPLACE TABLE `centering-brook-513216-a6.gold_alfabetizacao.analise_consolidada_municipios_tmp` AS
WITH mapa_uf AS (
  SELECT * FROM UNNEST([
    STRUCT(11 AS cod_uf, 'RO' AS sigla_uf),
    STRUCT(12, 'AC'), STRUCT(13, 'AM'), STRUCT(14, 'RR'), STRUCT(15, 'PA'),
    STRUCT(16, 'AP'), STRUCT(17, 'TO'), STRUCT(21, 'MA'), STRUCT(22, 'PI'),
    STRUCT(23, 'CE'), STRUCT(24, 'RN'), STRUCT(25, 'PB'), STRUCT(26, 'PE'),
    STRUCT(27, 'AL'), STRUCT(28, 'SE'), STRUCT(29, 'BA'), STRUCT(31, 'MG'),
    STRUCT(32, 'ES'), STRUCT(33, 'RJ'), STRUCT(35, 'SP'), STRUCT(41, 'PR'),
    STRUCT(42, 'SC'), STRUCT(43, 'RS'), STRUCT(50, 'MS'), STRUCT(51, 'MT'),
    STRUCT(52, 'GO'), STRUCT(53, 'DF')
  ])
)
SELECT 
    m.ano,
    m.id_municipio,
    uf.sigla_uf,
    m.serie,
    m.rede,
    d.valor AS rede_descricao,
    m.taxa_alfabetizacao AS taxa_alfabetizacao_realizada,
    mu.meta_alfabetizacao_2030 AS meta_2030,
    CASE 
        WHEN mu.meta_alfabetizacao_2030 IS NULL THEN 'Sem Meta'
        WHEN m.taxa_alfabetizacao >= mu.meta_alfabetizacao_2030 THEN 'Atingiu'
        ELSE 'Nao Atingiu'
    END AS status_meta
FROM `centering-brook-513216-a6.silver_alfabetizacao.municipio_limpo` m
LEFT JOIN mapa_uf uf
    ON CAST(LEFT(CAST(m.id_municipio AS STRING), 2) AS INT64) = uf.cod_uf
LEFT JOIN `centering-brook-513216-a6.silver_alfabetizacao.meta_alfabetizacao_municipio_limpo` mu
    ON m.id_municipio = mu.id_municipio
LEFT JOIN `centering-brook-513216-a6.silver_alfabetizacao.dicionario_limpo` d
    ON d.id_tabela = 'municipio'
    AND d.nome_coluna = 'rede'
    AND m.rede = SAFE_CAST(d.chave AS INT64)
;

INSERT INTO `centering-brook-513216-a6.silver_alfabetizacao.log_qualidade`
VALUES (CURRENT_TIMESTAMP(), 'analise_consolidada_municipios', 'Bloqueante', 'INICIANDO', 'Verificando taxa de correspondencia de JOIN');

ASSERT (
    (
        SELECT SAFE_DIVIDE(COUNT(meta_2030), COUNT(id_municipio))
        FROM `centering-brook-513216-a6.gold_alfabetizacao.analise_consolidada_municipios_tmp`
    ) >= 0.80
) AS 'FALHA DE QUALIDADE BLOQUEANTE: A taxa de correspondencia do JOIN entre municipios e metas ficou abaixo do limiar de 80%. Verifique os dados de origem!';

INSERT INTO `centering-brook-513216-a6.silver_alfabetizacao.log_qualidade`
VALUES (CURRENT_TIMESTAMP(), 'analise_consolidada_municipios', 'Bloqueante', 'SUCESSO', 'Taxa de correspondencia de JOINs superior ao limiar de 80%.');

CREATE OR REPLACE TABLE `centering-brook-513216-a6.gold_alfabetizacao.analise_consolidada_municipios` AS
SELECT * FROM `centering-brook-513216-a6.gold_alfabetizacao.analise_consolidada_municipios_tmp`;

DROP TABLE `centering-brook-513216-a6.gold_alfabetizacao.analise_consolidada_municipios_tmp`;

-- tabela gold hibrida
CREATE OR REPLACE TABLE `centering-brook-513216-a6.gold_alfabetizacao.analise_hibrida_batch_streaming` AS
WITH batch_oficial AS (
    SELECT 
        ano,
        CAST(id_municipio AS STRING) AS id_municipio,
        taxa_alfabetizacao_realizada AS taxa_alfabetizacao,
        'OFICIAL_INEP_BATCH' AS origem
    FROM `centering-brook-513216-a6.gold_alfabetizacao.analise_consolidada_municipios`
),
streaming_tratado AS (
    SELECT 
        ano,
        id_municipio,
        taxa_alfabetizacao,
        origem
    FROM `centering-brook-513216-a6.silver_alfabetizacao.streaming_avaliacoes_simuladas`
    WHERE taxa_alfabetizacao IS NOT NULL 
    QUALIFY ROW_NUMBER() OVER(PARTITION BY id_municipio, ano ORDER BY data_evento DESC) = 1 
)
SELECT ano, id_municipio, taxa_alfabetizacao, origem FROM batch_oficial
UNION ALL
SELECT ano, id_municipio, taxa_alfabetizacao, origem FROM streaming_tratado;


-- ============================================================================
-- 5. observabilidade e finops: metricas e view do information_schema
-- ============================================================================

INSERT INTO `centering-brook-513216-a6.silver_alfabetizacao.metricas_execucao_pipeline`
SELECT
    CURRENT_TIMESTAMP() AS data_hora_execucao,
    'Consolidacao_Streaming_Gold' AS etapa_pipeline,
    (SELECT COUNT(*) FROM `centering-brook-513216-a6.silver_alfabetizacao.streaming_avaliacoes_simuladas`) AS linhas_entrada,
    (SELECT COUNT(*) FROM `centering-brook-513216-a6.gold_alfabetizacao.analise_hibrida_batch_streaming` WHERE origem = 'SIMULACAO_STREAMING_ESCOLAS') AS linhas_saida,
    (SELECT COUNT(*) FROM `centering-brook-513216-a6.silver_alfabetizacao.streaming_avaliacoes_simuladas` WHERE taxa_alfabetizacao IS NULL) AS linhas_descartadas,
    (SELECT AVG(TIMESTAMP_DIFF(timestamp_ingestao_gcp, timestamp_producao_evento, SECOND)) FROM `centering-brook-513216-a6.silver_alfabetizacao.streaming_avaliacoes_simuladas`) AS latencia_media_segundos;

CREATE OR REPLACE VIEW `centering-brook-513216-a6.silver_alfabetizacao.vw_monitoramento_pipeline` AS
SELECT
    job_id AS id_execucao,
    creation_time AS inicio_ingestao,
    end_time AS fim_processamento,
    TIMESTAMP_DIFF(end_time, creation_time, SECOND) AS latencia_segundos,
    job_type AS tipo_operacao,
    statement_type AS comando_sql,
    total_bytes_processed AS bytes_processados,
    ROUND(total_bytes_processed / POW(1024, 2), 2) AS megabytes_processados,
    error_result.message AS mensagem_erro,
    CASE WHEN error_result IS NULL THEN 'SUCESSO' ELSE 'FALHA' END AS status_execucao
FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_USER
WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY);