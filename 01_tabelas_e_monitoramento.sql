-- cria tabela pra registrar a qualidade dos dados (data quality)
CREATE TABLE IF NOT EXISTS `centering-brook-513216-a6.silver_alfabetizacao.log_qualidade` (
    data_execucao TIMESTAMP,
    nome_tabela STRING,
    tipo_regra STRING, 
    status STRING,     
    mensagem_erro STRING
);

-- tabela pra guardar o historico de metricas da pipeline
CREATE TABLE IF NOT EXISTS `centering-brook-513216-a6.silver_alfabetizacao.metricas_execucao_pipeline` (
    data_hora_execucao TIMESTAMP,
    etapa_pipeline STRING,
    linhas_entrada INT64,
    linhas_saida INT64,
    duracao_segundos INT64,
    status_final STRING
);

-- tabela de streaming com os dois tempos pra medir latencia real
CREATE OR REPLACE TABLE `centering-brook-513216-a6.silver_alfabetizacao.streaming_avaliacoes_simuladas` (
    ano INT64,
    id_municipio STRING,
    taxa_alfabetizacao FLOAT64,
    timestamp_producao_evento TIMESTAMP, 
    timestamp_ingestao_gcp TIMESTAMP,    
    origem STRING
);

-- view pra monitorar latencia e bytes processados nativo do bq (finops)
CREATE OR REPLACE VIEW `centering-brook-513216-a6.silver_alfabetizacao.vw_monitoramento_pipeline` AS
SELECT 
    job_id AS id_execucao,
    creation_time AS inicio_ingestao,
    TIMESTAMP_DIFF(end_time, creation_time, SECOND) AS latencia_segundos,
    ROUND(total_bytes_processed / (1024 * 1024), 2) AS volume_processado_mb,
    state AS status_execucao,
    CASE 
        WHEN error_result IS NOT NULL THEN error_result.message 
        ELSE 'sucesso - sem falhas' 
    END AS alertas_erro
FROM `region-us.INFORMATION_SCHEMA.JOBS_BY_USER`
WHERE project_id = 'centering-brook-513216-a6'
ORDER BY creation_time DESC;
