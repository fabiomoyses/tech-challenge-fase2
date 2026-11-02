-- ============================================================================
-- exportacao da camada gold de volta para o cloud storage (consumo ia fase 3)
-- ============================================================================

EXPORT DATA OPTIONS(
    uri='gs://bronze-alfabetizacao-fabio-2026/exportacao_fase3/oficial_*.csv',
    format='CSV',
    overwrite=true,
    header=true,
    field_delimiter=','
) AS
SELECT * FROM `centering-brook-513216-a6.gold_alfabetizacao.analise_consolidada_municipios`;

EXPORT DATA OPTIONS(
    uri='gs://bronze-alfabetizacao-fabio-2026/exportacao_fase3/hibrida_*.csv',
    format='CSV',
    overwrite=true,
    header=true,
    field_delimiter=','
) AS
SELECT * FROM `centering-brook-513216-a6.gold_alfabetizacao.analise_hibrida_batch_streaming`;