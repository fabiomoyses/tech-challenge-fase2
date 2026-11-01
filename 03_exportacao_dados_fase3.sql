-- joga a tabela consolidada oficial pro storage no formato csv
EXPORT DATA OPTIONS(
  uri='gs://bronze-alfabetizacao-fabio-2026/exportacao_fase3/oficial_*.csv',
  format='CSV',
  overwrite=true,
  header=true,
  field_delimiter=','
) AS
SELECT * FROM `centering-brook-513216-a6.gold_alfabetizacao.analise_consolidada_municipios`;

-- joga a tabela hibrida (batch + streaming) pro storage tb
EXPORT DATA OPTIONS(
  uri='gs://bronze-alfabetizacao-fabio-2026/exportacao_fase3/hibrida_*.csv',
  format='CSV',
  overwrite=true,
  header=true,
  field_delimiter=','
) AS
SELECT * FROM `centering-brook-513216-a6.gold_alfabetizacao.analise_hibrida_batch_streaming`;
