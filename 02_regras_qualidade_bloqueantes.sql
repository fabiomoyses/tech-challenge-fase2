-- regra bloqueante pra barrar chave nula na silver (fail fast)
ASSERT (
  NOT EXISTS (
    SELECT 1 
    FROM `centering-brook-513216-a6.silver_alfabetizacao.streaming_avaliacoes_simuladas` 
    WHERE id_municipio IS NULL
  )
) AS 'falha bloqueante: pipeline parou pq tem id_municipio nulo na silver';

-- regra bloqueante pra garantir q o match do join fique acima de 80%
ASSERT (
  (
    SELECT 
      SAFE_DIVIDE(COUNT(meta_2030), COUNT(id_municipio))
    FROM `centering-brook-513216-a6.gold_alfabetizacao.analise_consolidada_municipios`
  ) >= 0.80
) AS 'falha bloqueante: taxa de match do join caiu pra menos de 80%';

-- se o pipeline sobreviver aos asserts acima, grava o sucesso no log
INSERT INTO `centering-brook-513216-a6.silver_alfabetizacao.log_qualidade`
VALUES (
    CURRENT_TIMESTAMP(), 
    'analise_consolidada_municipios', 
    'Bloqueante', 
    'SUCESSO', 
    'taxa de match do join ficou acima do limite de 80%'
);
