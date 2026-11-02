
-- regras de qualidade de dados (fail-fast com assert)


-- verificacao bloqueante de chaves nulas no streaming
INSERT INTO `centering-brook-513216-a6.silver_alfabetizacao.log_qualidade`
VALUES (CURRENT_TIMESTAMP(), 'streaming_avaliacoes_simuladas', 'Bloqueante', 'INICIANDO', 'Verificando chaves primarias nulas');

ASSERT (
    (SELECT COUNT(*) FROM `centering-brook-513216-a6.silver_alfabetizacao.streaming_avaliacoes_simuladas` WHERE id_municipio IS NULL) = 0
) AS 'FALHA DE QUALIDADE BLOQUEANTE: Foram encontrados registros com id_municipio nulo!';

INSERT INTO `centering-brook-513216-a6.silver_alfabetizacao.log_qualidade`
VALUES (CURRENT_TIMESTAMP(), 'streaming_avaliacoes_simuladas', 'Bloqueante', 'SUCESSO', 'Nenhuma chave nula encontrada.');