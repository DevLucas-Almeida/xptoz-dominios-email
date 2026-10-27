-- =============================================================================
-- Consultas de validação do pipeline (rodar no Postgres do desafio)
-- =============================================================================

-- 1. Volume em cada camada: os quatro totais devem ser iguais
SELECT 'raw'          AS camada, COUNT(*) AS registros FROM public.users_info
UNION ALL
SELECT 'staging',      COUNT(*) FROM staging.stg_users
UNION ALL
SELECT 'intermediate', COUNT(*) FROM intermediate.int_users_enriched
UNION ALL
SELECT 'marts (soma)', SUM(total_registros) FROM marts.mart_email_domains;

-- 2. Resultado final: domínios mais utilizados
SELECT
    dominio_email,
    total_registros,
    ROUND(100.0 * total_registros / SUM(total_registros) OVER (), 1) AS percentual,
    created_at,
    updated_at
FROM marts.mart_email_domains
ORDER BY total_registros DESC, dominio_email;

-- 3. Padronização da staging: origem x staging lado a lado
SELECT r.nome AS nome_origem, s.nome AS nome_staging,
       r.email AS email_origem, s.email AS email_staging,
       r.cidade AS cidade_origem, s.cidade AS cidade_staging,
       s.created_at, s.updated_at
FROM public.users_info r
JOIN staging.stg_users s USING (id)
ORDER BY id
LIMIT 10;

-- 4. Extração de nomes, incluindo casos com pronome de tratamento
SELECT nome, primeiro_nome, ultimo_nome, email, dominio_email
FROM intermediate.int_users_enriched
WHERE nome ~ '^(DR|DRA|SR|SRA|SRTA)\.'
ORDER BY id
LIMIT 10;

-- 5. Incremental: registros antigos mantêm o created_at original
--    (após mais de uma carga, deve haver mais de um lote de created_at)
SELECT created_at, COUNT(*) AS registros
FROM staging.stg_users
GROUP BY created_at
ORDER BY created_at;
