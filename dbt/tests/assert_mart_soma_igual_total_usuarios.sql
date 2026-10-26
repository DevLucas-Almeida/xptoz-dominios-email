-- A soma de total_registros no mart deve ser igual ao número de cadastros
-- com domínio na camada intermediate (nenhum registro perdido ou duplicado).
-- Retorna linha (falha) quando os totais divergem.

with mart as (
    select coalesce(sum(total_registros), 0) as total from {{ ref('mart_email_domains') }}
),

intermediate as (
    select count(*) as total from {{ ref('int_users_enriched') }} where dominio_email is not null
)

select
    mart.total         as total_mart,
    intermediate.total as total_intermediate
from mart
cross join intermediate
where mart.total <> intermediate.total
