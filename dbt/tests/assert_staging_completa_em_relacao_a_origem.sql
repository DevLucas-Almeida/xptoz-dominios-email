-- Todo registro da origem deve existir na staging.
-- Retorna os ids da origem que não chegaram na staging.

select origem.id
from {{ source('raw', 'users_info') }} as origem
left join {{ ref('stg_users') }} as stg
    on stg.id = origem.id
where stg.id is null
