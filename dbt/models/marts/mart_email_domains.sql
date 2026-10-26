{#
    Marts — quantidade de registros por domínio de e-mail.

    - created_at: quando o primeiro registro do domínio entrou na staging
      (menor created_at do grupo).
    - updated_at: momento em que a agregação foi recalculada (now()).
#}

with usuarios as (

    select * from {{ ref('int_users_enriched') }}

)

select
    dominio_email,
    count(*)        as total_registros,
    min(created_at) as created_at,
    now()           as updated_at
from usuarios
where dominio_email is not null
group by dominio_email
