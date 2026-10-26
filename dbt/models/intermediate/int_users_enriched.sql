{#
    Intermediate — enriquecimento para a análise.

    - nome_limpo: remove pronomes de tratamento do início (DR., DRA., SR., SRA., SRTA.),
      que o Faker gera em ~19% dos nomes e que seriam lidos como primeiro nome.
    - primeiro_nome: primeiro token do nome limpo.
    - ultimo_nome: último token do nome limpo (NULL se o nome tiver um único token).
      Limitação conhecida: sobrenomes compostos sem partícula (ex.: "CASA GRANDE")
      retornam apenas o último token ("GRANDE").
    - dominio_email: tudo após o "@".
#}

with usuarios as (

    select * from {{ ref('stg_users') }}

),

nome_tratado as (

    select
        *,
        trim(regexp_replace(nome, '^((DRA?|SRA?|SRTA)\.\s*)+', '')) as nome_limpo
    from usuarios

)

select
    id,
    nome,
    split_part(nome_limpo, ' ', 1)                       as primeiro_nome,
    case
        when position(' ' in nome_limpo) > 0
            then regexp_replace(nome_limpo, '^.*\s', '')
    end                                                  as ultimo_nome,
    email,
    nullif(split_part(email, '@', 2), '')                as dominio_email,
    idade,
    cidade,
    created_at,
    updated_at
from nome_tratado
