{#
    Staging — padronização dos dados brutos.

    - Campos textuais em maiúsculas, sem espaços nas pontas e sem espaços repetidos.
    - Textos vazios viram NULL; idades fora de 0–120 viram NULL.
    - created_at / updated_at com now():
        * incremental (merge por id): a origem é relida a cada execução, mas
          created_at é preservado para registros que já existiam;
        * updated_at só muda quando algum campo do registro mudou.
#}

{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='fail'
    )
}}

with origem as (

    select * from {{ source('raw', 'users_info') }}

),

padronizado as (

    select
        id,
        nullif(upper(regexp_replace(trim(nome), '\s+', ' ', 'g')), '')   as nome,
        nullif(upper(trim(email)), '')                                   as email,
        case when idade between 0 and 120 then idade end                 as idade,
        nullif(upper(regexp_replace(trim(cidade), '\s+', ' ', 'g')), '') as cidade
    from origem

)

select
    novo.id,
    novo.nome,
    novo.email,
    novo.idade,
    novo.cidade,

    {% if is_incremental() %}
    coalesce(atual.created_at, now())                            as created_at,
    case
        when atual.id is null then now()
        when (novo.nome, novo.email, novo.idade, novo.cidade)
             is distinct from
             (atual.nome, atual.email, atual.idade, atual.cidade) then now()
        else atual.updated_at
    end                                                          as updated_at
    {% else %}
    now()                                                        as created_at,
    now()                                                        as updated_at
    {% endif %}

from padronizado as novo

{% if is_incremental() %}
left join {{ this }} as atual
    on atual.id = novo.id
{% endif %}
