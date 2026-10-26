-- Regras de extração da camada intermediate:
--   * primeiro_nome não pode ser um pronome de tratamento;
--   * primeiro_nome deve ser o início do nome e ultimo_nome o final;
--   * dominio_email deve ser exatamente o trecho após o "@" e conter um ponto.

select id, nome, primeiro_nome, ultimo_nome, email, dominio_email
from {{ ref('int_users_enriched') }}
where primeiro_nome ~ '\.$'
   or position(primeiro_nome in nome) = 0
   or (ultimo_nome is not null and right(nome, length(ultimo_nome)) <> ultimo_nome)
   or email <> split_part(email, '@', 1) || '@' || dominio_email
   or dominio_email !~ '^[^@\s]+\.[^@\s]+$'
