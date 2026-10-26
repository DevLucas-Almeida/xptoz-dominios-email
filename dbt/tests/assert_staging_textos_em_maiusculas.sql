-- Campos textuais da staging devem estar em maiúsculas e sem espaços extras.

select id, nome, email, cidade
from {{ ref('stg_users') }}
where nome   <> upper(nome)
   or email  <> upper(email)
   or cidade <> upper(cidade)
   or nome   ~ '(^\s|\s$|\s{2,})'
   or cidade ~ '(^\s|\s$|\s{2,})'
