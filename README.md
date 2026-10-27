# XPTOZ — Análise de domínios de e-mail

Pipeline de dados que identifica os domínios de e-mail mais usados nos cadastros de pessoas e a quantidade de registros de cada domínio. Os dados brutos ficam no PostgreSQL, as transformações são feitas em camadas com **dbt** e a execução diária é orquestrada pelo **Apache Airflow**.

> 🚧 Em construção. Concluído: ambiente, camadas do dbt e DAG diária. Próximo: documentação final e vídeo.

## Arquitetura

```
scripts/generate_data.py ──► PostgreSQL: public.users_info  (raw)
                                    │
                     Airflow (DAG diária) ──► dbt
                                    │
  staging.stg_users ──► intermediate.int_users_enriched ──► marts.mart_email_domains
```

| Componente | Versão | Papel |
|---|---|---|
| PostgreSQL | 16 | Armazena os dados brutos e as camadas do dbt (local via Docker ou no Render) |
| Python + Faker | 3.12 | Gera e carrega os dados fictícios em `users_info` |
| dbt Core + dbt-postgres | 1.12 / 1.11 | Transformações em camadas (staging → intermediate → marts) |
| Apache Airflow | 3.3.2 | Agendamento diário e orquestração dos modelos dbt |
| Astronomer Cosmos | 1.15 | Renderiza cada modelo dbt como uma task do Airflow |

O dbt roda em um **virtualenv isolado** dentro da imagem do Airflow (`/opt/dbt_venv`) para evitar conflitos de dependências entre as duas ferramentas.

## Estrutura do repositório

```
├── dags/                       # DAGs do Airflow
│   ├── dag_dominios_email.py   # DAG diária (06:00) que orquestra o dbt via Cosmos
│   └── smoke_test_ambiente.py  # verificação do ambiente (execução manual)
├── dbt/                        # projeto dbt
│   ├── dbt_project.yml
│   ├── profiles.yml            # conexão via variáveis de ambiente (sem credenciais)
│   ├── macros/                 # generate_schema_name: schemas staging/intermediate/marts
│   ├── models/{staging,intermediate,marts}/
│   └── tests/
├── scripts/
│   └── generate_data.py        # script do case, lendo credenciais do ambiente
├── docs/evidencias/            # prints e resultados de execução
├── Dockerfile                  # Airflow 3.3.2 + Cosmos + dbt (venv isolado)
├── docker-compose.yml          # Airflow (LocalExecutor) + Postgres local
├── .env.example                # modelo de variáveis de ambiente
└── Makefile                    # atalhos dos comandos
```

## Pré-requisitos

- Docker Desktop (ou Docker Engine + Compose v2) com pelo menos 4 GB de memória disponíveis
- Git
- `make` (opcional — os comandos equivalentes estão abaixo)

## Configuração

### 1. Variáveis de ambiente

```bash
cp .env.example .env
```

Preencha o `.env`. Ele está no `.gitignore` e nunca deve ser versionado. As variáveis `PG_*` definem o banco do desafio e são lidas pelo script de carga, pelo dbt e pelas DAGs. Há duas opções:

**Opção A — Postgres local (container `warehouse`)**: mantenha os valores padrão do `.env.example`.

**Opção B — Postgres no Render**:

1. No [painel do Render](https://dashboard.render.com/new/database), crie um **Postgres** (plano Free, PostgreSQL 16, região mais próxima — ex.: Virginia).
2. Quando o status for *Available*, abra **Connect → External** e copie host, porta, banco, usuário e senha.
3. No `.env`, preencha `PG_HOST`, `PG_PORT`, `PG_USER`, `PG_PASSWORD`, `PG_DBNAME` e use `PG_SSLMODE=require` (o Render recusa conexões sem TLS).

> O plano gratuito do Render expira após 30 dias (com 14 dias de carência). Se o banco expirar, use a opção A ou recrie a instância e rode a carga novamente.

### 2. Segredos do Airflow

Gere e preencha no `.env`:

```bash
# FERNET_KEY
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
# AIRFLOW__API_AUTH__JWT_SECRET
python -c "import secrets; print(secrets.token_urlsafe(32))"
```

No Linux, defina também `AIRFLOW_UID` com o resultado de `id -u`.

## Execução

| Etapa | Com make | Sem make |
|---|---|---|
| Construir a imagem | `make build` | `docker compose build` |
| Subir o ambiente | `make up` | `docker compose up -d` |
| Carregar os dados | `make load-data` | `docker compose run --rm airflow-cli python /opt/airflow/scripts/generate_data.py` |
| Contar registros | `make db-count` | ver `Makefile` |
| Validar o dbt | `make dbt-debug` | `docker compose run --rm airflow-cli bash -c "dbt debug"` |
| Rodar modelos e testes | `make dbt-build` | `docker compose run --rm airflow-cli bash -c "dbt build"` |
| Documentação e lineage | `make dbt-docs` | `docker compose run --rm -p 8081:8081 airflow-cli bash -c "dbt docs generate && dbt docs serve --host 0.0.0.0 --port 8081 --no-browser"` |
| Rodar a DAG diária pelo terminal | `make dag-test` | `docker compose run --rm airflow-cli airflow dags test dominios_email_diario` |
| Testar o ambiente | `make smoke-test` | `docker compose run --rm airflow-cli airflow dags test smoke_test_ambiente` |
| Parar | `make down` | `docker compose down` |

A UI do Airflow fica em **http://localhost:8080** (usuário e senha definidos em `_AIRFLOW_WWW_USER_*` no `.env`; padrão `airflow`/`airflow`).

Com a opção A, o Postgres local fica acessível em `localhost:5433` para inspeção com DBeaver ou `psql`, usando as credenciais `LOCAL_PG_*`.

### Validação da carga

```sql
SELECT COUNT(*) AS total_registros FROM users_info;

SELECT id, nome, email, idade, cidade FROM users_info LIMIT 10;
```

A cada execução do script são gerados 1.000 registros. A quantidade inserida pode ser um pouco menor, porque e-mails repetidos são ignorados pela restrição `UNIQUE` em `email`.

## Camadas do dbt

| Camada | Modelo | Materialização | Conteúdo |
|---|---|---|---|
| Raw | `public.users_info` (source) | tabela de origem | Dados como gerados pelo script |
| Staging | `staging.stg_users` | incremental (`unique_key=id`) | Textos em maiúsculas e sem espaços extras, vazios como NULL, `created_at` e `updated_at` |
| Intermediate | `intermediate.int_users_enriched` | table | `primeiro_nome`, `ultimo_nome` e `dominio_email` |
| Marts | `marts.mart_email_domains` | table | `total_registros` por `dominio_email` |

### Decisões de modelagem

- **`created_at` / `updated_at` na staging:** o modelo é incremental com merge por `id`. Um registro novo recebe `now()` nos dois campos; um registro existente mantém o `created_at` original, e o `updated_at` só muda quando algum campo do registro foi alterado na origem. Assim os campos de controle têm significado estável entre execuções diárias, em vez de mudarem a cada leitura.
- **Pronomes de tratamento:** o Faker gera cerca de 19% dos nomes com `Dr.`, `Dra.`, `Sr.`, `Sra.` ou `Srta.`. Eles são removidos antes de separar o nome, para que o primeiro nome não seja, por exemplo, `DR.`. A coluna `nome` mantém o nome completo original.
- **Último nome:** é o último token do nome. Partículas (`DA`, `DAS`) ficam no meio e não afetam o resultado. Sobrenomes compostos sem partícula (ex.: `CASA GRANDE`) retornam só o último token — limitação documentada.
- **`created_at` / `updated_at` no mart:** `created_at` é quando o primeiro cadastro do domínio entrou na staging; `updated_at` é quando a agregação foi recalculada.
- **Apenas 3 domínios no resultado:** o script do case usa `fake.email()`, que por padrão gera somente domínios reservados para exemplos (`example.com`, `example.net`, `example.org`). Por isso o mart tem 3 linhas, com distribuição próxima de 1/3 cada. O script foi mantido como fornecido.

### Testes

`dbt build` executa os modelos e 25 testes na ordem das dependências:

- **Genéricos:** `unique` e `not_null` nas chaves da origem e de cada camada; `not_null` em `primeiro_nome`, `dominio_email` e nos campos de controle.
- **Singulares (`dbt/tests/`):**
  - `assert_staging_completa_em_relacao_a_origem` — nenhum registro da origem fica de fora da staging;
  - `assert_staging_textos_em_maiusculas` — padronização de textos;
  - `assert_intermediate_nomes_e_dominios_validos` — primeiro nome sem pronome, extração correta de nome e domínio;
  - `assert_mart_soma_igual_total_usuarios` — a soma do mart é igual ao total de cadastros (nenhum registro perdido ou duplicado na agregação).

### Consultas de conferência

```sql
-- Resultado final
SELECT dominio_email, total_registros
FROM marts.mart_email_domains
ORDER BY total_registros DESC;

-- Amostra da extração de nomes e domínios
SELECT nome, primeiro_nome, ultimo_nome, dominio_email
FROM intermediate.int_users_enriched
LIMIT 10;
```

## Orquestração (Airflow)

A DAG **`dominios_email_diario`** (`dags/dag_dominios_email.py`) executa o pipeline completo uma vez por dia.

| Configuração | Valor |
|---|---|
| Agendamento | `0 6 * * *` — todo dia às 06:00, fuso `America/Sao_Paulo` |
| Catchup | desativado (não reprocessa dias passados ao ser ativada) |
| Execuções simultâneas | no máximo 1 |
| Retentativas | 2, com intervalo de 5 minutos (a verificação da origem não tem retentativa) |
| Timeout da execução | 30 minutos |

### Fluxo de tasks

```
verificar_origem
  → transformacao_dbt
      raw_users_info (testes da source)
        → stg_users.run → stg_users.test
            → int_users_enriched.run → int_users_enriched.test
                → mart_email_domains.run → mart_email_domains.test
      + testes que dependem de mais de um modelo, em tasks próprias
  → resumo_resultado
```

- **`verificar_origem`** — falha imediatamente, sem retentativas, se `public.users_info` não existir ou estiver vazia.
- **`transformacao_dbt`** — grupo gerado pelo [Astronomer Cosmos](https://github.com/astronomer/astronomer-cosmos) a partir do projeto dbt. Cada modelo vira uma task de `run` seguida de uma task de `test`, e as dependências vêm dos `ref()` do dbt. Assim, a ordem staging → intermediate → marts é garantida e uma falha de teste interrompe as camadas seguintes. O status de cada modelo pode ser acompanhado individualmente na UI.
- **`resumo_resultado`** — imprime no log o ranking de domínios com a quantidade e o percentual de cada um, e publica o resultado em XCom.

### Como executar

1. Acesse http://localhost:8080 e ative a DAG `dominios_email_diario` pelo toggle (DAGs novas começam pausadas).
2. A partir daí, ela roda automaticamente todos os dias às 06:00. Para executar na hora, clique em **Trigger**.
3. Acompanhe o andamento em **Grid** ou **Graph**. O resultado final aparece no log da task `resumo_resultado`.

Para demonstrar a atualização dos resultados: rode `make load-data` para inserir novos cadastros e dispare a DAG de novo. Os totais em `marts.mart_email_domains` aumentam, e os registros antigos mantêm o `created_at` original na staging.
