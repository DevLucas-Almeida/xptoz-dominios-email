# XPTOZ — Análise de domínios de e-mail

Pipeline de dados que identifica os domínios de e-mail mais usados nos cadastros de pessoas e a quantidade de registros de cada domínio. Os dados brutos ficam no PostgreSQL, as transformações são feitas em camadas com **dbt** e a execução diária é orquestrada pelo **Apache Airflow**.

> 🚧 Em construção. Fase atual: **ambiente** (Docker, Airflow, Postgres e dbt conectados).

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
