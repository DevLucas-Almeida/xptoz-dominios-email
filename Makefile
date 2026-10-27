# Atalhos para os comandos mais usados. No Windows sem `make`, use os comandos
# `docker compose ...` equivalentes listados no README.

COMPOSE = docker compose
CLI     = $(COMPOSE) run --rm airflow-cli

.PHONY: help env build up down reset ps logs load-data db-count dbt-debug dbt-build dbt-docs dag-test shell smoke-test

help: ## Lista os comandos disponíveis
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-12s %s\n", $$1, $$2}'

env: ## Cria o .env a partir do .env.example (não sobrescreve)
	@test -f .env && echo ".env já existe" || (cp .env.example .env && echo ".env criado — preencha os valores")

build: ## Constrói a imagem do Airflow com dbt e Cosmos
	$(COMPOSE) build

up: ## Sobe o ambiente (UI em http://localhost:8080)
	$(COMPOSE) up -d

down: ## Para o ambiente (mantém os dados)
	$(COMPOSE) down

reset: ## Para o ambiente e APAGA os volumes (metadados do Airflow e Postgres local)
	$(COMPOSE) down -v

ps: ## Status dos containers
	$(COMPOSE) ps

logs: ## Acompanha os logs do scheduler
	$(COMPOSE) logs -f airflow-scheduler

load-data: ## Gera e carrega ~1.000 registros em users_info
	$(CLI) python /opt/airflow/scripts/generate_data.py

db-count: ## Conta os registros de users_info
	$(CLI) python -c "import os, psycopg2; c = psycopg2.connect(host=os.environ['PG_HOST'], port=os.environ['PG_PORT'], user=os.environ['PG_USER'], password=os.environ['PG_PASSWORD'], dbname=os.environ['PG_DBNAME'], sslmode=os.environ['PG_SSLMODE']); cur = c.cursor(); cur.execute('select count(*) from users_info'); print('users_info:', cur.fetchone()[0])"

dbt-debug: ## Valida a conexão do dbt com o banco
	$(CLI) bash -c "dbt debug"

dbt-build: ## Executa os modelos e testes do dbt (staging → intermediate → marts)
	$(CLI) bash -c "dbt build"

dbt-docs: ## Gera e serve a documentação do dbt em http://localhost:8081
	$(COMPOSE) run --rm -p 8081:8081 airflow-cli bash -c "dbt docs generate && dbt docs serve --host 0.0.0.0 --port 8081 --no-browser"

dag-test: ## Executa a DAG diária uma vez, de ponta a ponta, pelo terminal
	$(CLI) airflow dags test dominios_email_diario

smoke-test: ## Executa a DAG de verificação do ambiente
	$(CLI) airflow dags test smoke_test_ambiente

shell: ## Abre um bash dentro do container do Airflow
	$(CLI) bash
