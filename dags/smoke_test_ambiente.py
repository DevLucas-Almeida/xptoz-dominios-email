"""
DAG de verificação do ambiente (Fase 0).

Execução manual. Confirma que, de dentro do Airflow, é possível:
  1. conectar no Postgres do desafio (local ou Render) usando as variáveis PG_*;
  2. executar o dbt (virtualenv isolado) e validar o profile com `dbt debug`.
"""

import os

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task


@dag(
    dag_id="smoke_test_ambiente",
    description="Verifica conexão com o Postgres do desafio e a instalação do dbt",
    schedule=None,
    start_date=pendulum.datetime(2026, 10, 1, tz="America/Sao_Paulo"),
    catchup=False,
    tags=["infra", "smoke-test"],
)
def smoke_test_ambiente():
    @task
    def testar_conexao_postgres() -> str:
        import psycopg2

        conn = psycopg2.connect(
            host=os.environ["PG_HOST"],
            port=os.getenv("PG_PORT", "5432"),
            user=os.environ["PG_USER"],
            password=os.environ["PG_PASSWORD"],
            dbname=os.environ["PG_DBNAME"],
            sslmode=os.getenv("PG_SSLMODE", "prefer"),
            connect_timeout=15,
        )
        try:
            with conn.cursor() as cur:
                cur.execute("select version()")
                versao = cur.fetchone()[0]
                cur.execute("select to_regclass('public.users_info') is not null")
                tabela_existe = cur.fetchone()[0]
        finally:
            conn.close()

        print(f"Conectado em {os.environ['PG_HOST']}: {versao}")
        print(f"Tabela users_info existe: {tabela_existe}")
        return versao

    dbt_debug = BashOperator(
        task_id="dbt_debug",
        bash_command="dbt --version && dbt debug",
    )

    testar_conexao_postgres() >> dbt_debug


smoke_test_ambiente()
