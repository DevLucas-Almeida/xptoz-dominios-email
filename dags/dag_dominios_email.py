"""
DAG diária: domínios de e-mail mais utilizados nos cadastros.

Fluxo:
    verificar_origem
        → transformacao_dbt  (Cosmos: uma task de run + uma de test por modelo,
                              respeitando staging → intermediate → marts)
        → resumo_resultado   (registra no log o ranking final de domínios)

Agendamento: todo dia às 06:00 (America/Sao_Paulo).
"""

import os
from datetime import timedelta

import pendulum
from airflow.exceptions import AirflowFailException
from airflow.sdk import dag, task
from cosmos import (
    DbtTaskGroup,
    ExecutionConfig,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
)
from cosmos.constants import LoadMode, SourceRenderingBehavior, TestBehavior

DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/opt/airflow/dbt")
DBT_EXECUTABLE = os.getenv("DBT_EXECUTABLE_PATH", "/opt/dbt_venv/bin/dbt")


def conectar_postgres():
    """Abre conexão com o Postgres do desafio a partir das variáveis PG_*."""
    import psycopg2

    return psycopg2.connect(
        host=os.environ["PG_HOST"],
        port=os.getenv("PG_PORT", "5432"),
        user=os.environ["PG_USER"],
        password=os.environ["PG_PASSWORD"],
        dbname=os.environ["PG_DBNAME"],
        sslmode=os.getenv("PG_SSLMODE", "prefer"),
        connect_timeout=15,
    )


@dag(
    dag_id="dominios_email_diario",
    description="Consolida a quantidade de cadastros por domínio de e-mail (dbt: staging → intermediate → marts)",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2026, 10, 1, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=30),
    default_args={
        "owner": "engenharia-de-dados",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["xptoz", "dbt", "diario"],
    doc_md=__doc__,
)
def dominios_email_diario():
    @task(retries=0)
    def verificar_origem() -> int:
        """Falha cedo, sem retentativas, se a tabela de origem não existir ou estiver vazia."""
        conn = conectar_postgres()
        try:
            with conn.cursor() as cur:
                cur.execute("select to_regclass('public.users_info') is not null")
                if not cur.fetchone()[0]:
                    raise AirflowFailException(
                        "Tabela public.users_info não encontrada. Rode scripts/generate_data.py antes."
                    )
                cur.execute("select count(*) from public.users_info")
                total = cur.fetchone()[0]
        finally:
            conn.close()

        if total == 0:
            raise AirflowFailException("public.users_info está vazia; nada a processar.")

        print(f"users_info com {total} registros. Seguindo para o dbt.")
        return total

    transformacao_dbt = DbtTaskGroup(
        group_id="transformacao_dbt",
        project_config=ProjectConfig(dbt_project_path=DBT_PROJECT_DIR),
        profile_config=ProfileConfig(
            profile_name="xptoz",
            target_name="dev",
            profiles_yml_filepath=f"{DBT_PROJECT_DIR}/profiles.yml",
        ),
        execution_config=ExecutionConfig(dbt_executable_path=DBT_EXECUTABLE),
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            test_behavior=TestBehavior.AFTER_EACH,
            # Renderiza a source users_info para executar os testes dela antes da staging.
            source_rendering_behavior=SourceRenderingBehavior.WITH_TESTS_OR_FRESHNESS,
            # Testes que dependem de mais de um modelo (ex.: soma do mart vs. intermediate)
            # rodam em tasks próprias, depois de todos os modelos de que dependem.
            should_detach_multiple_parents_tests=True,
        ),
    )

    @task
    def resumo_resultado() -> list[dict]:
        """Registra no log o ranking de domínios gerado na camada marts."""
        conn = conectar_postgres()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    select dominio_email, total_registros, updated_at
                    from marts.mart_email_domains
                    order by total_registros desc, dominio_email
                    """
                )
                linhas = cur.fetchall()
        finally:
            conn.close()

        total = sum(qtd for _, qtd, _ in linhas)
        print(f"{'DOMÍNIO':<30}{'REGISTROS':>10}{'%':>8}")
        for dominio, qtd, _ in linhas:
            print(f"{dominio:<30}{qtd:>10}{qtd / total:>8.1%}")
        print(f"{'TOTAL':<30}{total:>10}")

        return [
            {"dominio_email": dominio, "total_registros": qtd, "atualizado_em": str(atualizado)}
            for dominio, qtd, atualizado in linhas
        ]

    verificar_origem() >> transformacao_dbt >> resumo_resultado()


dominios_email_diario()
