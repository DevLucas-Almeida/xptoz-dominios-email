"""
Testes de integridade das DAGs (executados no CI).

Garantem que as DAGs carregam sem erros, que o agendamento diário está
configurado e que o Cosmos gera as tasks na ordem das camadas.
"""

from pathlib import Path

import pytest
from airflow.dag_processing.dagbag import DagBag

DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"
DAG_ID = "dominios_email_diario"


@pytest.fixture(scope="session")
def dagbag() -> DagBag:
    return DagBag(dag_folder=str(DAGS_DIR))


@pytest.fixture(scope="session")
def dag(dagbag):
    dag = dagbag.get_dag(DAG_ID)
    assert dag is not None, f"DAG {DAG_ID} não encontrada"
    return dag


def test_dags_sem_erros_de_importacao(dagbag):
    assert dagbag.import_errors == {}


def test_dags_esperadas(dagbag):
    assert {DAG_ID, "smoke_test_ambiente"} <= set(dagbag.dag_ids)


def test_agendamento_diario(dag):
    assert dag.timetable.expression == "0 6 * * *"
    assert str(dag.timezone) == "America/Sao_Paulo"
    assert dag.catchup is False
    assert dag.max_active_runs == 1


def test_tasks_na_ordem_das_camadas(dag):
    ordem = [task.task_id for task in dag.topological_sort()]

    assert ordem[0] == "verificar_origem"
    assert ordem[-1] == "resumo_resultado"

    camadas = [
        "transformacao_dbt.stg_users.run",
        "transformacao_dbt.int_users_enriched.run",
        "transformacao_dbt.mart_email_domains.run",
    ]
    posicoes = [ordem.index(task_id) for task_id in camadas]
    assert posicoes == sorted(posicoes)


def test_cada_modelo_tem_task_de_teste(dag):
    for modelo in ("stg_users", "int_users_enriched", "mart_email_domains"):
        teste = dag.get_task(f"transformacao_dbt.{modelo}.test")
        assert f"transformacao_dbt.{modelo}.run" in teste.upstream_task_ids
