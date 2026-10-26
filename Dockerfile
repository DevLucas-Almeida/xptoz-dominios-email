ARG AIRFLOW_VERSION=3.3.2
ARG PYTHON_VERSION=3.12
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

ARG AIRFLOW_VERSION

# -----------------------------------------------------------------------------
# 1) dbt em um virtualenv isolado (/opt/dbt_venv).
#    dbt e Airflow compartilham várias dependências; separá-los evita conflitos
#    de versão. O link em /usr/local/bin deixa o comando `dbt` disponível no PATH.
# -----------------------------------------------------------------------------
USER root
COPY requirements-dbt.txt /tmp/requirements-dbt.txt
RUN python -m venv /opt/dbt_venv \
    && PIP_USER=false /opt/dbt_venv/bin/pip install --no-cache-dir -r /tmp/requirements-dbt.txt \
    && ln -s /opt/dbt_venv/bin/dbt /usr/local/bin/dbt \
    && rm /tmp/requirements-dbt.txt

# -----------------------------------------------------------------------------
# 2) Dependências do Airflow (Cosmos e Faker).
#    Fixar apache-airflow na mesma linha impede que o pip atualize o Airflow.
# -----------------------------------------------------------------------------
USER airflow
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements.txt

# O projeto dbt é montado em /opt/airflow/dbt pelo docker-compose.
ENV DBT_PROJECT_DIR=/opt/airflow/dbt \
    DBT_PROFILES_DIR=/opt/airflow/dbt
