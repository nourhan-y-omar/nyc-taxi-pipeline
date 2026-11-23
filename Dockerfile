FROM python:3.12-slim-bookworm AS pipeline
WORKDIR /opt/taxi
ENV PYTHONPATH=/opt/taxi/src \
    PYTHONUNBUFFERED=1 \
    DBT_SEND_ANONYMOUS_USAGE_STATS=false
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src ./src
COPY dbt ./dbt
ENTRYPOINT ["python", "-m", "taxi_pipeline"]

FROM apache/airflow:3.1.7-python3.12 AS airflow
WORKDIR /opt/taxi
ENV PYTHONPATH=/opt/taxi/src \
    PYTHONUNBUFFERED=1 \
    DBT_SEND_ANONYMOUS_USAGE_STATS=false
COPY --chown=airflow:root requirements.txt .
RUN python -m venv /opt/airflow/taxi-venv \
    && /opt/airflow/taxi-venv/bin/pip install --no-cache-dir -r requirements.txt \
    && mkdir -p /opt/airflow/local-state/logs
COPY --chown=airflow:root src ./src
COPY --chown=airflow:root dbt ./dbt
COPY --chown=airflow:root dags ./dags
