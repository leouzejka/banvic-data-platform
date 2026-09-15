FROM apache/airflow:3.3.1

USER airflow

RUN pip install --no-cache-dir \
    meltano==4.2.2 \
    apache-airflow-providers-postgres \
    apache-airflow-providers-cncf-kubernetes

COPY --chown=airflow:root meltano.yml /opt/airflow/meltano.yml
COPY --chown=airflow:root meltano/ /opt/airflow/meltano/
COPY --chown=airflow:root data/raw/ /opt/airflow/data/raw/
COPY --chown=airflow:root airflow/dags/ /opt/airflow/dags/