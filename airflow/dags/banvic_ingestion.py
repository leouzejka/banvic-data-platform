from datetime import datetime, timedelta, timezone

import os

import psycopg2

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from kubernetes.client import models as k8s


PIPELINE_NAME = "banvic_ingestion"

RAW_TABLES = {
    "agencias": ["cod_agencia"],
    "clientes": ["cod_cliente"],
    "colaborador_agencia": ["cod_colaborador", "cod_agencia"],
    "colaboradores": ["cod_colaborador"],
    "conta": ["num_conta"],
    "proposta_credito": ["cod_proposta"],
    "transacoes": ["cod_transacao"],
}

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


def get_database_connection():
    return psycopg2.connect(
        host=os.environ.get("TARGET_POSTGRES_HOST", "localhost"),
        port=os.environ.get("TARGET_POSTGRES_PORT", "5432"),
        database=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["TARGET_POSTGRES_PASSWORD"],
    )


def registrar_inicio(**context):
    execution_id = context["run_id"]
    started_at = context["dag_run"].start_date

    conn = get_database_connection()

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO metadata.pipeline_execution (
                    pipeline_name,
                    execution_id,
                    status,
                    started_at
                )
                VALUES (%s, %s, 'RUNNING', %s)
                ON CONFLICT (pipeline_name, execution_id)
                DO UPDATE SET
                    status = 'RUNNING',
                    started_at = EXCLUDED.started_at,
                    finished_at = NULL,
                    records_processed = NULL,
                    error_message = NULL
                """,
                (
                    PIPELINE_NAME,
                    execution_id,
                    started_at,
                ),
            )

        conn.commit()
    finally:
        conn.close()


def validar_raw():
    """Valida presença, chaves nulas e duplicidades na carga raw."""
    conn = get_database_connection()

    try:
        with conn.cursor() as cur:
            for table, key_columns in RAW_TABLES.items():
                cur.execute(f"SELECT COUNT(*) FROM raw.{table}")
                if cur.fetchone()[0] == 0:
                    raise AirflowException(
                        f"A tabela raw.{table} foi carregada sem registros."
                    )

                key_expression = ", ".join(key_columns)
                null_predicate = " OR ".join(
                    f"{column} IS NULL" for column in key_columns
                )
                cur.execute(
                    f"SELECT COUNT(*) FROM raw.{table} "
                    f"WHERE {null_predicate}"
                )
                if cur.fetchone()[0] > 0:
                    raise AirflowException(
                        f"A tabela raw.{table} possui chaves nulas."
                    )

                cur.execute(
                    f"SELECT {key_expression}, COUNT(*) "
                    f"FROM raw.{table} "
                    f"GROUP BY {key_expression} "
                    f"HAVING COUNT(*) > 1 LIMIT 1"
                )
                if cur.fetchone() is not None:
                    raise AirflowException(
                        f"A tabela raw.{table} possui chaves duplicadas."
                    )
    finally:
        conn.close()


def construir_analytics():
    """Publica uma visão analítica simples e reproduzível por agência."""
    conn = get_database_connection()

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS analytics.saldo_por_agencia (
                    cod_agencia BIGINT PRIMARY KEY,
                    nome_agencia TEXT NOT NULL,
                    quantidade_contas BIGINT NOT NULL,
                    saldo_total NUMERIC NOT NULL,
                    saldo_disponivel NUMERIC NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL
                )
                """
            )
            cur.execute("TRUNCATE TABLE analytics.saldo_por_agencia")
            cur.execute(
                """
                INSERT INTO analytics.saldo_por_agencia (
                    cod_agencia,
                    nome_agencia,
                    quantidade_contas,
                    saldo_total,
                    saldo_disponivel,
                    updated_at
                )
                SELECT
                    a.cod_agencia,
                    a.nome,
                    COUNT(c.num_conta),
                    COALESCE(SUM(c.saldo_total), 0),
                    COALESCE(SUM(c.saldo_disponivel), 0),
                    CURRENT_TIMESTAMP
                FROM raw.agencias AS a
                LEFT JOIN raw.conta AS c
                    ON c.cod_agencia = a.cod_agencia
                GROUP BY a.cod_agencia, a.nome
                """
            )
        conn.commit()
    finally:
        conn.close()


def finalizar_execucao(**context):
    execution_id = context["run_id"]
    finished_at = datetime.now(timezone.utc)

    task_states = context["task_instance"].get_task_states(
        dag_id=context["dag"].dag_id,
        task_ids=["ingestao_meltano", "validar_raw", "construir_analytics"],
        run_ids=[context["run_id"]],
    )
    states = task_states[context["run_id"]]
    failed_tasks = [
        task_id
        for task_id, state in states.items()
        if state != "success"
    ]

    if not failed_tasks:
        status = "SUCCESS"
        error_message = None

        conn = get_database_connection()

        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        (
                            SELECT COUNT(*) FROM raw.agencias
                        )
                        + (
                            SELECT COUNT(*) FROM raw.clientes
                        )
                        + (
                            SELECT COUNT(*) FROM raw.colaborador_agencia
                        )
                        + (
                            SELECT COUNT(*) FROM raw.colaboradores
                        )
                        + (
                            SELECT COUNT(*) FROM raw.conta
                        )
                        + (
                            SELECT COUNT(*) FROM raw.proposta_credito
                        )
                        + (
                            SELECT COUNT(*) FROM raw.transacoes
                        )
                    """
                )

                records_processed = cur.fetchone()[0]

                cur.execute(
                    """
                    UPDATE metadata.pipeline_execution
                    SET
                        status = %s,
                        finished_at = %s,
                        records_processed = %s,
                        error_message = %s
                    WHERE pipeline_name = %s
                      AND execution_id = %s
                    """,
                    (
                        status,
                        finished_at,
                        records_processed,
                        error_message,
                        PIPELINE_NAME,
                        execution_id,
                    ),
                )

            conn.commit()
        finally:
            conn.close()

        return

    status = "FAILED"
    error_message = f"Tarefas não concluídas com sucesso: {failed_tasks}"

    conn = get_database_connection()

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE metadata.pipeline_execution
                SET
                    status = %s,
                    finished_at = %s,
                    records_processed = NULL,
                    error_message = %s
                WHERE pipeline_name = %s
                  AND execution_id = %s
                """,
                (
                    status,
                    finished_at,
                    error_message,
                    PIPELINE_NAME,
                    execution_id,
                ),
            )

        conn.commit()
    finally:
        conn.close()

    raise AirflowException(error_message)


with DAG(
    dag_id="banvic_ingestion",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["banvic", "ingestion"],
) as dag:

    start_execution = PythonOperator(
        task_id="start_execution",
        python_callable=registrar_inicio,
    )

    ingestao_meltano = KubernetesPodOperator(
        task_id="ingestao_meltano",
        name="meltano-ingestion",
        namespace="banvic",
        image="banvic-meltano:4.2.2",
        image_pull_policy="IfNotPresent",
        cmds=["meltano"],
        arguments=["--environment=k8s", "run", "tap-csv", "target-postgres"],
        env_from=[
            k8s.V1EnvFromSource(
                secret_ref=k8s.V1SecretEnvSource(
                    name="banvic-postgres-connection",
                ),
            ),
        ],
        in_cluster=True,
        get_logs=True,
        on_finish_action="delete_pod",
        automount_service_account_token=False,
        startup_timeout_seconds=300,
        do_xcom_push=False,
    )

    validar_raw_task = PythonOperator(
        task_id="validar_raw",
        python_callable=validar_raw,
    )

    construir_analytics_task = PythonOperator(
        task_id="construir_analytics",
        python_callable=construir_analytics,
    )

    finalize_execution = PythonOperator(
        task_id="finalize_execution",
        python_callable=finalizar_execucao,
        trigger_rule=TriggerRule.ALL_DONE,
        retries=0,
    )

    (
        start_execution
        >> ingestao_meltano
        >> validar_raw_task
        >> construir_analytics_task
        >> finalize_execution
    )
