"""
dbt_pipeline — Week 6 starter DAG
=================================
Orchestrates the dbt pipeline on a schedule:

    dbt_seed
      -> dbt_test_sources
      -> dbt_run_stage
      -> dbt_test_stage
      -> dbt_run_dev
      -> dbt_test_dev
      -> dbt_build

Each task is a BashOperator that runs `dbt` directly. dbt is installed in
the Airflow image (see Dockerfile.airflow) and the dbt project is mounted at
/opt/airflow/dbt (see docker-compose.yml), so no Docker-in-Docker is needed.
dbt connects to the `postgres` service using the POSTGRES_* env vars that
docker-compose injects from .env into profiles.yml.

Read this file before you change it — Task 6.1 is just running it as-is.
Tasks 6.2 and 6.3 then ask you to extend it; look for the TODO comments.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# Path where docker-compose mounts ./dbt_learning inside the Airflow containers.
DBT_DIR = "/opt/airflow/dbt"

default_args = {
    "owner": "Osama",          # ← replace with your name
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def dbt_task(dag, task_id, dbt_command):
    """Build a BashOperator that cd's into the project and runs a dbt command."""
    return BashOperator(
        task_id=task_id,
        bash_command=f"cd {DBT_DIR} && dbt {dbt_command} --profiles-dir . --target dev",
        dag=dag,
    )


with DAG(
    dag_id="dbt_pipeline",
    description="Daily dbt pipeline: seed -> test sources -> stage -> dev",
    default_args=default_args,
    start_date=datetime(2026, 9, 1),
    schedule="0 6 * * *",             # daily at 06:00 UTC (Airflow cron is UTC)
    catchup=False,
    tags=["dbt", "dataops"],
) as dag:

    dbt_seed = dbt_task(dag, "dbt_seed", "seed")
    dbt_test_sources = dbt_task(dag, "dbt_test_sources", 'test --select "source:*"')
    dbt_run_stage = dbt_task(dag, "dbt_run_stage", "run --select stage")
    dbt_test_stage = dbt_task(dag, "dbt_test_stage", "test --select stage")
    dbt_run_dev = dbt_task(dag, "dbt_run_dev", "run --select dev")
    dbt_test_dev = dbt_task(dag, "dbt_test_dev", "test --select dev")
    dbt_build = dbt_task(dag, "dbt_build", "build")

    # TODO (Task 6.2): add a task that runs `dbt build`, then wire it onto the
    #   end of the chain below with `>>`.

    # Linear dependency chain.
    (
        dbt_seed
        >> dbt_test_sources
        >> dbt_run_stage
        >> dbt_test_stage
        >> dbt_run_dev
        >> dbt_test_dev
        >> dbt_build
    )
