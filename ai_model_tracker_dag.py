
"""
ai_model_tracker_dag.py

Runs the whole pipeline once a day, in order:

    extract  ->  load  ->  dbt_source_freshness  ->  dbt_build
    (API ->      (Cloud     (did today's raw         (staging, snapshot,
     Cloud        Storage    data really arrive?)     marts + all tests)
     Storage)     -> BigQuery)

If a task fails, Airflow retries it. If it still fails, the tasks after it
don't run, so bad or missing data never reaches the dashboard.

Each task runs a command with the project's own Python (.venv), so Airflow
and the project keep their packages separate and never conflict.
"""

from datetime import timedelta
from pathlib import Path

import pendulum

# Airflow 3 moved BashOperator to the "standard" provider; Airflow 2 has it
# in airflow.operators. Try the new place first, then the old one.
try:
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:
    from airflow.operators.bash import BashOperator

try:
    from airflow.sdk import DAG          # Airflow 3
except ImportError:
    from airflow import DAG              # Airflow 2

# ---------- Paths ----------
PROJECT_DIR = Path.home() / "ai-model-tracker"        # /Users/rashmi/ai-model-tracker
PYTHON = PROJECT_DIR / ".venv" / "bin" / "python"     # the project's Python
DBT = PROJECT_DIR / ".venv" / "bin" / "dbt"           # the project's dbt
DBT_DIR = PROJECT_DIR / "dbt"                          # where dbt_project.yml lives

# ---------- Settings shared by every task ----------
default_args = {
    "owner": "rashmi",
    "retries": 2,                              # try a failed task 2 more times
    "retry_delay": timedelta(minutes=5),       # wait 5 minutes between tries
    "execution_timeout": timedelta(minutes=30),  # stop a task stuck for 30+ minutes
}

with DAG(
    dag_id="ai_model_tracker",
    description="Daily OpenRouter model prices: API -> GCS -> BigQuery -> dbt",
    # 9:00 AM New York time, every day
    schedule="0 9 * * *",
    start_date=pendulum.datetime(2026, 10, 1, tz="America/New_York"),
    # Don't try to run the days before today: the API only has today's
    # data, so a "catch-up" run for an old day would just repeat today.
    catchup=False,
    max_active_runs=1,                         # never two runs at the same time
    default_args=default_args,
    tags=["gcp", "bigquery", "dbt", "portfolio"],
) as dag:

    # 1. Call the OpenRouter API and save the raw JSON to Cloud Storage.
    extract = BashOperator(
        task_id="extract_to_gcs",
        bash_command=f"cd {PROJECT_DIR} && {PYTHON} extract/extract_models.py",
    )

    # 2. Load today's raw file from Cloud Storage into the BigQuery raw table.
    load = BashOperator(
        task_id="load_to_bigquery",
        bash_command=f"cd {PROJECT_DIR} && {PYTHON} load/load_to_bigquery.py",
    )

    # 3. Check the raw table was loaded recently (the freshness rule in sources.yml).
    #    Fails the run if the newest data is more than 4 days old.
    freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f"cd {DBT_DIR} && {DBT} source freshness --profiles-dir .",
    )

    # 4. Build everything in dbt: staging, intermediate, snapshot (price
    #    history), marts, and run all the tests.
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {DBT_DIR} && {DBT} build --profiles-dir .",
    )

    # The order: each task starts only after the one before it succeeds.
    extract >> load >> freshness >> dbt_build
