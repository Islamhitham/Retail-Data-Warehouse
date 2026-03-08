"""
dbt_full_refresh_dag.py
========================
Daily full-refresh of ALL dbt layers (bronze → silver → gold),
followed by a full test run.

Environment : conda env  de320
Schedule    : daily at 02:00 UTC
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator

# project paths (ON THE WINDOWS HOST) 
# Since we are running via SSH on Windows, we use local Windows paths.
# We are using the 8.3 short path to avoid space escaping issues with Conda.
DBT_PROJECT_DIR = r"c:\Users\ISLAMH~1\dbt_project"
DBT_PROFILES_DIR = r"c:\Users\ISLAMH~1\.dbt"

# Connection ID for the Windows host (Configure this in Airflow UI)
SSH_CONN_ID = "ssh_windows_host"

# Command prefix for dbt in the de320 environment on the host
DBT_EXEC = r"C:\Anacoda\envs\de320\Scripts\dbt.exe"

# Base dbt command flags
DBT_FLAGS = (
    f"--project-dir '{DBT_PROJECT_DIR}' "
    f"--profiles-dir '{DBT_PROFILES_DIR}' "
    "--no-partial-parse"
)

# default task arguments 
default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# DAG definition 
with DAG(
    dag_id="dbt_daily_full_refresh",
    description="Daily full-refresh of all dbt layers (Docker -> Windows SSH)",
    default_args=default_args,
    start_date=datetime(2026, 3, 6),
    schedule="0 2 * * *",   # 02:00 UTC every day
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "retail", "full_refresh"],
) as dag:

    # 1. Install / update dbt packages 
    dbt_deps = SSHOperator(
        task_id="dbt_deps",
        ssh_conn_id=SSH_CONN_ID,
        command=f'powershell -Command "cd \'{DBT_PROJECT_DIR}\'; & \'{DBT_EXEC}\' deps"',
    )

    # 2. Full-refresh bronze 
    dbt_run_bronze = SSHOperator(
        task_id="dbt_run_bronze",
        ssh_conn_id=SSH_CONN_ID,
        command=(
            f'powershell -Command "cd \'{DBT_PROJECT_DIR}\'; '
            f'& \'{DBT_EXEC}\' run {DBT_FLAGS} '
            f'--select bronze --full-refresh"'
        ),
    )

    # 3. Full-refresh silver 
    dbt_run_silver = SSHOperator(
        task_id="dbt_run_silver",
        ssh_conn_id=SSH_CONN_ID,
        command=(
            f'powershell -Command "cd \'{DBT_PROJECT_DIR}\'; '
            f'& \'{DBT_EXEC}\' run {DBT_FLAGS} '
            f'--select silver --full-refresh"'
        ),
    )

    # 4. Full-refresh gold 
    dbt_run_gold = SSHOperator(
        task_id="dbt_run_gold",
        ssh_conn_id=SSH_CONN_ID,
        command=(
            f'powershell -Command "cd \'{DBT_PROJECT_DIR}\'; '
            f'& \'{DBT_EXEC}\' run {DBT_FLAGS} '
            f'--select gold --full-refresh"'
        ),
    )

    # 5. Run ALL dbt tests
    dbt_test = SSHOperator(
        task_id="dbt_test",
        ssh_conn_id=SSH_CONN_ID,
        command=f'powershell -Command "cd \'{DBT_PROJECT_DIR}\'; & \'{DBT_EXEC}\' test {DBT_FLAGS}"',
    )

    #  Task ordering 
    dbt_deps >> dbt_run_bronze >> dbt_run_silver >> dbt_run_gold >> dbt_test
