"""Job-ads pipeline: raw link files -> HDFS (bronze/silver/gold) -> SQL Server star schema.

extract_raw_files -> bronze_ingest -> silver_transform -> gold_build -> load_staging -> publish -> quality_checks
                                                     sqlserver_init_schema --^

Every task is idempotent (partition overwrite in HDFS, truncate-load in staging,
MERGE + full fact reload in SQL Server), so any run or retry can be repeated safely.
"""
import glob
import os
import shutil
import zipfile
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from jobads import db

PROJECT = "/opt/project"
RAW_ZIP_DIR = os.environ.get("RAW_ZIP_DIR", f"{PROJECT}/data/raw")
RAW_DIR = os.environ.get("RAW_DIR", "/tmp/raw_extracted")

SPARK_SUBMIT = "spark-submit --master local[2] --driver-memory 2g --conf spark.ui.showConsoleProgress=false"
MSSQL_JDBC = "com.microsoft.sqlserver:mssql-jdbc:12.6.1.jre11"


def extract_raw_files():
    """Unzip the raw CSV archives into a clean working folder."""
    shutil.rmtree(RAW_DIR, ignore_errors=True)
    os.makedirs(RAW_DIR)
    archives = sorted(glob.glob(f"{RAW_ZIP_DIR}/*.zip"))
    if not archives:
        raise FileNotFoundError(f"no .zip archives in {RAW_ZIP_DIR}")
    for path in archives:
        with zipfile.ZipFile(path) as zf:
            zf.extractall(RAW_DIR)
    n = len(glob.glob(f"{RAW_DIR}/*.csv"))
    print(f"extracted {n} csv files from {len(archives)} archive(s)")


def spark_task(task_id, script, extra=""):
    return BashOperator(
        task_id=task_id,
        bash_command=f"{SPARK_SUBMIT} {extra} {PROJECT}/spark_jobs/{script}",
    )


with DAG(
    dag_id="job_ads_pipeline",
    description="Peruvian job portals: link dumps -> HDFS lake -> SQL Server star schema",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(seconds=30)},
    tags=["etl", "spark", "hdfs", "sqlserver"],
) as dag:
    extract = PythonOperator(task_id="extract_raw_files", python_callable=extract_raw_files)
    bronze = spark_task("bronze_ingest", "bronze_ingest.py")
    silver = spark_task("silver_transform", "silver_transform.py")
    gold = spark_task("gold_build", "gold_build.py")
    init_db = PythonOperator(task_id="sqlserver_init_schema", python_callable=db.init_schema)
    load = spark_task("load_staging", "load_sqlserver.py", extra=f"--packages {MSSQL_JDBC}")
    publish = PythonOperator(task_id="publish_star_schema", python_callable=db.publish_star_schema)
    checks = PythonOperator(task_id="quality_checks", python_callable=db.run_quality_checks)

    extract >> bronze >> silver >> gold >> load
    init_db >> load
    load >> publish >> checks
