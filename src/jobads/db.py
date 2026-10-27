"""SQL Server helpers used by the Airflow DAG (schema setup, publish, quality checks)."""
import os
import re
import time

import pymssql

SQL_DIR = os.environ.get("SQL_DIR", "/opt/project/sql")


def connect(database=None, autocommit=True, retries=12, wait_s=10):
    """Connect to SQL Server, waiting while the container is still starting."""
    for attempt in range(retries):
        try:
            return pymssql.connect(
                server=os.environ.get("MSSQL_HOST", "sqlserver"),
                user=os.environ.get("MSSQL_USER", "sa"),
                password=os.environ["MSSQL_PASSWORD"],
                database=database or os.environ.get("MSSQL_DB", "jobads"),
                autocommit=autocommit,
            )
        except pymssql.OperationalError:
            if attempt == retries - 1:
                raise
            time.sleep(wait_s)


def split_batches(sql):
    """Split a T-SQL script on GO lines, like sqlcmd does."""
    return [b.strip() for b in re.split(r"^\s*GO\s*$", sql, flags=re.M | re.I) if b.strip()]


def run_sql_file(name, database=None):
    with open(os.path.join(SQL_DIR, name), encoding="utf-8") as fh:
        batches = split_batches(fh.read())
    conn = connect(database)
    try:
        cur = conn.cursor()
        for batch in batches:
            cur.execute(batch)
    finally:
        conn.close()


def init_schema():
    db = os.environ.get("MSSQL_DB", "jobads")
    conn = connect("master")
    try:
        conn.cursor().execute(f"IF DB_ID('{db}') IS NULL CREATE DATABASE [{db}]")
    finally:
        conn.close()
    for name in ("01_schema.sql", "02_load_procedure.sql", "03_views.sql"):
        run_sql_file(name)


def publish_star_schema():
    conn = connect()
    try:
        conn.cursor().execute("EXEC dw.usp_load_from_staging")
    finally:
        conn.close()


def run_quality_checks():
    """Each check returns (check_name, failed_rows, detail); any failed_rows > 0 fails the task."""
    with open(os.path.join(SQL_DIR, "04_quality_checks.sql"), encoding="utf-8") as fh:
        query = fh.read()
    conn = connect()
    try:
        cur = conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()
    finally:
        conn.close()
    for name, failed, detail in rows:
        print(f"{'FAIL' if failed else 'ok  '}  {name:<40} {failed:>8}  {detail}")
    failed = [r[0] for r in rows if r[1]]
    if failed:
        raise ValueError(f"data quality checks failed: {failed}")
