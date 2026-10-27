"""Paths and Spark session shared by the Spark jobs."""
import os

from pyspark.sql import SparkSession

LAKE = os.environ.get("LAKE_ROOT", "hdfs://namenode:9000/lake")
RAW_DIR = os.environ.get("RAW_DIR", "/tmp/raw_extracted")
SEEDS_DIR = os.environ.get("SEEDS_DIR", "/opt/project/seeds")

BRONZE = f"{LAKE}/bronze/job_links"
SILVER = f"{LAKE}/silver/ad_snapshots"
SILVER_AUDIT = f"{LAKE}/silver/file_audit"
GOLD_ADS = f"{LAKE}/gold/ads"
GOLD_DAILY = f"{LAKE}/gold/daily_portal_region"


def spark_session(app_name):
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "America/Lima")
        .getOrCreate()
    )
