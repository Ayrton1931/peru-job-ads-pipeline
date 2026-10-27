"""Load silver/gold tables from HDFS into SQL Server staging tables over JDBC.

Staging tables are created by sql/01_schema.sql; the write truncates them and
keeps their definitions. dw.usp_load_from_staging then builds the star schema.
"""
import os

from common import GOLD_ADS, GOLD_DAILY, SILVER, SILVER_AUDIT, spark_session

JDBC_URL = (
    f"jdbc:sqlserver://{os.environ.get('MSSQL_HOST', 'sqlserver')}:1433;"
    f"databaseName={os.environ.get('MSSQL_DB', 'jobads')};encrypt=true;trustServerCertificate=true;"
    "useBulkCopyForBatchInsert=true"
)

TABLES = {
    "stg.ad_snapshot": (SILVER, ["portal", "ad_id", "snapshot_date", "snapshot_freq", "title",
                                 "location_slug", "region", "url_path", "source_file"]),
    "stg.ad": (GOLD_ADS, ["portal", "ad_id", "title", "location_slug", "region", "first_seen",
                          "last_seen", "n_snapshots", "days_visible", "is_censored"]),
    "stg.daily_portal_region": (GOLD_DAILY, ["snapshot_date", "snapshot_freq", "portal", "region",
                                             "ads_listed", "new_ads"]),
    "stg.file_audit": (SILVER_AUDIT, ["source_file", "portal", "snapshot_date", "snapshot_freq",
                                      "total_lines", "ad_lines", "navigation_lines", "junk_lines",
                                      "unparsed_lines", "duplicate_lines"]),
}


def main():
    spark = spark_session("load_sqlserver")
    for table, (path, cols) in TABLES.items():
        df = spark.read.parquet(path).select(*cols)
        (
            df.write.format("jdbc")
            .option("url", JDBC_URL)
            .option("dbtable", table)
            .option("user", os.environ.get("MSSQL_USER", "sa"))
            .option("password", os.environ["MSSQL_PASSWORD"])
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
            .option("truncate", "true")
            .option("batchsize", 10000)
            .mode("overwrite")
            .save()
        )
        print(f"{table}: {df.count():,} rows")


if __name__ == "__main__":
    main()
