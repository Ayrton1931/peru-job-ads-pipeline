"""Bronze: land the raw link files in HDFS unchanged, plus file-level metadata.

Partitioned by portal and snapshot date, written with dynamic partition
overwrite, so re-running the job replaces partitions instead of duplicating them.
"""
from pyspark.sql import functions as F

from common import BRONZE, RAW_DIR, spark_session

FILE_RE = r"(\d{4})_(\d{1,2})_(\d{1,2})_(dia|semana)_([A-Za-z]+)_Links\.csv$"


def main():
    spark = spark_session("bronze_ingest")

    raw = spark.read.text(f"file://{RAW_DIR}/*.csv").withColumnRenamed("value", "raw_line")
    name = F.element_at(F.split(F.input_file_name(), "/"), -1)

    df = (
        raw.withColumn("source_file", name)
        .withColumn(
            "snapshot_date",
            F.make_date(
                F.regexp_extract("source_file", FILE_RE, 1).cast("int"),
                F.regexp_extract("source_file", FILE_RE, 2).cast("int"),
                F.regexp_extract("source_file", FILE_RE, 3).cast("int"),
            ),
        )
        .withColumn("snapshot_freq", F.regexp_extract("source_file", FILE_RE, 4))
        .withColumn("portal", F.lower(F.regexp_extract("source_file", FILE_RE, 5)))
        .withColumn("ingested_at", F.current_timestamp())
    )

    bad_names = df.filter(F.col("portal") == "").select("source_file").distinct().collect()
    if bad_names:
        raise SystemExit(f"file names not matching the expected pattern: {[r[0] for r in bad_names]}")

    df.write.mode("overwrite").partitionBy("portal", "snapshot_date").parquet(BRONZE)

    summary = spark.read.parquet(BRONZE).groupBy("portal").agg(
        F.countDistinct("source_file").alias("files"), F.count("*").alias("lines")
    )
    summary.orderBy("portal").show()


if __name__ == "__main__":
    main()
