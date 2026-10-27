"""Silver: parse each link into an ad record, map locations to regions, deduplicate.

Outputs:
  silver/ad_snapshots  one row per (portal, ad_id, snapshot_date)
  silver/file_audit    line counts per source file (ads, navigation, junk, unparsed, duplicates)

Fails the run if parsing or region mapping quality drops below the thresholds.
"""
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from common import BRONZE, SEEDS_DIR, SILVER, SILVER_AUDIT, spark_session
from jobads.parsers import link_kind, parse_link

MAX_UNPARSED_SHARE = 0.005
MAX_UNMAPPED_REGION_SHARE = 0.02

PARSED = T.StructType(
    [
        T.StructField("kind", T.StringType()),
        T.StructField("ad_id", T.StringType()),
        T.StructField("title", T.StringType()),
        T.StructField("location_slug", T.StringType()),
        T.StructField("region_slug", T.StringType()),
        T.StructField("url_path", T.StringType()),
    ]
)


@F.udf(PARSED)
def parse_udf(portal, raw_line):
    kind = link_kind(raw_line)
    if kind != "candidate":
        return (kind, None, None, None, None, None)
    rec = parse_link(portal, raw_line)
    if rec is None:
        return ("unparsed", None, None, None, None, None)
    return ("ad", rec["ad_id"], rec["title"], rec["location_slug"], rec["region_slug"], rec["url_path"])


def main():
    spark = spark_session("silver_transform")

    bronze = spark.read.parquet(BRONZE)
    parsed = bronze.withColumn("p", parse_udf("portal", "raw_line")).select(
        "source_file", "portal", "snapshot_date", "snapshot_freq", "p.*"
    ).cache()

    # Same ad listed twice in one snapshot (overlapping result pages): keep one row.
    w = Window.partitionBy("portal", "ad_id", "snapshot_date").orderBy("source_file", "url_path")
    ads = (
        parsed.filter(F.col("kind") == "ad")
        .withColumn("rn", F.row_number().over(w))
    )

    audit = (
        parsed.join(
            ads.filter("rn > 1").groupBy("source_file").agg(F.count("*").alias("duplicate_lines")),
            "source_file",
            "left",
        )
        .groupBy("source_file", "portal", "snapshot_date", "snapshot_freq")
        .agg(
            F.count("*").alias("total_lines"),
            F.sum(F.when(F.col("kind") == "ad", 1).otherwise(0)).alias("ad_lines"),
            F.sum(F.when(F.col("kind") == "navigation", 1).otherwise(0)).alias("navigation_lines"),
            F.sum(F.when(F.col("kind") == "junk", 1).otherwise(0)).alias("junk_lines"),
            F.sum(F.when(F.col("kind") == "unparsed", 1).otherwise(0)).alias("unparsed_lines"),
            F.coalesce(F.max("duplicate_lines"), F.lit(0)).alias("duplicate_lines"),
        )
    )

    regions = spark.read.option("header", True).csv(f"file://{SEEDS_DIR}/location_region.csv")
    silver = (
        ads.filter("rn = 1")
        .withColumn("location_key", F.coalesce("location_slug", "region_slug"))
        .join(F.broadcast(regions.withColumnRenamed("location_slug", "location_key")), "location_key", "left")
        .withColumn("region", F.coalesce("region", F.lit("unmapped")))
        .select(
            "portal", "ad_id", "snapshot_date", "snapshot_freq", "title",
            "location_slug", "region", "url_path", "source_file",
        )
    )

    silver.write.mode("overwrite").partitionBy("portal").parquet(SILVER)
    audit.write.mode("overwrite").parquet(SILVER_AUDIT)

    # Quality gate (computed from what was written).
    a = spark.read.parquet(SILVER_AUDIT).agg(
        F.sum("total_lines").alias("total"),
        F.sum("ad_lines").alias("ads"),
        F.sum("unparsed_lines").alias("unparsed"),
        F.sum(F.when(F.col("ad_lines") == 0, 1).otherwise(0)).alias("files_without_ads"),
    ).first()
    s = spark.read.parquet(SILVER)
    unmapped = s.filter(F.col("region") == "unmapped").count()
    rows = s.count()

    print(f"lines={a.total} ad_lines={a.ads} unparsed={a.unparsed} silver_rows={rows} "
          f"unmapped_region={unmapped} files_without_ads={a.files_without_ads}")
    s.groupBy("portal").agg(F.count("*").alias("rows"), F.countDistinct("ad_id").alias("unique_ads")).show()

    errors = []
    if a.unparsed / max(a.total, 1) > MAX_UNPARSED_SHARE:
        errors.append(f"unparsed share {a.unparsed / a.total:.3%} > {MAX_UNPARSED_SHARE:.1%}")
    if a.files_without_ads:
        errors.append(f"{a.files_without_ads} files produced no ads")
    if unmapped / max(rows, 1) > MAX_UNMAPPED_REGION_SHARE:
        errors.append(f"unmapped region share {unmapped / rows:.3%} > {MAX_UNMAPPED_REGION_SHARE:.1%}")
    if errors:
        raise SystemExit("silver quality gate failed: " + "; ".join(errors))


if __name__ == "__main__":
    main()
