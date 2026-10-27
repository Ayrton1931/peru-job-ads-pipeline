"""Gold: ad-level history and daily listing counts.

gold/ads                  one row per ad: first/last seen, snapshots, days visible
gold/daily_portal_region  ads listed and new ads per snapshot date, portal and region

An ad is right/left censored when it was already listed on the portal's first
snapshot or still listed on its last one, so its true duration is unknown.
"""
from pyspark.sql import functions as F

from common import GOLD_ADS, GOLD_DAILY, SILVER, spark_session


def main():
    spark = spark_session("gold_build")
    silver = spark.read.parquet(SILVER)

    portal_range = silver.groupBy("portal").agg(
        F.min("snapshot_date").alias("portal_first_date"),
        F.max("snapshot_date").alias("portal_last_date"),
    )

    ads = (
        silver.groupBy("portal", "ad_id")
        .agg(
            F.min("snapshot_date").alias("first_seen"),
            F.max("snapshot_date").alias("last_seen"),
            F.countDistinct("snapshot_date").alias("n_snapshots"),
            F.max_by("title", "snapshot_date").alias("title"),
            F.max_by("location_slug", "snapshot_date").alias("location_slug"),
            F.max_by("region", "snapshot_date").alias("region"),
        )
        .join(portal_range, "portal")
        .withColumn("days_visible", F.datediff("last_seen", "first_seen") + 1)
        .withColumn(
            "is_censored",
            (F.col("first_seen") == F.col("portal_first_date"))
            | (F.col("last_seen") == F.col("portal_last_date")),
        )
        .drop("portal_first_date", "portal_last_date")
    )

    daily = (
        silver.join(ads.select("portal", "ad_id", "first_seen"), ["portal", "ad_id"])
        .groupBy("snapshot_date", "snapshot_freq", "portal", "region")
        .agg(
            F.count("*").alias("ads_listed"),
            F.sum(F.when(F.col("snapshot_date") == F.col("first_seen"), 1).otherwise(0)).alias("new_ads"),
        )
    )

    ads.write.mode("overwrite").partitionBy("portal").parquet(GOLD_ADS)
    daily.write.mode("overwrite").parquet(GOLD_DAILY)

    spark.read.parquet(GOLD_ADS).groupBy("portal").agg(
        F.count("*").alias("ads"),
        F.round(F.avg(F.when(~F.col("is_censored"), F.col("days_visible"))), 1).alias("avg_days_visible_uncensored"),
    ).orderBy("portal").show()


if __name__ == "__main__":
    main()
