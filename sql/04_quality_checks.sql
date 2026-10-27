-- Post-load data quality checks. Each row: check_name, failed_rows (0 = pass), detail.
SELECT 'snapshot_rows_match_staging' AS check_name,
       ABS((SELECT COUNT_BIG(*) FROM stg.ad_snapshot) - (SELECT COUNT_BIG(*) FROM dw.fact_ad_snapshot)) AS failed_rows,
       CONCAT('stg=', (SELECT COUNT_BIG(*) FROM stg.ad_snapshot), ' dw=', (SELECT COUNT_BIG(*) FROM dw.fact_ad_snapshot)) AS detail
UNION ALL
SELECT 'ads_match_staging',
       ABS((SELECT COUNT_BIG(*) FROM stg.ad) - (SELECT COUNT_BIG(*) FROM dw.dim_ad)),
       CONCAT('stg=', (SELECT COUNT_BIG(*) FROM stg.ad), ' dw=', (SELECT COUNT_BIG(*) FROM dw.dim_ad))
UNION ALL
SELECT 'ads_without_snapshots',
       COUNT_BIG(*), 'dim_ad rows with no fact_ad_snapshot row'
FROM dw.dim_ad a
WHERE NOT EXISTS (SELECT 1 FROM dw.fact_ad_snapshot f WHERE f.ad_key = a.ad_key)
UNION ALL
SELECT 'last_seen_before_first_seen',
       COUNT_BIG(*), 'dim_ad rows with last_seen < first_seen'
FROM dw.dim_ad
WHERE last_seen_date_key < first_seen_date_key
UNION ALL
SELECT 'n_snapshots_matches_facts',
       COUNT_BIG(*), 'dim_ad.n_snapshots differs from its fact row count'
FROM dw.dim_ad a
JOIN (SELECT ad_key, COUNT(*) AS n FROM dw.fact_ad_snapshot GROUP BY ad_key) f ON f.ad_key = a.ad_key
WHERE f.n <> a.n_snapshots
UNION ALL
SELECT 'daily_listing_matches_snapshots',
       ABS((SELECT SUM(CAST(ads_listed AS BIGINT)) FROM dw.fact_daily_listing) - (SELECT COUNT_BIG(*) FROM dw.fact_ad_snapshot)),
       'sum(ads_listed) vs snapshot rows'
UNION ALL
SELECT 'files_without_ads',
       COUNT_BIG(*), 'source files that produced no ads'
FROM dw.file_audit
WHERE ad_lines = 0
UNION ALL
SELECT 'files_over_1pct_unparsed',
       COUNT_BIG(*), 'source files with more than 1% unparsed lines'
FROM dw.file_audit
WHERE unparsed_lines * 100 > total_lines
UNION ALL
SELECT 'unmapped_region_over_2pct',
       CASE WHEN 100.0 * SUM(CASE WHEN r.region = 'unmapped' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) > 2 THEN 1 ELSE 0 END,
       CONCAT('unmapped share = ',
              CAST(100.0 * SUM(CASE WHEN r.region = 'unmapped' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) AS DECIMAL(5, 2)), '%')
FROM dw.dim_ad a
JOIN dw.dim_region r ON r.region_key = a.region_key
WHERE r.region <> 'not_provided';  -- ads whose link has no location (Bumeran) cannot be mapped
