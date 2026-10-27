-- Builds the star schema from staging in one transaction.
-- Dimensions are upserted with MERGE (surrogate keys stay stable across runs);
-- facts are fully replaced, so re-running the pipeline gives the same result.

CREATE OR ALTER PROCEDURE dw.usp_load_from_staging
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRANSACTION;

    -- dim_portal
    MERGE dw.dim_portal AS t
    USING (SELECT DISTINCT portal FROM stg.ad) AS s
       ON t.portal = s.portal
    WHEN NOT MATCHED THEN
        INSERT (portal, base_url)
        VALUES (s.portal,
                CASE s.portal
                    WHEN 'computrabajo' THEN 'https://pe.computrabajo.com'
                    WHEN 'bumeran'      THEN 'https://www.bumeran.com.pe'
                    WHEN 'mipleo'       THEN 'https://www.mipleo.com.pe'
                END);

    -- dim_region
    MERGE dw.dim_region AS t
    USING (SELECT DISTINCT region FROM stg.ad) AS s
       ON t.region = s.region
    WHEN NOT MATCHED THEN INSERT (region) VALUES (s.region);

    -- dim_date: every calendar day in the observed range
    DECLARE @d0 DATE = (SELECT MIN(snapshot_date) FROM stg.ad_snapshot);
    DECLARE @d1 DATE = (SELECT MAX(snapshot_date) FROM stg.ad_snapshot);

    WITH days AS (
        SELECT @d0 AS d
        UNION ALL
        SELECT DATEADD(DAY, 1, d) FROM days WHERE d < @d1
    )
    INSERT INTO dw.dim_date (date_key, full_date, [year], [month], [day], iso_week, weekday_name, is_weekend)
    SELECT CONVERT(INT, CONVERT(CHAR(8), d, 112)), d, YEAR(d), MONTH(d), DAY(d),
           DATEPART(ISO_WEEK, d), DATENAME(WEEKDAY, d),
           CASE WHEN DATEPART(WEEKDAY, d) IN (1, 7) THEN 1 ELSE 0 END   -- with default DATEFIRST 7
    FROM days
    WHERE NOT EXISTS (SELECT 1 FROM dw.dim_date x WHERE x.full_date = days.d)
    OPTION (MAXRECURSION 0);

    -- facts reference dim_ad, so clear them before upserting it
    DELETE FROM dw.fact_ad_snapshot;
    DELETE FROM dw.fact_daily_listing;
    DELETE FROM dw.file_audit;

    -- dim_ad
    MERGE dw.dim_ad AS t
    USING (
        SELECT p.portal_key, a.ad_id, a.title, a.location_slug, r.region_key,
               CONVERT(INT, CONVERT(CHAR(8), a.first_seen, 112)) AS first_seen_date_key,
               CONVERT(INT, CONVERT(CHAR(8), a.last_seen, 112)) AS last_seen_date_key,
               a.n_snapshots, a.days_visible, a.is_censored
        FROM stg.ad a
        JOIN dw.dim_portal p ON p.portal = a.portal
        JOIN dw.dim_region r ON r.region = a.region
    ) AS s
       ON t.portal_key = s.portal_key AND t.ad_id = s.ad_id
    WHEN MATCHED THEN UPDATE SET
        title = s.title, location_slug = s.location_slug, region_key = s.region_key,
        first_seen_date_key = s.first_seen_date_key, last_seen_date_key = s.last_seen_date_key,
        n_snapshots = s.n_snapshots, days_visible = s.days_visible, is_censored = s.is_censored
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (portal_key, ad_id, title, location_slug, region_key, first_seen_date_key,
                last_seen_date_key, n_snapshots, days_visible, is_censored)
        VALUES (s.portal_key, s.ad_id, s.title, s.location_slug, s.region_key, s.first_seen_date_key,
                s.last_seen_date_key, s.n_snapshots, s.days_visible, s.is_censored)
    WHEN NOT MATCHED BY SOURCE THEN DELETE;

    -- facts
    INSERT INTO dw.fact_ad_snapshot (ad_key, date_key, snapshot_freq, source_file)
    SELECT d.ad_key, CONVERT(INT, CONVERT(CHAR(8), s.snapshot_date, 112)), s.snapshot_freq, s.source_file
    FROM stg.ad_snapshot s
    JOIN dw.dim_portal p ON p.portal = s.portal
    JOIN dw.dim_ad d     ON d.portal_key = p.portal_key AND d.ad_id = s.ad_id;

    INSERT INTO dw.fact_daily_listing (date_key, portal_key, region_key, snapshot_freq, ads_listed, new_ads)
    SELECT CONVERT(INT, CONVERT(CHAR(8), s.snapshot_date, 112)), p.portal_key, r.region_key,
           s.snapshot_freq, s.ads_listed, s.new_ads
    FROM stg.daily_portal_region s
    JOIN dw.dim_portal p ON p.portal = s.portal
    JOIN dw.dim_region r ON r.region = s.region;

    INSERT INTO dw.file_audit (source_file, portal_key, date_key, snapshot_freq, total_lines, ad_lines,
                               navigation_lines, junk_lines, unparsed_lines, duplicate_lines)
    SELECT f.source_file, p.portal_key, CONVERT(INT, CONVERT(CHAR(8), f.snapshot_date, 112)), f.snapshot_freq,
           f.total_lines, f.ad_lines, f.navigation_lines, f.junk_lines, f.unparsed_lines, f.duplicate_lines
    FROM stg.file_audit f
    JOIN dw.dim_portal p ON p.portal = f.portal;

    COMMIT TRANSACTION;
END;
GO
