-- Reporting views on top of the star schema.

-- New ads per month, portal and region.
-- A portal's first snapshot is excluded: every ad on it would count as "new".
CREATE OR ALTER VIEW dw.v_new_ads_monthly AS
SELECT d.[year], d.[month], p.portal, r.region,
       SUM(f.new_ads) AS new_ads
FROM dw.fact_daily_listing f
JOIN dw.dim_date   d ON d.date_key   = f.date_key
JOIN dw.dim_portal p ON p.portal_key = f.portal_key
JOIN dw.dim_region r ON r.region_key = f.region_key
WHERE f.date_key > (SELECT MIN(x.date_key) FROM dw.fact_daily_listing x WHERE x.portal_key = f.portal_key)
GROUP BY d.[year], d.[month], p.portal, r.region;
GO

-- How long ads stay posted, by portal (only ads whose start and end were both observed).
CREATE OR ALTER VIEW dw.v_ad_duration_by_portal AS
SELECT p.portal,
       COUNT(*)                                   AS ads,
       AVG(CAST(a.days_visible AS DECIMAL(9, 2))) AS avg_days_visible,
       MIN(a.days_visible)                        AS min_days,
       MAX(a.days_visible)                        AS max_days
FROM dw.dim_ad a
JOIN dw.dim_portal p ON p.portal_key = a.portal_key
WHERE a.is_censored = 0
GROUP BY p.portal;
GO

-- Regional share of ads per portal.
CREATE OR ALTER VIEW dw.v_region_share AS
SELECT p.portal, r.region,
       COUNT(*) AS ads,
       CAST(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY p.portal) AS DECIMAL(5, 2)) AS pct_of_portal
FROM dw.dim_ad a
JOIN dw.dim_portal p ON p.portal_key = a.portal_key
JOIN dw.dim_region r ON r.region_key = a.region_key
GROUP BY p.portal, r.region;
GO

-- Most frequent job titles per portal (exact title match).
CREATE OR ALTER VIEW dw.v_top_titles AS
SELECT portal, title, ads, title_rank
FROM (
    SELECT p.portal, a.title, COUNT(*) AS ads,
           ROW_NUMBER() OVER (PARTITION BY p.portal ORDER BY COUNT(*) DESC) AS title_rank
    FROM dw.dim_ad a
    JOIN dw.dim_portal p ON p.portal_key = a.portal_key
    GROUP BY p.portal, a.title
) t
WHERE title_rank <= 25;
GO
