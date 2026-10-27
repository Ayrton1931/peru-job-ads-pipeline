-- Staging (loaded by Spark over JDBC) and star schema (built by dw.usp_load_from_staging).
-- Idempotent: safe to run on every pipeline run.

IF SCHEMA_ID('stg') IS NULL EXEC('CREATE SCHEMA stg');
GO
IF SCHEMA_ID('dw') IS NULL EXEC('CREATE SCHEMA dw');
GO

------------------------------------------------------------------ staging
IF OBJECT_ID('stg.ad_snapshot') IS NULL
CREATE TABLE stg.ad_snapshot (
    portal          VARCHAR(20)     NOT NULL,
    ad_id           VARCHAR(32)     NOT NULL,
    snapshot_date   DATE            NOT NULL,
    snapshot_freq   VARCHAR(10)     NOT NULL,
    title           NVARCHAR(1000)  NULL,
    location_slug   VARCHAR(200)    NULL,
    region          VARCHAR(40)     NOT NULL,
    url_path        NVARCHAR(2000)  NULL,
    source_file     VARCHAR(100)    NOT NULL
);
GO
IF OBJECT_ID('stg.ad') IS NULL
CREATE TABLE stg.ad (
    portal          VARCHAR(20)     NOT NULL,
    ad_id           VARCHAR(32)     NOT NULL,
    title           NVARCHAR(1000)  NULL,
    location_slug   VARCHAR(200)    NULL,
    region          VARCHAR(40)     NOT NULL,
    first_seen      DATE            NOT NULL,
    last_seen       DATE            NOT NULL,
    n_snapshots     INT             NOT NULL,
    days_visible    INT             NOT NULL,
    is_censored     BIT             NOT NULL
);
GO
IF OBJECT_ID('stg.daily_portal_region') IS NULL
CREATE TABLE stg.daily_portal_region (
    snapshot_date   DATE            NOT NULL,
    snapshot_freq   VARCHAR(10)     NOT NULL,
    portal          VARCHAR(20)     NOT NULL,
    region          VARCHAR(40)     NOT NULL,
    ads_listed      INT             NOT NULL,
    new_ads         INT             NOT NULL
);
GO
IF OBJECT_ID('stg.file_audit') IS NULL
CREATE TABLE stg.file_audit (
    source_file      VARCHAR(100)   NOT NULL,
    portal           VARCHAR(20)    NOT NULL,
    snapshot_date    DATE           NOT NULL,
    snapshot_freq    VARCHAR(10)    NOT NULL,
    total_lines      INT            NOT NULL,
    ad_lines         INT            NOT NULL,
    navigation_lines INT            NOT NULL,
    junk_lines       INT            NOT NULL,
    unparsed_lines   INT            NOT NULL,
    duplicate_lines  INT            NOT NULL
);
GO

------------------------------------------------------------------ dimensions
IF OBJECT_ID('dw.dim_portal') IS NULL
CREATE TABLE dw.dim_portal (
    portal_key  INT IDENTITY(1,1) PRIMARY KEY,
    portal      VARCHAR(20)  NOT NULL UNIQUE,
    base_url    VARCHAR(100) NULL
);
GO
IF OBJECT_ID('dw.dim_region') IS NULL
CREATE TABLE dw.dim_region (
    region_key  INT IDENTITY(1,1) PRIMARY KEY,
    region      VARCHAR(40) NOT NULL UNIQUE,
    is_lima_callao AS CAST(CASE WHEN region IN ('lima', 'callao') THEN 1 ELSE 0 END AS BIT)
);
GO
IF OBJECT_ID('dw.dim_date') IS NULL
CREATE TABLE dw.dim_date (
    date_key     INT          PRIMARY KEY,   -- yyyymmdd
    full_date    DATE         NOT NULL UNIQUE,
    [year]       SMALLINT     NOT NULL,
    [month]      TINYINT      NOT NULL,
    [day]        TINYINT      NOT NULL,
    iso_week     TINYINT      NOT NULL,
    weekday_name VARCHAR(10)  NOT NULL,
    is_weekend   BIT          NOT NULL
);
GO
IF OBJECT_ID('dw.dim_ad') IS NULL
CREATE TABLE dw.dim_ad (
    ad_key             INT IDENTITY(1,1) PRIMARY KEY,
    portal_key         INT            NOT NULL REFERENCES dw.dim_portal(portal_key),
    ad_id              VARCHAR(32)    NOT NULL,
    title              NVARCHAR(1000) NULL,
    location_slug      VARCHAR(200)   NULL,
    region_key         INT            NOT NULL REFERENCES dw.dim_region(region_key),
    first_seen_date_key INT           NOT NULL REFERENCES dw.dim_date(date_key),
    last_seen_date_key  INT           NOT NULL REFERENCES dw.dim_date(date_key),
    n_snapshots        INT            NOT NULL,
    days_visible       INT            NOT NULL,
    is_censored        BIT            NOT NULL,
    CONSTRAINT uq_dim_ad UNIQUE (portal_key, ad_id)
);
GO

------------------------------------------------------------------ facts
IF OBJECT_ID('dw.fact_ad_snapshot') IS NULL
CREATE TABLE dw.fact_ad_snapshot (
    ad_key        INT          NOT NULL REFERENCES dw.dim_ad(ad_key),
    date_key      INT          NOT NULL REFERENCES dw.dim_date(date_key),
    snapshot_freq VARCHAR(10)  NOT NULL,
    source_file   VARCHAR(100) NOT NULL,
    CONSTRAINT pk_fact_ad_snapshot PRIMARY KEY (ad_key, date_key)
);
GO
IF OBJECT_ID('dw.fact_daily_listing') IS NULL
CREATE TABLE dw.fact_daily_listing (
    date_key      INT         NOT NULL REFERENCES dw.dim_date(date_key),
    portal_key    INT         NOT NULL REFERENCES dw.dim_portal(portal_key),
    region_key    INT         NOT NULL REFERENCES dw.dim_region(region_key),
    snapshot_freq VARCHAR(10) NOT NULL,
    ads_listed    INT         NOT NULL,
    new_ads       INT         NOT NULL,
    CONSTRAINT pk_fact_daily_listing PRIMARY KEY (date_key, portal_key, region_key)
);
GO
IF OBJECT_ID('dw.file_audit') IS NULL
CREATE TABLE dw.file_audit (
    source_file      VARCHAR(100) NOT NULL PRIMARY KEY,
    portal_key       INT          NOT NULL REFERENCES dw.dim_portal(portal_key),
    date_key         INT          NOT NULL REFERENCES dw.dim_date(date_key),
    snapshot_freq    VARCHAR(10)  NOT NULL,
    total_lines      INT          NOT NULL,
    ad_lines         INT          NOT NULL,
    navigation_lines INT          NOT NULL,
    junk_lines       INT          NOT NULL,
    unparsed_lines   INT          NOT NULL,
    duplicate_lines  INT          NOT NULL,
    loaded_at        DATETIME2    NOT NULL DEFAULT SYSUTCDATETIME()
);
GO
