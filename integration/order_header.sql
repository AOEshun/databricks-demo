-- order_header.sql — Integration layer for ORDER_HEADER (ADRs 0010, 0011, 0014, 0015, 0016).
--
-- Four-object tagged-MV pipeline:
--   1. order_header_src   — tagged source MV: type-fixes, SA_* → WA_* admin, WA_HASH,
--                            failed_rules ARRAY<STRING>; per-rule CONSTRAINT EXPECT
--                            so each drop-rule's violation count surfaces in the DLT
--                            event log (ADR-0011).
--   2. DW_ORDER_HEADER    — cleansed SCD2 streaming table populated via
--                            FLOW AUTO CDC … STORED AS SCD TYPE 2 from the
--                            tagged MV filtered to size(failed_rules)=0 (ADR-0010).
--   3. DWH_ORDER_HEADER   — view exposing every DW version with renamed validity
--                            columns (WA_FROMDATE/WA_UNTODATE/WA_ISCURR) and
--                            window-derived WKP_/WKR_ surrogates (ADR-0013).
--                            No self-side BK hash column (ADR-0014).
--   4. DWQ_ORDER_HEADER   — append-only quarantine streaming table populated from
--                            the same tagged MV filtered to size(failed_rules)>0;
--                            carries failed_rules + raw _change_type (ADR-0011).

-- ============================================================================
-- Object 1 — Tagged source MV (rule logic + admin columns live here)
-- ============================================================================
CREATE OR REFRESH MATERIALIZED VIEW order_header_src (
  CONSTRAINT order_ts_not_null         EXPECT (NOT array_contains(failed_rules, 'order_ts_not_null')),
  CONSTRAINT customer_id_not_null      EXPECT (NOT array_contains(failed_rules, 'customer_id_not_null')),
  CONSTRAINT order_currency_not_null   EXPECT (NOT array_contains(failed_rules, 'order_currency_not_null')),
  CONSTRAINT order_total_non_negative  EXPECT (NOT array_contains(failed_rules, 'order_total_non_negative')),
  CONSTRAINT order_amount_non_negative EXPECT (NOT array_contains(failed_rules, 'order_amount_non_negative'))
)
COMMENT 'Tagged source MV for ORDER_HEADER: type-fixes + SA_* → WA_* admin + WA_HASH + failed_rules. Feeds DW_ORDER_HEADER (cleansed) and DWQ_ORDER_HEADER (rejected).'
AS
SELECT
  -- BK
  CAST(ORDER_ID AS BIGINT) AS order_id,
  -- Business columns (typed)
  CAST(TRUCK_ID    AS INT)    AS truck_id,
  CAST(LOCATION_ID AS INT)    AS location_id,
  CAST(CUSTOMER_ID AS BIGINT) AS customer_id,
  CAST(DISCOUNT_ID AS INT)    AS discount_id,
  CAST(SHIFT_ID    AS INT)    AS shift_id,
  -- Shift times: millis-since-midnight → 'HH:mm:ss' (timezone-safe)
  CONCAT_WS(':',
    LPAD(CAST(FLOOR( SHIFT_START_TIME / 3600000) AS STRING), 2, '0'),
    LPAD(CAST(FLOOR((SHIFT_START_TIME %  3600000) / 60000) AS STRING), 2, '0'),
    LPAD(CAST(FLOOR((SHIFT_START_TIME %    60000) /  1000) AS STRING), 2, '0')
  ) AS shift_start_time,
  CONCAT_WS(':',
    LPAD(CAST(FLOOR( SHIFT_END_TIME   / 3600000) AS STRING), 2, '0'),
    LPAD(CAST(FLOOR((SHIFT_END_TIME   %  3600000) / 60000) AS STRING), 2, '0'),
    LPAD(CAST(FLOOR((SHIFT_END_TIME   %    60000) /  1000) AS STRING), 2, '0')
  ) AS shift_end_time,
  CAST(ORDER_CHANNEL          AS STRING)         AS order_channel,
  CAST(ORDER_TS               AS TIMESTAMP)      AS order_ts,
  CAST(SERVED_TS              AS TIMESTAMP)      AS served_ts,
  CAST(ORDER_CURRENCY         AS STRING)         AS order_currency,
  CAST(ORDER_AMOUNT           AS DECIMAL(38,4))  AS order_amount,
  CAST(ORDER_TAX_AMOUNT       AS DECIMAL(38,4))  AS order_tax_amount,
  CAST(ORDER_DISCOUNT_AMOUNT  AS DECIMAL(38,4))  AS order_discount_amount,
  CAST(ORDER_TOTAL            AS DECIMAL(38,4))  AS order_total,
  -- WA_* admin (ADR-0017: SA_* → WA_* mapping; WA_CRUD derived from CDF _change_type)
  SA_CRUDDTS AS WA_CRUDDTS,
  CASE _change_type
    WHEN 'insert'           THEN 'C'
    WHEN 'update_postimage' THEN 'U'
    WHEN 'delete'           THEN 'D'
  END           AS WA_CRUD,
  SA_SRC        AS WA_SRC,
  SA_RUNID      AS WA_RUNID,
  -- Row content signature over all non-BK business columns (ADRs 0015, 0019)
  SHA2(CONCAT_WS('||',
    COALESCE(CAST(TRUCK_ID              AS STRING), ''),
    COALESCE(CAST(LOCATION_ID           AS STRING), ''),
    COALESCE(CAST(CUSTOMER_ID           AS STRING), ''),
    COALESCE(CAST(DISCOUNT_ID           AS STRING), ''),
    COALESCE(CAST(SHIFT_ID              AS STRING), ''),
    COALESCE(CAST(SHIFT_START_TIME      AS STRING), ''),
    COALESCE(CAST(SHIFT_END_TIME        AS STRING), ''),
    COALESCE(CAST(ORDER_CHANNEL         AS STRING), ''),
    COALESCE(CAST(ORDER_TS              AS STRING), ''),
    COALESCE(CAST(SERVED_TS             AS STRING), ''),
    COALESCE(CAST(ORDER_CURRENCY        AS STRING), ''),
    COALESCE(CAST(ORDER_AMOUNT          AS STRING), ''),
    COALESCE(CAST(ORDER_TAX_AMOUNT      AS STRING), ''),
    COALESCE(CAST(ORDER_DISCOUNT_AMOUNT AS STRING), ''),
    COALESCE(CAST(ORDER_TOTAL           AS STRING), '')
  ), 256) AS WA_HASH,
  -- failed_rules — drop-grade rule violations, compact non-null elements only
  array_compact(array(
    CASE WHEN ORDER_TS       IS NULL THEN 'order_ts_not_null'         END,
    CASE WHEN CUSTOMER_ID    IS NULL THEN 'customer_id_not_null'      END,
    CASE WHEN ORDER_CURRENCY IS NULL THEN 'order_currency_not_null'   END,
    CASE WHEN ORDER_TOTAL    <  0    THEN 'order_total_non_negative'  END,
    CASE WHEN ORDER_AMOUNT   <  0    THEN 'order_amount_non_negative' END
  )) AS failed_rules,
  -- CDF metadata (pass-through for downstream FLOW AUTO CDC SEQUENCE BY + DWQ diagnostic)
  _commit_timestamp,
  _change_type
FROM STREAM table_changes('${pipeline.catalog}.STAGING_AZURESTORAGE.STG_ORDER_HEADER', 1)
WHERE _change_type IN ('insert', 'update_postimage', 'delete');

-- ============================================================================
-- Object 2 — DW_ORDER_HEADER streaming table (cleansed, SCD2)
-- ============================================================================
CREATE OR REFRESH STREAMING TABLE DW_ORDER_HEADER (
  WK_ORDER_HEADER       BIGINT GENERATED ALWAYS AS IDENTITY,
  order_id              BIGINT,
  truck_id              INT,
  location_id           INT,
  customer_id           BIGINT,
  discount_id           INT,
  shift_id              INT,
  shift_start_time      STRING,
  shift_end_time        STRING,
  order_channel         STRING,
  order_ts              TIMESTAMP,
  served_ts             TIMESTAMP,
  order_currency        STRING,
  order_amount          DECIMAL(38,4),
  order_tax_amount      DECIMAL(38,4),
  order_discount_amount DECIMAL(38,4),
  order_total           DECIMAL(38,4),
  WA_CRUDDTS            TIMESTAMP,
  WA_CRUD               STRING,
  WA_SRC                STRING,
  WA_RUNID              STRING,
  WA_HASH               STRING,
  CONSTRAINT order_id_not_null EXPECT (order_id IS NOT NULL) ON VIOLATION FAIL UPDATE
)
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
COMMENT 'Cleansed ORDER_HEADER, SCD2 via AUTO CDC from order_header_src (where size(failed_rules)=0).'
FLOW AUTO CDC
FROM (
  SELECT * EXCEPT (failed_rules, _change_type)
  FROM STREAM(${pipeline.catalog}.INTEGRATION.order_header_src)
  WHERE size(failed_rules) = 0
)
KEYS (order_id)
SEQUENCE BY _commit_timestamp
APPLY AS DELETE WHEN WA_CRUD = 'D'
COLUMNS * EXCEPT (_commit_timestamp)
STORED AS SCD TYPE 2;

-- ============================================================================
-- Object 3 — DWH_ORDER_HEADER view (renamed validity columns + WKP_/WKR_)
-- ============================================================================
CREATE OR REPLACE VIEW DWH_ORDER_HEADER AS
SELECT
  WK_ORDER_HEADER,
  LAG(WK_ORDER_HEADER)         OVER (PARTITION BY order_id ORDER BY __START_AT) AS WKP_ORDER_HEADER,
  FIRST_VALUE(WK_ORDER_HEADER) OVER (PARTITION BY order_id ORDER BY __START_AT) AS WKR_ORDER_HEADER,
  order_id,
  truck_id,
  location_id,
  customer_id,
  discount_id,
  shift_id,
  shift_start_time,
  shift_end_time,
  order_channel,
  order_ts,
  served_ts,
  order_currency,
  order_amount,
  order_tax_amount,
  order_discount_amount,
  order_total,
  __START_AT                                              AS WA_FROMDATE,
  COALESCE(__END_AT, TIMESTAMP '9999-12-31 00:00:00')     AS WA_UNTODATE,
  CASE WHEN __END_AT IS NULL THEN 1 ELSE 0 END            AS WA_ISCURR,
  WA_CRUDDTS,
  WA_CRUD,
  WA_SRC,
  WA_RUNID,
  WA_HASH
FROM ${pipeline.catalog}.INTEGRATION.DW_ORDER_HEADER;

-- ============================================================================
-- Object 4 — DWQ_ORDER_HEADER streaming table (quarantine, append-only)
-- ============================================================================
CREATE OR REFRESH STREAMING TABLE DWQ_ORDER_HEADER
COMMENT 'Quarantine for ORDER_HEADER rows failing drop-grade rules. failed_rules carries violated rule names.'
AS
SELECT
  order_id,
  truck_id,
  location_id,
  customer_id,
  discount_id,
  shift_id,
  shift_start_time,
  shift_end_time,
  order_channel,
  order_ts,
  served_ts,
  order_currency,
  order_amount,
  order_tax_amount,
  order_discount_amount,
  order_total,
  WA_CRUDDTS,
  WA_CRUD,
  WA_SRC,
  WA_RUNID,
  WA_HASH,
  failed_rules,
  _change_type
FROM STREAM(${pipeline.catalog}.INTEGRATION.order_header_src)
WHERE size(failed_rules) > 0;
