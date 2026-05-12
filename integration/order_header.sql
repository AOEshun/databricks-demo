-- order_header.sql — Cleansed Silver Streaming Table.
--
-- Pattern:
--   1. order_header_clean_src     : MV with type-fixes + snake_case + drop-rule filter
--   2. order_header               : target Streaming Table with fail + warn Expectations
--   3. APPLY CHANGES FROM SNAPSHOT: declarative MERGE handling both full-overwrite
--                                   and incremental-append modes in Bronze
--
-- Reading from a snapshot (vs CDF) means we don't depend on _change_type metadata;
-- DLT computes the diff between current and previous Bronze snapshots automatically.

-- ============================================================================
-- Step 1 — typed + cleaned source (drop-rule filter applied here)
-- ============================================================================
CREATE OR REFRESH MATERIALIZED VIEW order_header_clean_src
COMMENT 'Type-fixed staging snapshot of order_header with drop-rule filter applied. Source for APPLY CHANGES FROM SNAPSHOT.'
AS
SELECT
  CAST(ORDER_ID    AS DECIMAL(38,0))                                 AS order_id,
  CAST(TRUCK_ID    AS DECIMAL(38,0))                                 AS truck_id,
  CAST(LOCATION_ID AS DECIMAL(38,0))                                 AS location_id,
  CAST(CUSTOMER_ID AS DECIMAL(38,0))                                 AS customer_id,
  CAST(DISCOUNT_ID AS DECIMAL(38,0))                                 AS discount_id,
  CAST(SHIFT_ID    AS DECIMAL(38,0))                                 AS shift_id,
  -- millis-from-midnight → 'HH:mm:ss' string (timezone-safe, no TIMESTAMP_MILLIS)
  CONCAT_WS(':',
    LPAD(CAST(FLOOR( SHIFT_START_TIME / 3600000) AS INT), 2, '0'),
    LPAD(CAST(FLOOR((SHIFT_START_TIME /   60000) % 60) AS INT), 2, '0'),
    LPAD(CAST(FLOOR((SHIFT_START_TIME /    1000) % 60) AS INT), 2, '0')
  )                                                                  AS shift_start_time,
  CONCAT_WS(':',
    LPAD(CAST(FLOOR( SHIFT_END_TIME   / 3600000) AS INT), 2, '0'),
    LPAD(CAST(FLOOR((SHIFT_END_TIME   /   60000) % 60) AS INT), 2, '0'),
    LPAD(CAST(FLOOR((SHIFT_END_TIME   /    1000) % 60) AS INT), 2, '0')
  )                                                                  AS shift_end_time,
  ORDER_CHANNEL                                                      AS order_channel,
  ORDER_TS                                                           AS order_ts,
  TO_TIMESTAMP(SERVED_TS, 'yyyy-MM-dd HH:mm:ss')                     AS served_ts,
  ORDER_CURRENCY                                                     AS order_currency,
  CAST(ORDER_AMOUNT          AS DECIMAL(38,4))                       AS order_amount,
  CAST(ORDER_TAX_AMOUNT      AS DECIMAL(38,4))                       AS order_tax_amount,
  CAST(ORDER_DISCOUNT_AMOUNT AS DECIMAL(38,4))                       AS order_discount_amount,
  CAST(ORDER_TOTAL           AS DECIMAL(38,4))                       AS order_total,
  _ingestion_timestamp,
  _source_system,
  _source_file,
  _last_modified,
  _pipeline_run_id
FROM ${pipeline.catalog}.STAGING_AZURESTORAGE.order_header
-- Drop-rule filter — rows failing these go to order_header_quarantine
WHERE ORDER_TS       IS NOT NULL
  AND CUSTOMER_ID    IS NOT NULL
  AND ORDER_CURRENCY IS NOT NULL
  AND CAST(ORDER_TOTAL  AS DECIMAL(38,4)) >= 0
  AND CAST(ORDER_AMOUNT AS DECIMAL(38,4)) >= 0;

-- ============================================================================
-- Step 2 — target Streaming Table with fail + warn Expectations
-- ============================================================================
CREATE OR REFRESH STREAMING TABLE order_header (
  -- fail rule: pipeline halts on violation
  CONSTRAINT order_id_not_null      EXPECT (order_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  -- warn rules: rows stay, violation count appears in DLT event log
  CONSTRAINT truck_id_not_null      EXPECT (truck_id IS NOT NULL),
  CONSTRAINT location_id_not_null   EXPECT (location_id IS NOT NULL),
  CONSTRAINT shift_times_ordered    EXPECT (shift_start_time <= shift_end_time)
)
COMMENT 'Cleansed order_header: type-fixes + snake_case + drop-filter; warn/fail Expectations enforced on write.'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

-- ============================================================================
-- Step 3 — Snapshot-based MERGE (handles full overwrites + incremental appends)
-- ============================================================================
APPLY CHANGES INTO order_header
FROM SNAPSHOT order_header_clean_src
KEYS (order_id)
STORED AS SCD TYPE 1;
