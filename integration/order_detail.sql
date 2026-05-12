-- order_detail.sql — Cleansed Silver Streaming Table.
--
-- Same pattern as order_header.sql:
--   1. order_detail_clean_src     : MV with type-fixes + snake_case + drop-rule filter
--   2. order_detail               : target Streaming Table with fail + warn Expectations
--   3. APPLY CHANGES FROM SNAPSHOT: declarative MERGE

-- ============================================================================
-- Step 1 — typed + cleaned source (drop-rule filter applied here)
-- ============================================================================
CREATE OR REFRESH MATERIALIZED VIEW order_detail_clean_src
COMMENT 'Type-fixed staging snapshot of order_detail with drop-rule filter applied.'
AS
SELECT
  CAST(ORDER_DETAIL_ID            AS DECIMAL(38,0))                  AS order_detail_id,
  CAST(ORDER_ID                   AS DECIMAL(38,0))                  AS order_id,
  CAST(MENU_ITEM_ID               AS DECIMAL(38,0))                  AS menu_item_id,
  CAST(QUANTITY                   AS DECIMAL(38,4))                  AS quantity,
  CAST(UNIT_PRICE                 AS DECIMAL(38,4))                  AS unit_price,
  CAST(PRICE                      AS DECIMAL(38,4))                  AS price,
  CAST(ORDER_ITEM_DISCOUNT_AMOUNT AS DECIMAL(38,4))                  AS order_item_discount_amount,
  CAST(LINE_NUMBER                AS DECIMAL(38,0))                  AS line_number,
  _ingestion_timestamp,
  _source_system,
  _source_file,
  _last_modified,
  _pipeline_run_id
FROM ${pipeline.catalog}.STAGING_AZURESTORAGE.order_detail
-- Drop-rule filter — rows failing these go to order_detail_quarantine
WHERE ORDER_ID     IS NOT NULL
  AND MENU_ITEM_ID IS NOT NULL
  AND CAST(QUANTITY   AS DECIMAL(38,4)) > 0
  AND CAST(UNIT_PRICE AS DECIMAL(38,4)) >= 0
  AND CAST(PRICE      AS DECIMAL(38,4)) >= 0;

-- ============================================================================
-- Step 2 — target Streaming Table with fail + warn Expectations
-- ============================================================================
CREATE OR REFRESH STREAMING TABLE order_detail (
  CONSTRAINT order_detail_id_not_null EXPECT (order_detail_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT line_number_positive     EXPECT (line_number > 0)
)
COMMENT 'Cleansed order_detail: type-fixes + snake_case + drop-filter; warn/fail Expectations enforced on write.'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

-- ============================================================================
-- Step 3 — Snapshot-based MERGE
-- ============================================================================
APPLY CHANGES INTO order_detail
FROM SNAPSHOT order_detail_clean_src
KEYS (order_detail_id)
STORED AS SCD TYPE 1;
