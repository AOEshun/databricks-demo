-- order_detail_quarantine.sql — Quarantine Streaming Table mirroring order_detail.
--
-- Pattern: inverse-filter sink. Rows failing at least one drop-rule from the source
-- land here with a `failed_rules ARRAY<STRING>` column.
--
-- Triage example:
--     SELECT * FROM INTEGRATION.order_detail_quarantine
--     WHERE  array_contains(failed_rules, 'quantity_positive');

-- ============================================================================
-- Step 1 — quarantine source: inverse of the clean-source drop-filter,
--          plus `failed_rules` enumerating violations
-- ============================================================================
CREATE OR REFRESH MATERIALIZED VIEW order_detail_quarantine_src
COMMENT 'Inverse-filter source for order_detail quarantine. Rows failing >=1 drop-rule + failed_rules array.'
AS
WITH typed AS (
  SELECT
    CAST(ORDER_DETAIL_ID            AS DECIMAL(38,0))   AS order_detail_id,
    CAST(ORDER_ID                   AS DECIMAL(38,0))   AS order_id,
    CAST(MENU_ITEM_ID               AS DECIMAL(38,0))   AS menu_item_id,
    CAST(QUANTITY                   AS DECIMAL(38,4))   AS quantity,
    CAST(UNIT_PRICE                 AS DECIMAL(38,4))   AS unit_price,
    CAST(PRICE                      AS DECIMAL(38,4))   AS price,
    CAST(ORDER_ITEM_DISCOUNT_AMOUNT AS DECIMAL(38,4))   AS order_item_discount_amount,
    CAST(LINE_NUMBER                AS DECIMAL(38,0))   AS line_number,
    _ingestion_timestamp,
    _source_system,
    _source_file,
    _last_modified,
    _pipeline_run_id
  FROM ${pipeline.catalog}.STAGING_AZURESTORAGE.order_detail
)
SELECT
  *,
  ARRAY_EXCEPT(
    ARRAY(
      CASE WHEN order_id     IS NULL THEN 'order_id_not_null'       END,
      CASE WHEN menu_item_id IS NULL THEN 'menu_item_id_not_null'   END,
      CASE WHEN quantity    <= 0     THEN 'quantity_positive'       END,
      CASE WHEN unit_price   < 0     THEN 'unit_price_non_negative' END,
      CASE WHEN price        < 0     THEN 'price_non_negative'      END
    ),
    ARRAY(CAST(NULL AS STRING))
  ) AS failed_rules
FROM typed
WHERE NOT (
       order_id     IS NOT NULL
   AND menu_item_id IS NOT NULL
   AND quantity     >  0
   AND unit_price   >= 0
   AND price        >= 0
);

-- ============================================================================
-- Step 2 — target Streaming Table
-- ============================================================================
CREATE OR REFRESH STREAMING TABLE order_detail_quarantine
COMMENT 'Quarantine — order_detail rows that failed >=1 drop rule. failed_rules ARRAY<STRING> lists which.'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

-- ============================================================================
-- Step 3 — Snapshot-based MERGE from quarantine source
-- ============================================================================
APPLY CHANGES INTO order_detail_quarantine
FROM SNAPSHOT order_detail_quarantine_src
KEYS (order_detail_id)
STORED AS SCD TYPE 1;
