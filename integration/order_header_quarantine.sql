-- order_header_quarantine.sql — Quarantine Streaming Table mirroring order_header.
--
-- Pattern: inverse-filter sink. Rows failing at least one drop-rule from the source
-- land here with a `failed_rules ARRAY<STRING>` column listing every rule violated.
-- This makes triage trivial:
--     SELECT * FROM INTEGRATION.order_header_quarantine
--     WHERE  array_contains(failed_rules, 'order_total_non_negative');

-- ============================================================================
-- Step 1 — quarantine source: inverse of the clean-source drop-filter,
--          plus `failed_rules` enumerating which rules each row violated
-- ============================================================================
CREATE OR REFRESH MATERIALIZED VIEW order_header_quarantine_src
COMMENT 'Inverse-filter source for order_header quarantine. Rows failing >=1 drop-rule + failed_rules array.'
AS
WITH typed AS (
  SELECT
    CAST(ORDER_ID    AS DECIMAL(38,0))                 AS order_id,
    CAST(TRUCK_ID    AS DECIMAL(38,0))                 AS truck_id,
    CAST(LOCATION_ID AS DECIMAL(38,0))                 AS location_id,
    CAST(CUSTOMER_ID AS DECIMAL(38,0))                 AS customer_id,
    CAST(DISCOUNT_ID AS DECIMAL(38,0))                 AS discount_id,
    CAST(SHIFT_ID    AS DECIMAL(38,0))                 AS shift_id,
    CONCAT_WS(':',
      LPAD(CAST(FLOOR( SHIFT_START_TIME / 3600000) AS INT), 2, '0'),
      LPAD(CAST(FLOOR((SHIFT_START_TIME /   60000) % 60) AS INT), 2, '0'),
      LPAD(CAST(FLOOR((SHIFT_START_TIME /    1000) % 60) AS INT), 2, '0')
    )                                                  AS shift_start_time,
    CONCAT_WS(':',
      LPAD(CAST(FLOOR( SHIFT_END_TIME   / 3600000) AS INT), 2, '0'),
      LPAD(CAST(FLOOR((SHIFT_END_TIME   /   60000) % 60) AS INT), 2, '0'),
      LPAD(CAST(FLOOR((SHIFT_END_TIME   /    1000) % 60) AS INT), 2, '0')
    )                                                  AS shift_end_time,
    ORDER_CHANNEL                                      AS order_channel,
    ORDER_TS                                           AS order_ts,
    TO_TIMESTAMP(SERVED_TS, 'yyyy-MM-dd HH:mm:ss')     AS served_ts,
    ORDER_CURRENCY                                     AS order_currency,
    CAST(ORDER_AMOUNT          AS DECIMAL(38,4))       AS order_amount,
    CAST(ORDER_TAX_AMOUNT      AS DECIMAL(38,4))       AS order_tax_amount,
    CAST(ORDER_DISCOUNT_AMOUNT AS DECIMAL(38,4))       AS order_discount_amount,
    CAST(ORDER_TOTAL           AS DECIMAL(38,4))       AS order_total,
    _ingestion_timestamp,
    _source_system,
    _source_file,
    _last_modified,
    _pipeline_run_id
  FROM ${pipeline.catalog}.STAGING_AZURESTORAGE.order_header
)
SELECT
  *,
  ARRAY_EXCEPT(
    ARRAY(
      CASE WHEN order_ts       IS NULL THEN 'order_ts_not_null'         END,
      CASE WHEN customer_id    IS NULL THEN 'customer_id_not_null'      END,
      CASE WHEN order_currency IS NULL THEN 'order_currency_not_null'   END,
      CASE WHEN order_total     < 0    THEN 'order_total_non_negative'  END,
      CASE WHEN order_amount    < 0    THEN 'order_amount_non_negative' END
    ),
    ARRAY(CAST(NULL AS STRING))
  ) AS failed_rules
FROM typed
WHERE NOT (
       order_ts       IS NOT NULL
   AND customer_id    IS NOT NULL
   AND order_currency IS NOT NULL
   AND order_total    >= 0
   AND order_amount   >= 0
);

-- ============================================================================
-- Step 2 — target Streaming Table
-- ============================================================================
CREATE OR REFRESH STREAMING TABLE order_header_quarantine
COMMENT 'Quarantine — order_header rows that failed >=1 drop rule. failed_rules ARRAY<STRING> lists which.'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

-- ============================================================================
-- Step 3 — Snapshot-based MERGE from quarantine source
-- ============================================================================
APPLY CHANGES INTO order_header_quarantine
FROM SNAPSHOT order_header_quarantine_src
KEYS (order_id)
STORED AS SCD TYPE 1;
