-- order_detail.sql — Cleansed Silver layer for ORDER_DETAIL.
--
-- Pattern (per ADRs 0010, 0011, 0014, 0015, 0016):
--   1. order_detail_src     : tagged MV with type-fixes, WA_* admin cols, WA_HASH,
--                             failed_rules array and per-rule CONSTRAINT EXPECT tags.
--   2. DW_ORDER_DETAIL      : SCD2 Streaming Table via APPLY CHANGES STORED AS SCD TYPE 2
--                             over STREAM table_changes() (CDF), with WK_ surrogate key.
--   3. DWH_ORDER_DETAIL     : paired view exposing WA_FROMDATE / WA_UNTODATE / WA_ISCURR
--                             + WKP_ (previous) / WKR_ (first) surrogate-key navigators.
--   4. DWQ_ORDER_DETAIL     : paired quarantine Streaming Table for rows failing
--                             >=1 drop-grade rule (failed_rules surfaced).

-- ============================================================================
-- Object 1 — Tagged source MV with type-fixes, WA_* admin, WA_HASH, failed_rules
-- ============================================================================
CREATE OR REFRESH MATERIALIZED VIEW order_detail_src (
  CONSTRAINT order_id_not_null       EXPECT (NOT array_contains(failed_rules, 'order_id_not_null')),
  CONSTRAINT menu_item_id_not_null   EXPECT (NOT array_contains(failed_rules, 'menu_item_id_not_null')),
  CONSTRAINT quantity_positive       EXPECT (NOT array_contains(failed_rules, 'quantity_positive')),
  CONSTRAINT unit_price_non_negative EXPECT (NOT array_contains(failed_rules, 'unit_price_non_negative')),
  CONSTRAINT price_non_negative      EXPECT (NOT array_contains(failed_rules, 'price_non_negative'))
)
COMMENT 'Tagged source MV for ORDER_DETAIL: type-fixes + SA_*->WA_* admin + WA_HASH + failed_rules.'
AS
SELECT
  CAST(order_detail_id AS BIGINT) AS order_detail_id,
  CAST(order_id AS BIGINT) AS order_id,
  CAST(menu_item_id AS BIGINT) AS menu_item_id,
  CAST(quantity AS INT) AS quantity,
  CAST(unit_price AS DECIMAL(38,4)) AS unit_price,
  CAST(price AS DECIMAL(38,4)) AS price,
  CAST(order_item_discount_amount AS DECIMAL(38,4)) AS order_item_discount_amount,
  CAST(line_number AS INT) AS line_number,
  -- WA_* admin
  SA_CRUDDTS AS WA_CRUDDTS,
  CASE _change_type
    WHEN 'insert' THEN 'C'
    WHEN 'update_postimage' THEN 'U'
    WHEN 'delete' THEN 'D'
  END AS WA_CRUD,
  SA_SRC AS WA_SRC,
  SA_RUNID AS WA_RUNID,
  SHA2(CONCAT_WS('||',
    COALESCE(CAST(order_id AS STRING), ''),
    COALESCE(CAST(menu_item_id AS STRING), ''),
    COALESCE(CAST(quantity AS STRING), ''),
    COALESCE(CAST(unit_price AS STRING), ''),
    COALESCE(CAST(price AS STRING), ''),
    COALESCE(CAST(order_item_discount_amount AS STRING), ''),
    COALESCE(CAST(line_number AS STRING), '')
  ), 256) AS WA_HASH,
  array_compact(array(
    CASE WHEN order_id IS NULL THEN 'order_id_not_null' END,
    CASE WHEN menu_item_id IS NULL THEN 'menu_item_id_not_null' END,
    CASE WHEN quantity <= 0 THEN 'quantity_positive' END,
    CASE WHEN unit_price < 0 THEN 'unit_price_non_negative' END,
    CASE WHEN price < 0 THEN 'price_non_negative' END
  )) AS failed_rules,
  _commit_timestamp,
  _change_type
FROM STREAM table_changes('${pipeline.catalog}.STAGING_AZURESTORAGE.STG_ORDER_DETAIL', 1)
WHERE _change_type IN ('insert', 'update_postimage', 'delete');

-- ============================================================================
-- Object 2 — DW_ORDER_DETAIL: SCD2 Streaming Table via APPLY CHANGES
-- ============================================================================
CREATE OR REFRESH STREAMING TABLE DW_ORDER_DETAIL (
  WK_ORDER_DETAIL BIGINT GENERATED ALWAYS AS IDENTITY,
  order_detail_id BIGINT,
  order_id BIGINT,
  menu_item_id BIGINT,
  quantity INT,
  unit_price DECIMAL(38,4),
  price DECIMAL(38,4),
  order_item_discount_amount DECIMAL(38,4),
  line_number INT,
  WA_CRUDDTS TIMESTAMP,
  WA_CRUD STRING,
  WA_SRC STRING,
  WA_RUNID STRING,
  WA_HASH STRING,
  CONSTRAINT order_detail_id_not_null EXPECT (order_detail_id IS NOT NULL) ON VIOLATION FAIL UPDATE
)
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
COMMENT 'Cleansed ORDER_DETAIL, SCD2 via APPLY CHANGES.';

APPLY CHANGES INTO LIVE.DW_ORDER_DETAIL
FROM (
  SELECT * EXCEPT (failed_rules, _change_type)
  FROM STREAM(LIVE.order_detail_src)
  WHERE size(failed_rules) = 0
)
KEYS (order_detail_id)
SEQUENCE BY _commit_timestamp
APPLY AS DELETE WHEN WA_CRUD = 'D'
COLUMNS * EXCEPT (_commit_timestamp)
STORED AS SCD TYPE 2;

-- ============================================================================
-- Object 3 — DWH_ORDER_DETAIL: SCD2 history view with WA_FROMDATE/WA_UNTODATE/WA_ISCURR
-- ============================================================================
CREATE OR REPLACE VIEW DWH_ORDER_DETAIL AS
SELECT
  WK_ORDER_DETAIL,
  LAG(WK_ORDER_DETAIL) OVER (PARTITION BY order_detail_id ORDER BY __START_AT) AS WKP_ORDER_DETAIL,
  FIRST_VALUE(WK_ORDER_DETAIL) OVER (PARTITION BY order_detail_id ORDER BY __START_AT) AS WKR_ORDER_DETAIL,
  order_detail_id, order_id, menu_item_id, quantity, unit_price, price,
  order_item_discount_amount, line_number,
  __START_AT AS WA_FROMDATE,
  COALESCE(__END_AT, TIMESTAMP '9999-12-31 00:00:00') AS WA_UNTODATE,
  CASE WHEN __END_AT IS NULL THEN 1 ELSE 0 END AS WA_ISCURR,
  WA_CRUDDTS, WA_CRUD, WA_SRC, WA_RUNID, WA_HASH
FROM LIVE.DW_ORDER_DETAIL;

-- ============================================================================
-- Object 4 — DWQ_ORDER_DETAIL: quarantine for rows failing >=1 drop-grade rule
-- ============================================================================
CREATE OR REFRESH STREAMING TABLE DWQ_ORDER_DETAIL
COMMENT 'Quarantine for ORDER_DETAIL rows failing drop-grade rules.'
AS
SELECT
  order_detail_id, order_id, menu_item_id, quantity, unit_price, price,
  order_item_discount_amount, line_number,
  WA_CRUDDTS, WA_CRUD, WA_SRC, WA_RUNID, WA_HASH,
  failed_rules,
  _change_type
FROM STREAM(LIVE.order_detail_src)
WHERE size(failed_rules) > 0;
