-- fact_order.sql — Order-grain fact, reads DWH_ORDER_HEADER directly (ADR-0020).
-- MK_<NAAM> = WKR_ORDER_HEADER (root surrogate, stable across SCD2 version churn).
-- MK_DATE = yyyymmdd INT (ADR-0018).
-- For the current demo, SCD1 entity dims collapse versions to one row per BK,
-- so the fact reads WA_ISCURR=1 of DWH_ORDER_HEADER to pick the latest header per order_id.

CREATE OR REFRESH MATERIALIZED VIEW FCT_ORDER
COMMENT 'Order-grain fact. Direct read of DWH_ORDER_HEADER per ADR-0020. MK_<NAAM>=WKR_ORDER_HEADER for header-derived dims; MK_DATE=yyyymmdd INT.'
AS
SELECT
  -- dim FKs
  CAST(date_format(order_ts, 'yyyyMMdd') AS INT) AS MK_DATE,
  WKR_ORDER_HEADER AS MK_TRUCK,
  WKR_ORDER_HEADER AS MK_LOCATION,
  WKR_ORDER_HEADER AS MK_CUSTOMER,
  WKR_ORDER_HEADER AS MK_SHIFT,
  WKR_ORDER_HEADER AS MK_CURRENCY,
  WKR_ORDER_HEADER AS MK_ORDER_CHANNEL,
  WKR_ORDER_HEADER AS MK_DISCOUNT,
  -- degenerate dim
  order_id,
  -- measures
  order_amount,
  order_tax_amount,
  order_discount_amount,
  order_total,
  CAST(UNIX_TIMESTAMP(served_ts) - UNIX_TIMESTAMP(order_ts) AS BIGINT) AS time_to_serve_seconds
FROM ${pipeline.catalog}.INTEGRATION.DWH_ORDER_HEADER
WHERE WA_ISCURR = 1;
