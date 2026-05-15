-- fact_sales_line.sql — Line-grain fact, reads DWH_ORDER_HEADER ⨝ DWH_ORDER_DETAIL directly (ADR-0020).
-- Half-open SCD2 interval picks the header version current at the line's order_ts.
-- MK_<NAAM>=WKR_<TABEL> root surrogate per side; MK_DATE=yyyymmdd INT.

CREATE OR REFRESH MATERIALIZED VIEW FCT_SALES_LINE
CLUSTER BY (MK_TRUCK, MK_LOCATION, MK_DATE, MK_CURRENCY)
COMMENT 'Line-grain fact. Direct DWH_ join with half-open SCD2 temporal interval. Liquid Clustering on the high-cardinality dim FKs.'
AS
SELECT
  -- dim FKs
  CAST(date_format(h.order_ts, 'yyyyMMdd') AS INT) AS MK_DATE,
  h.WKR_ORDER_HEADER AS MK_TRUCK,
  h.WKR_ORDER_HEADER AS MK_LOCATION,
  h.WKR_ORDER_HEADER AS MK_CUSTOMER,
  h.WKR_ORDER_HEADER AS MK_SHIFT,
  h.WKR_ORDER_HEADER AS MK_CURRENCY,
  h.WKR_ORDER_HEADER AS MK_ORDER_CHANNEL,
  h.WKR_ORDER_HEADER AS MK_DISCOUNT,
  d.WKR_ORDER_DETAIL AS MK_MENU_ITEM,
  -- degenerate dims
  d.order_id,
  d.order_detail_id,
  d.line_number,
  -- measures
  d.quantity,
  d.unit_price,
  d.price,
  d.order_item_discount_amount,
  CAST(d.quantity * d.unit_price AS DECIMAL(38, 4)) AS line_subtotal
FROM ${pipeline.catalog}.INTEGRATION.DWH_ORDER_DETAIL d
JOIN ${pipeline.catalog}.INTEGRATION.DWH_ORDER_HEADER h
  ON d.order_id = h.order_id
  AND h.WA_FROMDATE <= d.order_ts
  AND d.order_ts < h.WA_UNTODATE
WHERE d.WA_ISCURR = 1;
