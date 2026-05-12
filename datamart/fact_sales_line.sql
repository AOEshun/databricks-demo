-- fact_sales_line — Line-grain fact MV with Liquid Clustering.
--
-- The only Gold object that stays materialised: heaviest table in the model,
-- 9 SHA2 inline + clustering benefits warrant the storage cost.  Reads from
-- INTEGRATION.sales_line which is now a plain SQL view (applied by apply_views
-- before this MV refreshes) — so the join runs once per refresh, results cluster
-- on disk.
--
-- 9 SHA2 dimension FKs + 5 measures + 3 degenerate dims.

CREATE OR REFRESH MATERIALIZED VIEW fact_sales_line
CLUSTER BY (dim_truck_key, dim_location_key, dim_date_key, dim_currency_key)
COMMENT 'Line-grain fact. One row per order_detail_id from INTEGRATION.sales_line. 9 SHA2 dimension FKs + 5 measures + 3 degenerate dims. Liquid Clustering on (dim_truck_key, dim_location_key, dim_date_key, dim_currency_key).'
AS
SELECT
  -- ---------------------------------------------------------------------
  -- Foreign keys (SHA2 surrogates, computed inline to match dim definitions)
  -- ---------------------------------------------------------------------
  SHA2(CAST(CAST(order_ts AS DATE) AS STRING), 256)                  AS dim_date_key,
  SHA2(COALESCE(CAST(truck_id        AS STRING), '__UNKNOWN__'), 256) AS dim_truck_key,
  SHA2(COALESCE(CAST(location_id     AS STRING), '__UNKNOWN__'), 256) AS dim_location_key,
  SHA2(COALESCE(CAST(customer_id     AS STRING), '__UNKNOWN__'), 256) AS dim_customer_key,
  SHA2(COALESCE(CAST(menu_item_id    AS STRING), '__UNKNOWN__'), 256) AS dim_menu_item_key,
  SHA2(COALESCE(CAST(shift_id        AS STRING), '__UNKNOWN__'), 256) AS dim_shift_key,
  SHA2(COALESCE(order_currency,                  '__UNKNOWN__'), 256) AS dim_currency_key,
  SHA2(COALESCE(order_channel,                   '__UNKNOWN__'), 256) AS dim_order_channel_key,
  SHA2(COALESCE(CAST(discount_id     AS STRING), '__UNKNOWN__'), 256) AS dim_discount_key,
  -- ---------------------------------------------------------------------
  -- Degenerate dimensions
  -- ---------------------------------------------------------------------
  order_id,
  order_detail_id,
  line_number,
  -- ---------------------------------------------------------------------
  -- Event timestamps (denormalised from header)
  -- ---------------------------------------------------------------------
  order_ts,
  served_ts,
  -- ---------------------------------------------------------------------
  -- Measures
  -- ---------------------------------------------------------------------
  quantity,
  unit_price,
  price,
  order_item_discount_amount,
  CAST(quantity * unit_price AS DECIMAL(38, 4)) AS line_subtotal
FROM ${pipeline.catalog}.INTEGRATION.sales_line;
