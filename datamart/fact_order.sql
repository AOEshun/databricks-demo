-- fact_order — Order-grain fact MV.
--
-- Eén rij per order_id uit INTEGRATION.order_header. 8 SHA2 dimension FKs +
-- 5 measures + 1 degenerate dim. Materialised zodat Silver-correcties
-- automatisch propageren bij elke dlt_datamart refresh, naast fact_sales_line
-- in dezelfde DLT pipeline.

CREATE OR REFRESH MATERIALIZED VIEW fact_order
COMMENT 'Order-grain fact. One row per order_id from INTEGRATION.order_header. 8 SHA2 dimension FKs + 5 measures + 1 degenerate dim.'
AS
SELECT
  -- ---------------------------------------------------------------------
  -- Foreign keys (SHA2 surrogates, computed inline to match dim definitions)
  -- ---------------------------------------------------------------------
  SHA2(CAST(CAST(order_ts AS DATE) AS STRING), 256)                   AS dim_date_key,
  SHA2(COALESCE(CAST(truck_id        AS STRING), '__UNKNOWN__'), 256) AS dim_truck_key,
  SHA2(COALESCE(CAST(location_id     AS STRING), '__UNKNOWN__'), 256) AS dim_location_key,
  SHA2(COALESCE(CAST(customer_id     AS STRING), '__UNKNOWN__'), 256) AS dim_customer_key,
  SHA2(COALESCE(CAST(shift_id        AS STRING), '__UNKNOWN__'), 256) AS dim_shift_key,
  SHA2(COALESCE(order_currency,                  '__UNKNOWN__'), 256) AS dim_currency_key,
  SHA2(COALESCE(order_channel,                   '__UNKNOWN__'), 256) AS dim_order_channel_key,
  SHA2(COALESCE(CAST(discount_id     AS STRING), '__UNKNOWN__'), 256) AS dim_discount_key,
  -- ---------------------------------------------------------------------
  -- Degenerate dimension
  -- ---------------------------------------------------------------------
  order_id,
  -- ---------------------------------------------------------------------
  -- Event timestamps
  -- ---------------------------------------------------------------------
  order_ts,
  served_ts,
  -- ---------------------------------------------------------------------
  -- Measures
  -- ---------------------------------------------------------------------
  order_amount,
  order_tax_amount,
  order_discount_amount,
  order_total,
  CAST(UNIX_TIMESTAMP(served_ts) - UNIX_TIMESTAMP(order_ts) AS BIGINT) AS time_to_serve_seconds
FROM ${pipeline.catalog}.INTEGRATION.order_header;
