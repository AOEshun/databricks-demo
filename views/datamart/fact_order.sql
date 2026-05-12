-- fact_order — Standard SQL view. Order-grain fact. One row per order_id.
-- 8 SHA2 dimension FKs (computed inline) + 5 measures + 1 degenerate dim.

CREATE OR REPLACE VIEW DATAMART.fact_order AS
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
FROM INTEGRATION.order_header;
