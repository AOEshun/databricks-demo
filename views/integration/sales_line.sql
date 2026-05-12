-- sales_line — Integrated business view (line-grain). Standard SQL view, NOT a DLT MV.
--
-- Joins INTEGRATION.order_header ⨝ INTEGRATION.order_detail on order_id. One row per
-- order_detail_id with all order_header attributes denormalised onto each line.
--
-- Why a view (vs MV):
--   - No state, pure join — Spark optimizer can push filters down to both Silver streaming tables.
--   - Always fresh: header corrections appear immediately, no refresh lag.
--   - Quarantined rows excluded by definition (they're absent from clean Silver tables).
--
-- Catalog is resolved from the session USE CATALOG set by the apply_views orchestrator.

CREATE OR REPLACE VIEW INTEGRATION.sales_line AS
SELECT
  -- Line-grain key
  od.order_detail_id,
  od.order_id,                   -- kept from order_detail side
  -- order_detail business columns
  od.menu_item_id,
  od.quantity,
  od.unit_price,
  od.price,
  od.order_item_discount_amount,
  od.line_number,
  -- order_header business columns (denormalised onto each line)
  oh.truck_id,
  oh.location_id,
  oh.customer_id,
  oh.discount_id,
  oh.shift_id,
  oh.shift_start_time,
  oh.shift_end_time,
  oh.order_channel,
  oh.order_ts,
  oh.served_ts,
  oh.order_currency,
  oh.order_amount,
  oh.order_tax_amount,
  oh.order_discount_amount,
  oh.order_total
FROM INTEGRATION.order_detail  od
JOIN INTEGRATION.order_header  oh USING (order_id);
