-- dim_order_channel — Standard SQL view. Order channel dimension; SHA2 surrogate key.

CREATE OR REPLACE VIEW DATAMART.dim_order_channel AS
SELECT
  SHA2(COALESCE(order_channel, '__UNKNOWN__'), 256) AS dim_order_channel_key,
  order_channel
FROM INTEGRATION.order_header
GROUP BY order_channel;
