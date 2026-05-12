-- dim_customer — Standard SQL view. Customer dimension; SHA2 surrogate key.
-- customer_id is non-null in clean Silver (drop-rule); COALESCE remains defensive.

CREATE OR REPLACE VIEW DATAMART.dim_customer AS
SELECT
  SHA2(COALESCE(CAST(customer_id AS STRING), '__UNKNOWN__'), 256) AS dim_customer_key,
  customer_id
FROM INTEGRATION.order_header
GROUP BY customer_id;
