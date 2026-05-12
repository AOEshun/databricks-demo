-- dim_currency — Standard SQL view. Currency dimension; SHA2 surrogate key.

CREATE OR REPLACE VIEW DATAMART.dim_currency AS
SELECT
  SHA2(COALESCE(order_currency, '__UNKNOWN__'), 256) AS dim_currency_key,
  order_currency
FROM INTEGRATION.order_header
GROUP BY order_currency;
