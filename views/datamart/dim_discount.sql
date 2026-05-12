-- dim_discount — Standard SQL view. Discount dimension; SHA2 surrogate key.
-- discount_id is nullable; orders without a discount collapse to one "Unknown" member.

CREATE OR REPLACE VIEW DATAMART.dim_discount AS
SELECT
  SHA2(COALESCE(CAST(discount_id AS STRING), '__UNKNOWN__'), 256) AS dim_discount_key,
  discount_id
FROM INTEGRATION.order_header
GROUP BY discount_id;
