-- dim_location — Standard SQL view. Location dimension; SHA2 surrogate key.
-- NULL location_id (warn-rule in Silver) collapses to one shared "Unknown" member.

CREATE OR REPLACE VIEW DATAMART.dim_location AS
SELECT
  SHA2(COALESCE(CAST(location_id AS STRING), '__UNKNOWN__'), 256) AS dim_location_key,
  location_id
FROM INTEGRATION.order_header
GROUP BY location_id;
