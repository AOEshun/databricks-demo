-- dim_truck — Standard SQL view. Truck dimension; SHA2 surrogate key.
-- NULL truck_id (warn-rule in Silver) collapses to one shared "Unknown" member.

CREATE OR REPLACE VIEW DATAMART.dim_truck AS
SELECT
  SHA2(COALESCE(CAST(truck_id AS STRING), '__UNKNOWN__'), 256) AS dim_truck_key,
  truck_id
FROM INTEGRATION.order_header
GROUP BY truck_id;
