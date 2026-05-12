-- dim_menu_item — Standard SQL view. Sourced from INTEGRATION.order_detail
-- (line-grain). SHA2 surrogate key.

CREATE OR REPLACE VIEW DATAMART.dim_menu_item AS
SELECT
  SHA2(COALESCE(CAST(menu_item_id AS STRING), '__UNKNOWN__'), 256) AS dim_menu_item_key,
  menu_item_id
FROM INTEGRATION.order_detail
GROUP BY menu_item_id;
