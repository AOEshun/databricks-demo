-- dim_date — Standard SQL view. Date dimension covering [MIN(order_ts), MAX(order_ts)].
-- Days generated via SEQUENCE+EXPLODE so the dimension has no gaps even on days without orders.
-- SHA2(full_date) is the surrogate key.

CREATE OR REPLACE VIEW DATAMART.dim_date AS
WITH date_bounds AS (
  SELECT
    MIN(CAST(order_ts AS DATE)) AS start_date,
    MAX(CAST(order_ts AS DATE)) AS end_date
  FROM INTEGRATION.order_header
),
dates AS (
  SELECT EXPLODE(SEQUENCE(start_date, end_date, INTERVAL 1 DAY)) AS full_date
  FROM date_bounds
)
SELECT
  SHA2(CAST(full_date AS STRING), 256)                       AS dim_date_key,
  full_date,
  YEAR(full_date)                                            AS year,
  QUARTER(full_date)                                         AS quarter,
  MONTH(full_date)                                           AS month,
  DATE_FORMAT(full_date, 'MMMM')                             AS month_name,
  DAY(full_date)                                             AS day,
  DAYOFWEEK(full_date)                                       AS day_of_week,
  DATE_FORMAT(full_date, 'EEEE')                             AS day_name,
  WEEKOFYEAR(full_date)                                      AS week_of_year,
  DAYOFWEEK(full_date) IN (1, 7)                             AS is_weekend,
  CAST(DATE_TRUNC('month',   full_date) AS DATE)             AS year_month_start,
  CAST(DATE_TRUNC('quarter', full_date) AS DATE)             AS year_quarter_start,
  CAST(DATE_TRUNC('year',    full_date) AS DATE)             AS year_start
FROM dates;
