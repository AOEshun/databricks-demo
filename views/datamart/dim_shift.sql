-- dim_shift — Standard SQL view. Shift dimension with start/end times + derived
-- shift_duration_minutes (UNIX_TIMESTAMP difference on 'HH:mm:ss' strings).

CREATE OR REPLACE VIEW DATAMART.dim_shift AS
SELECT
  SHA2(COALESCE(CAST(shift_id AS STRING), '__UNKNOWN__'), 256) AS dim_shift_key,
  shift_id,
  shift_start_time,
  shift_end_time,
  CAST(
    (UNIX_TIMESTAMP(shift_end_time, 'HH:mm:ss') - UNIX_TIMESTAMP(shift_start_time, 'HH:mm:ss')) / 60
    AS INT
  )                                                          AS shift_duration_minutes
FROM INTEGRATION.order_header
GROUP BY shift_id, shift_start_time, shift_end_time;
