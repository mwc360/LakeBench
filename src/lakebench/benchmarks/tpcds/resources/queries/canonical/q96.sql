SELECT
  COUNT(*)
FROM store_sales
JOIN household_demographics ON ss_hdemo_sk = household_demographics.hd_demo_sk
JOIN time_dim ON ss_sold_time_sk = time_dim.t_time_sk
JOIN store ON ss_store_sk = s_store_sk
WHERE
  time_dim.t_hour = 8
  AND time_dim.t_minute >= 30
  AND household_demographics.hd_dep_count = 6
  AND store.s_store_name = 'ese'
ORDER BY
  COUNT(*)
LIMIT 100