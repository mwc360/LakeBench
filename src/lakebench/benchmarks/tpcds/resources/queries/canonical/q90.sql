SELECT
  CAST(amc AS DECIMAL(15, 4)) / CAST(pmc AS DECIMAL(15, 4)) AS am_pm_ratio
FROM (
  SELECT
    COUNT(*) AS amc
  FROM web_sales
  JOIN household_demographics ON ws_ship_hdemo_sk = household_demographics.hd_demo_sk
  JOIN time_dim ON ws_sold_time_sk = time_dim.t_time_sk
  JOIN web_page ON ws_web_page_sk = web_page.wp_web_page_sk
  WHERE
    time_dim.t_hour BETWEEN 7 AND 7 + 1
    AND household_demographics.hd_dep_count = 2
    AND web_page.wp_char_count BETWEEN 5000 AND 5200
) AS at_time, (
  SELECT
    COUNT(*) AS pmc
  FROM web_sales
  JOIN household_demographics ON ws_ship_hdemo_sk = household_demographics.hd_demo_sk
  JOIN time_dim ON ws_sold_time_sk = time_dim.t_time_sk
  JOIN web_page ON ws_web_page_sk = web_page.wp_web_page_sk
  WHERE
    time_dim.t_hour BETWEEN 21 AND 21 + 1
    AND household_demographics.hd_dep_count = 2
    AND web_page.wp_char_count BETWEEN 5000 AND 5200
) AS pt_time
ORDER BY
  am_pm_ratio
LIMIT 100