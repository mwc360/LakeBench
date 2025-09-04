SELECT
  SUM(ws_net_paid) AS total_sum,
  i_category,
  i_class,
  GROUPING(i_category) + GROUPING(i_class) AS lochierarchy,
  RANK() OVER (PARTITION BY GROUPING(i_category) + GROUPING(i_class), CASE WHEN GROUPING(i_class) = 0 THEN i_category END ORDER BY SUM(ws_net_paid) DESC) AS rank_within_parent
FROM web_sales
JOIN date_dim AS d1 ON d1.d_date_sk = ws_sold_date_sk
JOIN item ON i_item_sk = ws_item_sk
WHERE
  d1.d_month_seq BETWEEN 1183 AND 1183 + 11
GROUP BY
ROLLUP (
  i_category,
  i_class
)
ORDER BY
  lochierarchy DESC,
  CASE WHEN lochierarchy = 0 THEN i_category END, /* Sail does not support using GROUPING() function in ORDER BY clause directly https://github.com/lakehq/sail/issues/731 */
  rank_within_parent
LIMIT 100