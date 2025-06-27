SELECT
  SUM(ss_net_profit) / SUM(ss_ext_sales_price) AS gross_margin,
  i_category,
  i_class,
  GROUPING(i_category) + GROUPING(i_class) AS lochierarchy,
  RANK() OVER (PARTITION BY GROUPING(i_category) + GROUPING(i_class), CASE WHEN GROUPING(i_class) = 0 THEN i_category END ORDER BY SUM(ss_net_profit) / SUM(ss_ext_sales_price) ASC) AS rank_within_parent
FROM store_sales, date_dim AS d1, item, store
WHERE
  d1.d_year = 2002
  AND d1.d_date_sk = ss_sold_date_sk
  AND i_item_sk = ss_item_sk
  AND s_store_sk = ss_store_sk
  AND s_state IN ('AL', 'MI', 'NM', 'NY', 'LA', 'GA', 'PA', 'MN')
GROUP BY
ROLLUP (
  i_category,
  i_class
)
ORDER BY
  lochierarchy DESC,
  CASE WHEN GROUPING(i_category) + GROUPING(i_class) = 0 THEN i_category END,
  rank_within_parent
LIMIT 100