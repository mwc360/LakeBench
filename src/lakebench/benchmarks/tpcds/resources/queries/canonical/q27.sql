SELECT
  i_item_id,
  s_state,
  GROUPING(s_state) AS g_state,
  AVG(ss_quantity) AS agg1,
  AVG(ss_list_price) AS agg2,
  AVG(ss_coupon_amt) AS agg3,
  AVG(ss_sales_price) AS agg4
FROM store_sales
JOIN date_dim ON ss_sold_date_sk = d_date_sk
JOIN item ON ss_item_sk = i_item_sk
JOIN store ON ss_store_sk = s_store_sk
JOIN customer_demographics ON ss_cdemo_sk = cd_demo_sk
WHERE
  cd_gender = 'M'
  AND cd_marital_status = 'S'
  AND cd_education_status = '2 yr Degree'
  AND d_year = 1999
  AND s_state IN ('AL', 'MI', 'NM', 'NY', 'LA', 'GA')
GROUP BY
ROLLUP (
  i_item_id,
  s_state
)
ORDER BY
  i_item_id,
  s_state
LIMIT 100