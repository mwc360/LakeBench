SELECT
  dt.d_year,
  item.i_brand_id AS brand_id,
  item.i_brand AS brand,
  SUM(ss_ext_sales_price) AS ext_price
FROM date_dim AS dt
JOIN store_sales ON dt.d_date_sk = store_sales.ss_sold_date_sk
JOIN item ON store_sales.ss_item_sk = item.i_item_sk
WHERE
  item.i_manager_id = 1
  AND dt.d_moy = 12
  AND dt.d_year = 1999
GROUP BY
  dt.d_year,
  item.i_brand,
  item.i_brand_id
ORDER BY
  dt.d_year,
  ext_price DESC,
  brand_id
LIMIT 100