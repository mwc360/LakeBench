SELECT
  i_brand_id AS brand_id,
  i_brand AS brand,
  SUM(ss_ext_sales_price) AS ext_price
FROM date_dim
JOIN store_sales ON d_date_sk = ss_sold_date_sk
JOIN item ON ss_item_sk = i_item_sk
WHERE
  i_manager_id = 17
  AND d_moy = 12
  AND d_year = 2000
GROUP BY
  i_brand,
  i_brand_id
ORDER BY
  ext_price DESC,
  i_brand_id
LIMIT 100