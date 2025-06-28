SELECT
  i_product_name,
  i_brand,
  i_class,
  i_category,
  AVG(CAST(inv_quantity_on_hand AS BIGINT)) AS qoh
FROM inventory
JOIN date_dim ON inv_date_sk = d_date_sk
JOIN item ON inv_item_sk = i_item_sk
WHERE d_month_seq BETWEEN 1183 AND 1183 + 11
GROUP BY
ROLLUP (
  i_product_name,
  i_brand,
  i_class,
  i_category
)
ORDER BY
  qoh,
  i_product_name,
  i_brand,
  i_class,
  i_category
LIMIT 100
