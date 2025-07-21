SELECT
  i_item_id,
  i_item_desc,
  i_current_price
FROM item
JOIN inventory ON inv_item_sk = i_item_sk
JOIN date_dim ON d_date_sk = inv_date_sk
JOIN store_sales ON ss_item_sk = i_item_sk
WHERE
  i_current_price BETWEEN 82 AND 82 + 30
  AND d_date BETWEEN CAST('2002-07-07' AS DATE) AND (
    DATE_ADD(CAST('2002-07-07' AS DATE), 60)
  )
  AND i_manufact_id IN (718, 646, 539, 176)
  AND inv_quantity_on_hand BETWEEN 100 AND 500
GROUP BY
  i_item_id,
  i_item_desc,
  i_current_price
ORDER BY
  i_item_id
LIMIT 100