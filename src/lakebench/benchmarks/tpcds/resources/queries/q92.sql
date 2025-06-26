SELECT
  SUM(ws_ext_discount_amt) AS `Excess Discount Amount`
FROM web_sales, item, date_dim
WHERE
  i_manufact_id = 33
  AND i_item_sk = ws_item_sk
  AND d_date BETWEEN '1999-02-10' AND (
    DATE_ADD(CAST('1999-02-10' AS DATE), 90)
  )
  AND d_date_sk = ws_sold_date_sk
  AND ws_ext_discount_amt > (
    SELECT
      1.3 * AVG(ws_ext_discount_amt)
    FROM web_sales, date_dim
    WHERE
      ws_item_sk = i_item_sk
      AND d_date BETWEEN '1999-02-10' AND (
        DATE_ADD(CAST('1999-02-10' AS DATE), 90)
      )
      AND d_date_sk = ws_sold_date_sk
  )
ORDER BY
  SUM(ws_ext_discount_amt)
LIMIT 100