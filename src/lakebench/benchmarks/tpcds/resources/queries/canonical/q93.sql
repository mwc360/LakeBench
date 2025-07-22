SELECT
  ss_customer_sk,
  SUM(act_sales) AS sumsales
FROM (
  SELECT
    ss_item_sk,
    ss_ticket_number,
    ss_customer_sk,
    CASE
      WHEN NOT sr_return_quantity IS NULL
      THEN (
        ss_quantity - sr_return_quantity
      ) * ss_sales_price
      ELSE (
        ss_quantity * ss_sales_price
      )
    END AS act_sales
  FROM store_sales
  LEFT OUTER JOIN store_returns
    ON (
      sr_item_sk = ss_item_sk AND sr_ticket_number = ss_ticket_number
    )
  JOIN reason ON sr_reason_sk = r_reason_sk
  WHERE
    r_reason_desc = 'reason 42'
) AS t
GROUP BY
  ss_customer_sk
ORDER BY
  sumsales,
  ss_customer_sk
LIMIT 100