SELECT
  SUBSTR(r_reason_desc, 1, 20),
  AVG(ws_quantity),
  AVG(wr_refunded_cash),
  AVG(wr_fee)
FROM web_sales, web_returns, web_page, customer_demographics AS cd1, customer_demographics AS cd2, customer_address, date_dim, reason
WHERE
  ws_web_page_sk = wp_web_page_sk
  AND ws_item_sk = wr_item_sk
  AND ws_order_number = wr_order_number
  AND ws_sold_date_sk = d_date_sk
  AND d_year = 1999
  AND cd1.cd_demo_sk = wr_refunded_cdemo_sk
  AND cd2.cd_demo_sk = wr_returning_cdemo_sk
  AND ca_address_sk = wr_refunded_addr_sk
  AND r_reason_sk = wr_reason_sk
  AND (
    (
      cd1.cd_marital_status = 'U'
      AND cd1.cd_marital_status = cd2.cd_marital_status
      AND cd1.cd_education_status = 'Primary'
      AND cd1.cd_education_status = cd2.cd_education_status
      AND ws_sales_price BETWEEN 100.00 AND 150.00
    )
    OR (
      cd1.cd_marital_status = 'D'
      AND cd1.cd_marital_status = cd2.cd_marital_status
      AND cd1.cd_education_status = 'College'
      AND cd1.cd_education_status = cd2.cd_education_status
      AND ws_sales_price BETWEEN 50.00 AND 100.00
    )
    OR (
      cd1.cd_marital_status = 'M'
      AND cd1.cd_marital_status = cd2.cd_marital_status
      AND cd1.cd_education_status = 'Unknown'
      AND cd1.cd_education_status = cd2.cd_education_status
      AND ws_sales_price BETWEEN 150.00 AND 200.00
    )
  )
  AND (
    (
      ca_country = 'United States'
      AND ca_state IN ('IN', 'MN', 'NE')
      AND ws_net_profit BETWEEN 100 AND 200
    )
    OR (
      ca_country = 'United States'
      AND ca_state IN ('OH', 'SC', 'KY')
      AND ws_net_profit BETWEEN 150 AND 300
    )
    OR (
      ca_country = 'United States'
      AND ca_state IN ('VA', 'IL', 'GA')
      AND ws_net_profit BETWEEN 50 AND 250
    )
  )
GROUP BY
  r_reason_desc
ORDER BY
  SUBSTR(r_reason_desc, 1, 20),
  AVG(ws_quantity),
  AVG(wr_refunded_cash),
  AVG(wr_fee)
LIMIT 100