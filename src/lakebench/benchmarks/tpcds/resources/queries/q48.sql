SELECT
  SUM(ss_quantity)
FROM store_sales
JOIN store ON s_store_sk = ss_store_sk
JOIN customer_demographics ON cd_demo_sk = ss_cdemo_sk
JOIN customer_address ON ss_addr_sk = ca_address_sk
JOIN date_dim ON ss_sold_date_sk = d_date_sk
WHERE
  d_year = 1999
  AND (
    (
      cd_marital_status = 'U'
      AND cd_education_status = 'Primary'
      AND ss_sales_price BETWEEN 100.00 AND 150.00
    )
    OR (
      cd_marital_status = 'D'
      AND cd_education_status = 'College'
      AND ss_sales_price BETWEEN 50.00 AND 100.00
    )
    OR (
      cd_marital_status = 'M'
      AND cd_education_status = 'Unknown'
      AND ss_sales_price BETWEEN 150.00 AND 200.00
    )
  )
  AND (
    (
      ca_country = 'United States'
      AND ca_state IN ('IN', 'MN', 'NE')
      AND ss_net_profit BETWEEN 0 AND 2000
    )
    OR (
      ca_country = 'United States'
      AND ca_state IN ('OH', 'SC', 'KY')
      AND ss_net_profit BETWEEN 150 AND 3000
    )
    OR (
      ca_country = 'United States'
      AND ca_state IN ('VA', 'IL', 'GA')
      AND ss_net_profit BETWEEN 50 AND 25000
    )
  )