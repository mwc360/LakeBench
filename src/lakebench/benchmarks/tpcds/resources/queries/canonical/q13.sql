SELECT
  AVG(ss_quantity),
  AVG(ss_ext_sales_price),
  AVG(ss_ext_wholesale_cost),
  SUM(ss_ext_wholesale_cost)
FROM store_sales
JOIN store ON s_store_sk = ss_store_sk
JOIN customer_demographics ON cd_demo_sk = ss_cdemo_sk
JOIN household_demographics ON ss_hdemo_sk = hd_demo_sk
JOIN customer_address ON ss_addr_sk = ca_address_sk
JOIN date_dim ON ss_sold_date_sk = d_date_sk
WHERE
  d_year = 2001
  AND (
    (
      cd_marital_status = 'W'
      AND cd_education_status = 'College'
      AND ss_sales_price BETWEEN 100.00 AND 150.00
      AND hd_dep_count = 3
    )
    OR (
      cd_marital_status = 'M'
      AND cd_education_status = 'Secondary'
      AND ss_sales_price BETWEEN 50.00 AND 100.00
      AND hd_dep_count = 1
    )
    OR (
      cd_marital_status = 'U'
      AND cd_education_status = 'Primary'
      AND ss_sales_price BETWEEN 150.00 AND 200.00
      AND hd_dep_count = 1
    )
  )
  AND (
    (
      ca_country = 'United States'
      AND ca_state IN ('KS', 'MT', 'NE')
      AND ss_net_profit BETWEEN 100 AND 200
    )
    OR (
      ca_country = 'United States'
      AND ca_state IN ('OH', 'NY', 'TN')
      AND ss_net_profit BETWEEN 150 AND 300
    )
    OR (
      ca_country = 'United States'
      AND ca_state IN ('PA', 'GA', 'MI')
      AND ss_net_profit BETWEEN 50 AND 250
    )
  )
