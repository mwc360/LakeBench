SELECT
  i_item_id,
  ca_country,
  ca_state,
  ca_county,
  AVG(CAST(cs_quantity AS DECIMAL(12, 2))) AS agg1,
  AVG(CAST(cs_list_price AS DECIMAL(12, 2))) AS agg2,
  AVG(CAST(cs_coupon_amt AS DECIMAL(12, 2))) AS agg3,
  AVG(CAST(cs_sales_price AS DECIMAL(12, 2))) AS agg4,
  AVG(CAST(cs_net_profit AS DECIMAL(12, 2))) AS agg5,
  AVG(CAST(c_birth_year AS DECIMAL(12, 2))) AS agg6,
  AVG(CAST(cd1.cd_dep_count AS DECIMAL(12, 2))) AS agg7
FROM catalog_sales, customer_demographics AS cd1, customer_demographics AS cd2, customer, customer_address, date_dim, item
WHERE
  cs_sold_date_sk = d_date_sk
  AND cs_item_sk = i_item_sk
  AND cs_bill_cdemo_sk = cd1.cd_demo_sk
  AND cs_bill_customer_sk = c_customer_sk
  AND cd1.cd_gender = 'M'
  AND cd1.cd_education_status = 'Primary'
  AND c_current_cdemo_sk = cd2.cd_demo_sk
  AND c_current_addr_sk = ca_address_sk
  AND c_birth_month IN (4, 6, 8, 11, 2, 12)
  AND d_year = 2001
  AND ca_state IN ('NE', 'PA', 'RI', 'TX', 'MN', 'KY', 'ID')
GROUP BY
ROLLUP (
  i_item_id,
  ca_country,
  ca_state,
  ca_county
)
ORDER BY
  ca_country,
  ca_state,
  ca_county,
  i_item_id
LIMIT 100