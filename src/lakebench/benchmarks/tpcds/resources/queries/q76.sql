SELECT
  channel,
  col_name,
  d_year,
  d_qoy,
  i_category,
  COUNT(*) AS sales_cnt,
  SUM(ext_sales_price) AS sales_amt
FROM (
  SELECT
    'store' AS channel,
    'ss_cdemo_sk' AS col_name,
    d_year,
    d_qoy,
    i_category,
    ss_ext_sales_price AS ext_sales_price
  FROM store_sales
  JOIN item ON ss_item_sk = i_item_sk
  JOIN date_dim ON ss_sold_date_sk = d_date_sk
  WHERE
    ss_cdemo_sk IS NULL
  UNION ALL
  SELECT
    'web' AS channel,
    'ws_web_site_sk' AS col_name,
    d_year,
    d_qoy,
    i_category,
    ws_ext_sales_price AS ext_sales_price
  FROM web_sales
  JOIN item ON ws_item_sk = i_item_sk
  JOIN date_dim ON ws_sold_date_sk = d_date_sk
  WHERE
    ws_web_site_sk IS NULL
  UNION ALL
  SELECT
    'catalog' AS channel,
    'cs_bill_addr_sk' AS col_name,
    d_year,
    d_qoy,
    i_category,
    cs_ext_sales_price AS ext_sales_price
  FROM catalog_sales
  JOIN item ON cs_item_sk = i_item_sk
  JOIN date_dim ON cs_sold_date_sk = d_date_sk
  WHERE
    cs_bill_addr_sk IS NULL
) AS foo
GROUP BY
  channel,
  col_name,
  d_year,
  d_qoy,
  i_category
ORDER BY
  channel,
  col_name,
  d_year,
  d_qoy,
  i_category
LIMIT 100