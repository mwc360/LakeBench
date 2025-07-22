WITH ss AS (
  SELECT
    i_item_id,
    SUM(ss_ext_sales_price) AS total_sales
  FROM store_sales
  JOIN date_dim ON ss_sold_date_sk = d_date_sk
  JOIN customer_address ON ss_addr_sk = ca_address_sk
  JOIN item ON ss_item_sk = i_item_sk
  WHERE
    i_item_id IN (
      SELECT
        i_item_id
      FROM item
      WHERE
        i_category IN ('Men')
    )
    AND d_year = 2002
    AND d_moy = 8
    AND ca_gmt_offset = -6
  GROUP BY
    i_item_id
), cs AS (
  SELECT
    i_item_id,
    SUM(cs_ext_sales_price) AS total_sales
  FROM catalog_sales
  JOIN date_dim ON cs_sold_date_sk = d_date_sk
  JOIN customer_address ON cs_bill_addr_sk = ca_address_sk
  JOIN item ON cs_item_sk = i_item_sk
  WHERE
    i_item_id IN (
      SELECT
        i_item_id
      FROM item
      WHERE
        i_category IN ('Men')
    )
    AND d_year = 2002
    AND d_moy = 8
    AND ca_gmt_offset = -6
  GROUP BY
    i_item_id
), ws AS (
  SELECT
    i_item_id,
    SUM(ws_ext_sales_price) AS total_sales
  FROM web_sales
  JOIN item ON ws_item_sk = i_item_sk
  JOIN date_dim ON ws_sold_date_sk = d_date_sk
  JOIN customer_address ON ws_bill_addr_sk = ca_address_sk
  WHERE
    i_item_id IN (
      SELECT
        i_item_id
      FROM item
      WHERE
        i_category IN ('Men')
    )
    AND d_year = 2002
    AND d_moy = 8
    AND ca_gmt_offset = -6
  GROUP BY
    i_item_id
)
SELECT
  i_item_id,
  SUM(total_sales) AS total_sales
FROM (
  SELECT
    *
  FROM ss
  UNION ALL
  SELECT
    *
  FROM cs
  UNION ALL
  SELECT
    *
  FROM ws
) AS tmp1
GROUP BY
  i_item_id
ORDER BY
  i_item_id,
  total_sales
LIMIT 100