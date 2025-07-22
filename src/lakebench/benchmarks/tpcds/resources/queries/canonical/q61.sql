SELECT
  promotions,
  total,
  CAST(promotions AS DECIMAL(15, 4)) / CAST(total AS DECIMAL(15, 4)) * 100
FROM (
  SELECT
    SUM(ss_ext_sales_price) AS promotions
  FROM store_sales
  JOIN store ON ss_store_sk = s_store_sk
  JOIN promotion ON ss_promo_sk = p_promo_sk
  JOIN date_dim ON ss_sold_date_sk = d_date_sk
  JOIN customer ON ss_customer_sk = c_customer_sk
  JOIN customer_address ON ca_address_sk = c_current_addr_sk
  JOIN item ON ss_item_sk = i_item_sk
  WHERE
    ca_gmt_offset = -7
    AND i_category = 'Home'
    AND (
      p_channel_dmail = 'Y' OR p_channel_email = 'Y' OR p_channel_tv = 'Y'
    )
    AND s_gmt_offset = -7
    AND d_year = 2002
    AND d_moy = 11
) AS promotional_sales, (
  SELECT
    SUM(ss_ext_sales_price) AS total
  FROM store_sales
  JOIN store ON ss_store_sk = s_store_sk
  JOIN date_dim ON ss_sold_date_sk = d_date_sk
  JOIN customer ON ss_customer_sk = c_customer_sk
  JOIN customer_address ON ca_address_sk = c_current_addr_sk
  JOIN item ON ss_item_sk = i_item_sk
  WHERE
    ca_gmt_offset = -7
    AND i_category = 'Home'
    AND s_gmt_offset = -7
    AND d_year = 2002
    AND d_moy = 11
) AS all_sales
ORDER BY
  promotions,
  total
LIMIT 100