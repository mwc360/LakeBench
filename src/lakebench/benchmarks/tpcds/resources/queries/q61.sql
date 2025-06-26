SELECT
  promotions,
  total,
  CAST(promotions AS DECIMAL(15, 4)) / CAST(total AS DECIMAL(15, 4)) * 100
FROM (
  SELECT
    SUM(ss_ext_sales_price) AS promotions
  FROM store_sales, store, promotion, date_dim, customer, customer_address, item
  WHERE
    ss_sold_date_sk = d_date_sk
    AND ss_store_sk = s_store_sk
    AND ss_promo_sk = p_promo_sk
    AND ss_customer_sk = c_customer_sk
    AND ca_address_sk = c_current_addr_sk
    AND ss_item_sk = i_item_sk
    AND ca_gmt_offset = -7
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
  FROM store_sales, store, date_dim, customer, customer_address, item
  WHERE
    ss_sold_date_sk = d_date_sk
    AND ss_store_sk = s_store_sk
    AND ss_customer_sk = c_customer_sk
    AND ca_address_sk = c_current_addr_sk
    AND ss_item_sk = i_item_sk
    AND ca_gmt_offset = -7
    AND i_category = 'Home'
    AND s_gmt_offset = -7
    AND d_year = 2002
    AND d_moy = 11
) AS all_sales
ORDER BY
  promotions,
  total
LIMIT 100