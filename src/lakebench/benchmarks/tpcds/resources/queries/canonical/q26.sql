SELECT
  i_item_id,
  AVG(cs_quantity) AS agg1,
  AVG(cs_list_price) AS agg2,
  AVG(cs_coupon_amt) AS agg3,
  AVG(cs_sales_price) AS agg4
FROM catalog_sales
JOIN date_dim ON cs_sold_date_sk = d_date_sk
JOIN item ON cs_item_sk = i_item_sk
JOIN customer_demographics ON cs_bill_cdemo_sk = cd_demo_sk
JOIN promotion ON cs_promo_sk = p_promo_sk
WHERE
  cd_gender = 'M'
  AND cd_marital_status = 'D'
  AND cd_education_status = 'Secondary'
  AND (
    p_channel_email = 'N' OR p_channel_event = 'N'
  )
  AND d_year = 1999
GROUP BY
  i_item_id
ORDER BY
  i_item_id
LIMIT 100