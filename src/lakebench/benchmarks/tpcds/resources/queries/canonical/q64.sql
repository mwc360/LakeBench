WITH cs_ui AS (
  SELECT
    cs_item_sk,
    SUM(cs_ext_list_price) AS sale,
    SUM(cr_refunded_cash + cr_reversed_charge + cr_store_credit) AS refund
  FROM catalog_sales
  JOIN catalog_returns ON cs_item_sk = cr_item_sk AND cs_order_number = cr_order_number
  GROUP BY
    cs_item_sk
  HAVING
    SUM(cs_ext_list_price) > 2 * SUM(cr_refunded_cash + cr_reversed_charge + cr_store_credit)
), cross_sales AS (
  SELECT
    i_product_name AS product_name,
    i_item_sk AS item_sk,
    s_store_name AS store_name,
    s_zip AS store_zip,
    ad1.ca_street_number AS b_street_number,
    ad1.ca_street_name AS b_street_name,
    ad1.ca_city AS b_city,
    ad1.ca_zip AS b_zip,
    ad2.ca_street_number AS c_street_number,
    ad2.ca_street_name AS c_street_name,
    ad2.ca_city AS c_city,
    ad2.ca_zip AS c_zip,
    d1.d_year AS syear,
    d2.d_year AS fsyear,
    d3.d_year AS s2year,
    COUNT(*) AS cnt,
    SUM(ss_wholesale_cost) AS s1,
    SUM(ss_list_price) AS s2,
    SUM(ss_coupon_amt) AS s3
  FROM store_sales
  JOIN store ON ss_store_sk = s_store_sk
  JOIN date_dim AS d1 ON ss_sold_date_sk = d1.d_date_sk
  JOIN customer ON ss_customer_sk = c_customer_sk
  JOIN customer_demographics AS cd1 ON ss_cdemo_sk = cd1.cd_demo_sk
  JOIN household_demographics AS hd1 ON ss_hdemo_sk = hd1.hd_demo_sk
  JOIN customer_address AS ad1 ON ss_addr_sk = ad1.ca_address_sk
  JOIN item ON ss_item_sk = i_item_sk
  JOIN store_returns ON ss_item_sk = sr_item_sk
    AND ss_ticket_number = sr_ticket_number
  JOIN cs_ui ON ss_item_sk = cs_ui.cs_item_sk
  JOIN customer_demographics AS cd2 ON c_current_cdemo_sk = cd2.cd_demo_sk
  JOIN household_demographics AS hd2 ON c_current_hdemo_sk = hd2.hd_demo_sk
  JOIN customer_address AS ad2 ON c_current_addr_sk = ad2.ca_address_sk
  JOIN date_dim AS d2 ON c_first_sales_date_sk = d2.d_date_sk
  JOIN date_dim AS d3 ON c_first_shipto_date_sk = d3.d_date_sk
  JOIN promotion ON ss_promo_sk = p_promo_sk
  JOIN income_band AS ib1 ON hd1.hd_income_band_sk = ib1.ib_income_band_sk
  JOIN income_band AS ib2 ON hd2.hd_income_band_sk = ib2.ib_income_band_sk
  WHERE
    cd1.cd_marital_status <> cd2.cd_marital_status
    AND i_color IN ('coral', 'thistle', 'lace', 'beige', 'spring', 'orchid')
    AND i_current_price BETWEEN 81 AND 81 + 10
    AND i_current_price BETWEEN 81 + 1 AND 81 + 15
  GROUP BY
    i_product_name,
    i_item_sk,
    s_store_name,
    s_zip,
    ad1.ca_street_number,
    ad1.ca_street_name,
    ad1.ca_city,
    ad1.ca_zip,
    ad2.ca_street_number,
    ad2.ca_street_name,
    ad2.ca_city,
    ad2.ca_zip,
    d1.d_year,
    d2.d_year,
    d3.d_year
)
SELECT
  cs1.product_name,
  cs1.store_name,
  cs1.store_zip,
  cs1.b_street_number,
  cs1.b_street_name,
  cs1.b_city,
  cs1.b_zip,
  cs1.c_street_number,
  cs1.c_street_name,
  cs1.c_city,
  cs1.c_zip,
  cs1.syear,
  cs1.cnt,
  cs1.s1 AS s11,
  cs1.s2 AS s21,
  cs1.s3 AS s31,
  cs2.s1 AS s12,
  cs2.s2 AS s22,
  cs2.s3 AS s32,
  cs2.syear,
  cs2.cnt
FROM cross_sales AS cs1
JOIN cross_sales AS cs2 ON cs1.item_sk = cs2.item_sk
  AND cs1.store_name = cs2.store_name
  AND cs1.store_zip = cs2.store_zip
WHERE
  cs1.syear = 2001
  AND cs2.syear = 2001 + 1
  AND cs2.cnt <= cs1.cnt
ORDER BY
  cs1.product_name,
  cs1.store_name,
  cs2.cnt,
  cs1.s1,
  cs2.s1