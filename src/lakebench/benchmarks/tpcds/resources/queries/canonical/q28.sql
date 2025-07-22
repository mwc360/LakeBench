SELECT
  *
FROM (
  SELECT
    AVG(ss_list_price) AS B1_LP,
    COUNT(ss_list_price) AS B1_CNT,
    COUNT(DISTINCT ss_list_price) AS B1_CNTD
  FROM store_sales
  WHERE
    ss_quantity BETWEEN 0 AND 5
    AND (
      ss_list_price BETWEEN 190 AND 190 + 10
      OR ss_coupon_amt BETWEEN 55 AND 55 + 1000
      OR ss_wholesale_cost BETWEEN 42 AND 42 + 20
    )
) AS B1, (
  SELECT
    AVG(ss_list_price) AS B2_LP,
    COUNT(ss_list_price) AS B2_CNT,
    COUNT(DISTINCT ss_list_price) AS B2_CNTD
  FROM store_sales
  WHERE
    ss_quantity BETWEEN 6 AND 10
    AND (
      ss_list_price BETWEEN 115 AND 115 + 10
      OR ss_coupon_amt BETWEEN 7271 AND 7271 + 1000
      OR ss_wholesale_cost BETWEEN 74 AND 74 + 20
    )
) AS B2, (
  SELECT
    AVG(ss_list_price) AS B3_LP,
    COUNT(ss_list_price) AS B3_CNT,
    COUNT(DISTINCT ss_list_price) AS B3_CNTD
  FROM store_sales
  WHERE
    ss_quantity BETWEEN 11 AND 15
    AND (
      ss_list_price BETWEEN 72 AND 72 + 10
      OR ss_coupon_amt BETWEEN 3099 AND 3099 + 1000
      OR ss_wholesale_cost BETWEEN 77 AND 77 + 20
    )
) AS B3, (
  SELECT
    AVG(ss_list_price) AS B4_LP,
    COUNT(ss_list_price) AS B4_CNT,
    COUNT(DISTINCT ss_list_price) AS B4_CNTD
  FROM store_sales
  WHERE
    ss_quantity BETWEEN 16 AND 20
    AND (
      ss_list_price BETWEEN 16 AND 16 + 10
      OR ss_coupon_amt BETWEEN 5099 AND 5099 + 1000
      OR ss_wholesale_cost BETWEEN 70 AND 70 + 20
    )
) AS B4, (
  SELECT
    AVG(ss_list_price) AS B5_LP,
    COUNT(ss_list_price) AS B5_CNT,
    COUNT(DISTINCT ss_list_price) AS B5_CNTD
  FROM store_sales
  WHERE
    ss_quantity BETWEEN 21 AND 25
    AND (
      ss_list_price BETWEEN 149 AND 149 + 10
      OR ss_coupon_amt BETWEEN 7344 AND 7344 + 1000
      OR ss_wholesale_cost BETWEEN 78 AND 78 + 20
    )
) AS B5, (
  SELECT
    AVG(ss_list_price) AS B6_LP,
    COUNT(ss_list_price) AS B6_CNT,
    COUNT(DISTINCT ss_list_price) AS B6_CNTD
  FROM store_sales
  WHERE
    ss_quantity BETWEEN 26 AND 30
    AND (
      ss_list_price BETWEEN 165 AND 165 + 10
      OR ss_coupon_amt BETWEEN 3569 AND 3569 + 1000
      OR ss_wholesale_cost BETWEEN 59 AND 59 + 20
    )
) AS B6
LIMIT 100