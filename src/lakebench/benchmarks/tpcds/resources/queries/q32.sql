SELECT
  SUM(cs_ext_discount_amt) AS `excess discount amount`
FROM catalog_sales
JOIN item ON i_item_sk = cs_item_sk
JOIN date_dim ON d_date_sk = cs_sold_date_sk
WHERE
  i_manufact_id = 33
  AND d_date BETWEEN '1999-02-10' AND 
    DATE_ADD(CAST('1999-02-10' AS DATE), 90)
  AND cs_ext_discount_amt > (
    SELECT
      1.3 * AVG(cs_ext_discount_amt)
    FROM catalog_sales
    JOIN date_dim ON d_date_sk = cs_sold_date_sk
    WHERE
      cs_item_sk = i_item_sk
      AND d_date BETWEEN '1999-02-10' AND 
        DATE_ADD(CAST('1999-02-10' AS DATE), 90)
  )
LIMIT 100