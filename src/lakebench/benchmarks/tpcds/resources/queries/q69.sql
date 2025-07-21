SELECT
  cd_gender,
  cd_marital_status,
  cd_education_status,
  COUNT(*) AS cnt1,
  cd_purchase_estimate,
  COUNT(*) AS cnt2,
  cd_credit_rating,
  COUNT(*) AS cnt3
FROM customer AS c
JOIN customer_address AS ca ON c.c_current_addr_sk = ca.ca_address_sk
JOIN customer_demographics ON cd_demo_sk = c.c_current_cdemo_sk
WHERE
  ca_state IN ('KS', 'MT', 'NE')
  AND EXISTS(
    SELECT
      *
    FROM store_sales
    JOIN date_dim ON ss_sold_date_sk = d_date_sk
    WHERE
      c.c_customer_sk = ss_customer_sk
      AND d_year = 2000
      AND d_moy BETWEEN 4 AND 4 + 2
  )
  AND (
    NOT EXISTS(
      SELECT
        *
      FROM web_sales
      JOIN date_dim ON ws_sold_date_sk = d_date_sk
      WHERE
        c.c_customer_sk = ws_bill_customer_sk
        AND d_year = 2000
        AND d_moy BETWEEN 4 AND 4 + 2
    )
    AND NOT EXISTS(
      SELECT
        *
      FROM catalog_sales
      JOIN date_dim ON cs_sold_date_sk = d_date_sk
      WHERE
        c.c_customer_sk = cs_ship_customer_sk
        AND d_year = 2000
        AND d_moy BETWEEN 4 AND 4 + 2
    )
  )
GROUP BY
  cd_gender,
  cd_marital_status,
  cd_education_status,
  cd_purchase_estimate,
  cd_credit_rating
ORDER BY
  cd_gender,
  cd_marital_status,
  cd_education_status,
  cd_purchase_estimate,
  cd_credit_rating
LIMIT 100