SELECT
  cd_gender,
  cd_marital_status,
  cd_education_status,
  COUNT(*) AS cnt1,
  cd_purchase_estimate,
  COUNT(*) AS cnt2,
  cd_credit_rating,
  COUNT(*) AS cnt3,
  cd_dep_count,
  COUNT(*) AS cnt4,
  cd_dep_employed_count,
  COUNT(*) AS cnt5,
  cd_dep_college_count,
  COUNT(*) AS cnt6
FROM customer AS c
JOIN customer_address AS ca ON c.c_current_addr_sk = ca.ca_address_sk
JOIN customer_demographics AS cd ON cd_demo_sk = c.c_current_cdemo_sk
WHERE
  ca_county IN ('Shawnee County', 'Jersey County', 'Big Stone County', 'Polk County', 'Wolfe County')
  AND EXISTS(
    SELECT
      *
    FROM store_sales AS ss
    JOIN date_dim AS d ON ss.ss_sold_date_sk = d.d_date_sk
    WHERE
      c.c_customer_sk = ss.ss_customer_sk
      AND d.d_year = 1999
      AND d.d_moy BETWEEN 4 AND 4 + 3
  )
  AND (
    EXISTS(
      SELECT
        *
      FROM web_sales AS ws
      JOIN date_dim AS d ON ws.ws_sold_date_sk = d.d_date_sk
      WHERE
        c.c_customer_sk = ws.ws_bill_customer_sk
        AND d.d_year = 1999
        AND d.d_moy BETWEEN 4 AND 4 + 3
    )
    OR EXISTS(
      SELECT
        *
      FROM catalog_sales AS cs
      JOIN date_dim AS d ON cs.cs_sold_date_sk = d.d_date_sk
      WHERE
        c.c_customer_sk = cs.cs_ship_customer_sk
        AND d.d_year = 1999
        AND d.d_moy BETWEEN 4 AND 4 + 3
    )
  )
GROUP BY
  cd_gender,
  cd_marital_status,
  cd_education_status,
  cd_purchase_estimate,
  cd_credit_rating,
  cd_dep_count,
  cd_dep_employed_count,
  cd_dep_college_count
ORDER BY
  cd_gender,
  cd_marital_status,
  cd_education_status,
  cd_purchase_estimate,
  cd_credit_rating,
  cd_dep_count,
  cd_dep_employed_count,
  cd_dep_college_count
LIMIT 100