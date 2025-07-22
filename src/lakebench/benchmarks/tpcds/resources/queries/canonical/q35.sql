SELECT
  ca_state,
  cd_gender,
  cd_marital_status,
  cd_dep_count,
  COUNT(*) AS cnt1,
  MAX(cd_dep_count),
  MAX(cd_dep_count),
  MIN(cd_dep_count),
  cd_dep_employed_count,
  COUNT(*) AS cnt2,
  MAX(cd_dep_employed_count),
  MAX(cd_dep_employed_count),
  MIN(cd_dep_employed_count),
  cd_dep_college_count,
  COUNT(*) AS cnt3,
  MAX(cd_dep_college_count),
  MAX(cd_dep_college_count),
  MIN(cd_dep_college_count)
FROM customer AS c
JOIN customer_address AS ca ON c.c_current_addr_sk = ca.ca_address_sk
JOIN customer_demographics ON cd_demo_sk = c.c_current_cdemo_sk
WHERE
  EXISTS(
    SELECT
      *
    FROM store_sales
    JOIN date_dim ON ss_sold_date_sk = d_date_sk
    WHERE
      c.c_customer_sk = ss_customer_sk
      AND d_year = 2000
      AND d_qoy < 4
  )
  AND (
    EXISTS(
      SELECT
        *
      FROM web_sales
      JOIN date_dim ON ws_sold_date_sk = d_date_sk
      WHERE
        c.c_customer_sk = ws_bill_customer_sk
        AND d_year = 2000
        AND d_qoy < 4
    )
    OR EXISTS(
      SELECT
        *
      FROM catalog_sales
      JOIN date_dim ON cs_sold_date_sk = d_date_sk
      WHERE
        c.c_customer_sk = cs_ship_customer_sk
        AND d_year = 2000
        AND d_qoy < 4
    )
  )
GROUP BY
  ca_state,
  cd_gender,
  cd_marital_status,
  cd_dep_count,
  cd_dep_employed_count,
  cd_dep_college_count
ORDER BY
  ca_state,
  cd_gender,
  cd_marital_status,
  cd_dep_count,
  cd_dep_employed_count,
  cd_dep_college_count
LIMIT 100