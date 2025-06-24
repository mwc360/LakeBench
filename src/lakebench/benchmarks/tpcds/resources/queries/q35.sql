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
FROM customer AS c, customer_address AS ca, customer_demographics
WHERE
  c.c_current_addr_sk = ca.ca_address_sk
  AND cd_demo_sk = c.c_current_cdemo_sk
  AND EXISTS(
    SELECT
      *
    FROM store_sales, date_dim
    WHERE
      c.c_customer_sk = ss_customer_sk
      AND ss_sold_date_sk = d_date_sk
      AND d_year = 2000
      AND d_qoy < 4
  )
  AND (
    EXISTS(
      SELECT
        *
      FROM web_sales, date_dim
      WHERE
        c.c_customer_sk = ws_bill_customer_sk
        AND ws_sold_date_sk = d_date_sk
        AND d_year = 2000
        AND d_qoy < 4
    )
    OR EXISTS(
      SELECT
        *
      FROM catalog_sales, date_dim
      WHERE
        c.c_customer_sk = cs_ship_customer_sk
        AND cs_sold_date_sk = d_date_sk
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