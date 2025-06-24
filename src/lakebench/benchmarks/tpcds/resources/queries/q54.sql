WITH my_customers AS (
  SELECT DISTINCT
    c_customer_sk,
    c_current_addr_sk
  FROM (
    SELECT
      cs_sold_date_sk AS sold_date_sk,
      cs_bill_customer_sk AS customer_sk,
      cs_item_sk AS item_sk
    FROM catalog_sales
    UNION ALL
    SELECT
      ws_sold_date_sk AS sold_date_sk,
      ws_bill_customer_sk AS customer_sk,
      ws_item_sk AS item_sk
    FROM web_sales
  ) AS cs_or_ws_sales, item, date_dim, customer
  WHERE
    sold_date_sk = d_date_sk
    AND item_sk = i_item_sk
    AND i_category = 'Home'
    AND i_class = 'paint'
    AND c_customer_sk = cs_or_ws_sales.customer_sk
    AND d_moy = 3
    AND d_year = 2002
), my_revenue AS (
  SELECT
    c_customer_sk,
    SUM(ss_ext_sales_price) AS revenue
  FROM my_customers, store_sales, customer_address, store, date_dim
  WHERE
    c_current_addr_sk = ca_address_sk
    AND ca_county = s_county
    AND ca_state = s_state
    AND ss_sold_date_sk = d_date_sk
    AND c_customer_sk = ss_customer_sk
    AND d_month_seq BETWEEN (
      SELECT DISTINCT
        d_month_seq + 1
      FROM date_dim
      WHERE
        d_year = 2002 AND d_moy = 3
    ) AND (
      SELECT DISTINCT
        d_month_seq + 3
      FROM date_dim
      WHERE
        d_year = 2002 AND d_moy = 3
    )
  GROUP BY
    c_customer_sk
), segments AS (
  SELECT
    CAST((
      revenue / 50
    ) AS INT) AS segment
  FROM my_revenue
)
SELECT
  segment,
  COUNT(*) AS num_customers,
  segment * 50 AS segment_base
FROM segments
GROUP BY
  segment
ORDER BY
  segment,
  num_customers
LIMIT 100