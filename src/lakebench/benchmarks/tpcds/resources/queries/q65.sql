SELECT
  s_store_name,
  i_item_desc,
  sc.revenue,
  i_current_price,
  i_wholesale_cost,
  i_brand
FROM store, item, (
  SELECT
    ss_store_sk,
    AVG(revenue) AS ave
  FROM (
    SELECT
      ss_store_sk,
      ss_item_sk,
      SUM(ss_sales_price) AS revenue
    FROM store_sales, date_dim
    WHERE
      ss_sold_date_sk = d_date_sk AND d_month_seq BETWEEN 1183 AND 1183 + 11
    GROUP BY
      ss_store_sk,
      ss_item_sk
  ) AS sa
  GROUP BY
    ss_store_sk
) AS sb, (
  SELECT
    ss_store_sk,
    ss_item_sk,
    SUM(ss_sales_price) AS revenue
  FROM store_sales, date_dim
  WHERE
    ss_sold_date_sk = d_date_sk AND d_month_seq BETWEEN 1183 AND 1183 + 11
  GROUP BY
    ss_store_sk,
    ss_item_sk
) AS sc
WHERE
  sb.ss_store_sk = sc.ss_store_sk
  AND sc.revenue <= 0.1 * sb.ave
  AND s_store_sk = sc.ss_store_sk
  AND i_item_sk = sc.ss_item_sk
ORDER BY
  s_store_name,
  i_item_desc
LIMIT 100