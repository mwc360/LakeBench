WITH cross_items AS (
  SELECT
    i_item_sk AS ss_item_sk
  FROM item
  JOIN (
    SELECT
      iss.i_brand_id AS brand_id,
      iss.i_class_id AS class_id,
      iss.i_category_id AS category_id
    FROM store_sales
    JOIN item AS iss ON ss_item_sk = iss.i_item_sk
    JOIN date_dim AS d1 ON ss_sold_date_sk = d1.d_date_sk
    WHERE d1.d_year BETWEEN 1999 AND 1999 + 2
    INTERSECT
    SELECT
      ics.i_brand_id,
      ics.i_class_id,
      ics.i_category_id
    FROM catalog_sales
    JOIN item AS ics ON cs_item_sk = ics.i_item_sk
    JOIN date_dim AS d2 ON cs_sold_date_sk = d2.d_date_sk
    WHERE d2.d_year BETWEEN 1999 AND 1999 + 2
    INTERSECT
    SELECT
      iws.i_brand_id,
      iws.i_class_id,
      iws.i_category_id
    FROM web_sales
    JOIN item AS iws ON ws_item_sk = iws.i_item_sk
    JOIN date_dim AS d3 ON ws_sold_date_sk = d3.d_date_sk
    WHERE d3.d_year BETWEEN 1999 AND 1999 + 2
  ) AS x ON i_brand_id = brand_id AND i_class_id = class_id AND i_category_id = category_id
), avg_sales AS (
  SELECT
    AVG(quantity * list_price) AS average_sales
  FROM (
    SELECT
      ss_quantity AS quantity,
      ss_list_price AS list_price
    FROM store_sales
    JOIN date_dim ON ss_sold_date_sk = d_date_sk
    WHERE d_year BETWEEN 1999 AND 1999 + 2
    UNION ALL
    SELECT
      cs_quantity AS quantity,
      cs_list_price AS list_price
    FROM catalog_sales
    JOIN date_dim ON cs_sold_date_sk = d_date_sk
    WHERE d_year BETWEEN 1999 AND 1999 + 2
    UNION ALL
    SELECT
      ws_quantity AS quantity,
      ws_list_price AS list_price
    FROM web_sales
    JOIN date_dim ON ws_sold_date_sk = d_date_sk
    WHERE d_year BETWEEN 1999 AND 1999 + 2
  ) AS x
)
SELECT
  this_year.channel AS ty_channel,
  this_year.i_brand_id AS ty_brand,
  this_year.i_class_id AS ty_class,
  this_year.i_category_id AS ty_category,
  this_year.sales AS ty_sales,
  this_year.number_sales AS ty_number_sales,
  last_year.channel AS ly_channel,
  last_year.i_brand_id AS ly_brand,
  last_year.i_class_id AS ly_class,
  last_year.i_category_id AS ly_category,
  last_year.sales AS ly_sales,
  last_year.number_sales AS ly_number_sales
FROM (
  SELECT
    'store' AS channel,
    i_brand_id,
    i_class_id,
    i_category_id,
    SUM(ss_quantity * ss_list_price) AS sales,
    COUNT(*) AS number_sales
  FROM store_sales
  JOIN item ON ss_item_sk = i_item_sk
  JOIN date_dim ON ss_sold_date_sk = d_date_sk
  WHERE
    ss_item_sk IN (
      SELECT
        ss_item_sk
      FROM cross_items
    )
    AND d_week_seq = (
      SELECT
        d_week_seq
      FROM date_dim
      WHERE
        d_year = 1999 + 1 AND d_moy = 12 AND d_dom = 1
    )
  GROUP BY
    i_brand_id,
    i_class_id,
    i_category_id
  HAVING
    SUM(ss_quantity * ss_list_price) > (
      SELECT
        average_sales
      FROM avg_sales
    )
) AS this_year
JOIN (
  SELECT
    'store' AS channel,
    i_brand_id,
    i_class_id,
    i_category_id,
    SUM(ss_quantity * ss_list_price) AS sales,
    COUNT(*) AS number_sales
  FROM store_sales
  JOIN item ON ss_item_sk = i_item_sk
  JOIN date_dim ON ss_sold_date_sk = d_date_sk
  WHERE
    ss_item_sk IN (
      SELECT
        ss_item_sk
      FROM cross_items
    )
    AND d_week_seq = (
      SELECT
        d_week_seq
      FROM date_dim
      WHERE
        d_year = 1999 AND d_moy = 12 AND d_dom = 1
    )
  GROUP BY
    i_brand_id,
    i_class_id,
    i_category_id
  HAVING
    SUM(ss_quantity * ss_list_price) > (
      SELECT
        average_sales
      FROM avg_sales
    )
) AS last_year ON this_year.i_brand_id = last_year.i_brand_id
  AND this_year.i_class_id = last_year.i_class_id
  AND this_year.i_category_id = last_year.i_category_id
ORDER BY
  this_year.channel,
  this_year.i_brand_id,
  this_year.i_class_id,
  this_year.i_category_id
LIMIT 100
