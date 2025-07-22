WITH wss AS (
  SELECT
    d_week_seq,
    ss_store_sk,
    SUM(CASE WHEN (
      d_day_name = 'Sunday'
    ) THEN ss_sales_price ELSE NULL END) AS sun_sales,
    SUM(CASE WHEN (
      d_day_name = 'Monday'
    ) THEN ss_sales_price ELSE NULL END) AS mon_sales,
    SUM(CASE WHEN (
      d_day_name = 'Tuesday'
    ) THEN ss_sales_price ELSE NULL END) AS tue_sales,
    SUM(CASE WHEN (
      d_day_name = 'Wednesday'
    ) THEN ss_sales_price ELSE NULL END) AS wed_sales,
    SUM(CASE WHEN (
      d_day_name = 'Thursday'
    ) THEN ss_sales_price ELSE NULL END) AS thu_sales,
    SUM(CASE WHEN (
      d_day_name = 'Friday'
    ) THEN ss_sales_price ELSE NULL END) AS fri_sales,
    SUM(CASE WHEN (
      d_day_name = 'Saturday'
    ) THEN ss_sales_price ELSE NULL END) AS sat_sales
  FROM store_sales
  JOIN date_dim ON d_date_sk = ss_sold_date_sk
  GROUP BY
    d_week_seq,
    ss_store_sk
)
SELECT
  s_store_name1,
  s_store_id1,
  d_week_seq1,
  sun_sales1 / sun_sales2,
  mon_sales1 / mon_sales2,
  tue_sales1 / tue_sales2,
  wed_sales1 / wed_sales2,
  thu_sales1 / thu_sales2,
  fri_sales1 / fri_sales2,
  sat_sales1 / sat_sales2
FROM (
  SELECT
    s_store_name AS s_store_name1,
    wss.d_week_seq AS d_week_seq1,
    s_store_id AS s_store_id1,
    sun_sales AS sun_sales1,
    mon_sales AS mon_sales1,
    tue_sales AS tue_sales1,
    wed_sales AS wed_sales1,
    thu_sales AS thu_sales1,
    fri_sales AS fri_sales1,
    sat_sales AS sat_sales1
  FROM wss
  JOIN store ON ss_store_sk = s_store_sk
  JOIN date_dim AS d ON d.d_week_seq = wss.d_week_seq
  WHERE
    d_month_seq BETWEEN 1199 AND 1199 + 11
) AS y
JOIN (
  SELECT
    s_store_name AS s_store_name2,
    wss.d_week_seq AS d_week_seq2,
    s_store_id AS s_store_id2,
    sun_sales AS sun_sales2,
    mon_sales AS mon_sales2,
    tue_sales AS tue_sales2,
    wed_sales AS wed_sales2,
    thu_sales AS thu_sales2,
    fri_sales AS fri_sales2,
    sat_sales AS sat_sales2
  FROM wss
  JOIN store ON ss_store_sk = s_store_sk
  JOIN date_dim AS d ON d.d_week_seq = wss.d_week_seq
  WHERE
    d_month_seq BETWEEN 1199 + 12 AND 1199 + 23
) AS x ON s_store_id1 = s_store_id2 AND d_week_seq1 = d_week_seq2 - 52
ORDER BY
  s_store_name1,
  s_store_id1,
  d_week_seq1
LIMIT 100