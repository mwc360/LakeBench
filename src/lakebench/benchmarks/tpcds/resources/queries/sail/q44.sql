SELECT
  asceding.rnk,
  i1.i_product_name AS best_performing,
  i2.i_product_name AS worst_performing
FROM (
  SELECT
    *
  FROM (
    SELECT
      item_sk,
      CAST(RANK() OVER (ORDER BY rank_col ASC) AS LONG) AS rnk /* RANK() is internally represented as *unsigned* int; additional cast is necessary; https://github.com/lakehq/sail/issues/732 */
    FROM (
      SELECT
        ss_item_sk AS item_sk,
        AVG(ss_net_profit) AS rank_col
      FROM store_sales AS ss1
      WHERE
        ss_store_sk = 120
      GROUP BY
        ss_item_sk
      HAVING
        AVG(ss_net_profit) > 0.9 * (
          SELECT
            AVG(ss_net_profit) AS rank_col
          FROM store_sales
          WHERE
            ss_store_sk = 120 AND ss_hdemo_sk IS NULL
          GROUP BY
            ss_store_sk
        )
    ) AS V1
  ) AS V11
  WHERE
    rnk < 11
) AS asceding
JOIN (
  SELECT
    *
  FROM (
    SELECT
      item_sk,
      RANK() OVER (ORDER BY rank_col DESC) AS rnk
    FROM (
      SELECT
        ss_item_sk AS item_sk,
        AVG(ss_net_profit) AS rank_col
      FROM store_sales AS ss1
      WHERE
        ss_store_sk = 120
      GROUP BY
        ss_item_sk
      HAVING
        AVG(ss_net_profit) > 0.9 * (
          SELECT
            AVG(ss_net_profit) AS rank_col
          FROM store_sales
          WHERE
            ss_store_sk = 120 AND ss_hdemo_sk IS NULL
          GROUP BY
            ss_store_sk
        )
    ) AS V2
  ) AS V21
  WHERE
    rnk < 11
) AS descending ON asceding.rnk = descending.rnk
JOIN item AS i1 ON i1.i_item_sk = asceding.item_sk
JOIN item AS i2 ON i2.i_item_sk = descending.item_sk
ORDER BY
  asceding.rnk
LIMIT 100