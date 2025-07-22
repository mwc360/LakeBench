WITH sr_items AS (
  SELECT
    i_item_id AS item_id,
    SUM(sr_return_quantity) AS sr_item_qty
  FROM store_returns
  JOIN item ON sr_item_sk = i_item_sk
  JOIN date_dim ON sr_returned_date_sk = d_date_sk
  WHERE
    d_date IN (
      SELECT
        d_date
      FROM date_dim
      WHERE
        d_week_seq IN (
          SELECT
            d_week_seq
          FROM date_dim
          WHERE
            d_date IN ('1999-02-22', '1999-08-19', '1999-11-19')
        )
    )
  GROUP BY
    i_item_id
), cr_items AS (
  SELECT
    i_item_id AS item_id,
    SUM(cr_return_quantity) AS cr_item_qty
  FROM catalog_returns
  JOIN item ON cr_item_sk = i_item_sk
  JOIN date_dim ON cr_returned_date_sk = d_date_sk
  WHERE
    d_date IN (
      SELECT
        d_date
      FROM date_dim
      WHERE
        d_week_seq IN (
          SELECT
            d_week_seq
          FROM date_dim
          WHERE
            d_date IN ('1999-02-22', '1999-08-19', '1999-11-19')
        )
    )
  GROUP BY
    i_item_id
), wr_items AS (
  SELECT
    i_item_id AS item_id,
    SUM(wr_return_quantity) AS wr_item_qty
  FROM web_returns
  JOIN item ON wr_item_sk = i_item_sk
  JOIN date_dim ON wr_returned_date_sk = d_date_sk
  WHERE
    d_date IN (
      SELECT
        d_date
      FROM date_dim
      WHERE
        d_week_seq IN (
          SELECT
            d_week_seq
          FROM date_dim
          WHERE
            d_date IN ('1999-02-22', '1999-08-19', '1999-11-19')
        )
    )
  GROUP BY
    i_item_id
)
SELECT
  sr_items.item_id,
  sr_item_qty,
  sr_item_qty / (
    sr_item_qty + cr_item_qty + wr_item_qty
  ) / 3.0 * 100 AS sr_dev,
  cr_item_qty,
  cr_item_qty / (
    sr_item_qty + cr_item_qty + wr_item_qty
  ) / 3.0 * 100 AS cr_dev,
  wr_item_qty,
  wr_item_qty / (
    sr_item_qty + cr_item_qty + wr_item_qty
  ) / 3.0 * 100 AS wr_dev,
  (
    sr_item_qty + cr_item_qty + wr_item_qty
  ) / 3.0 AS average
FROM sr_items
JOIN cr_items ON sr_items.item_id = cr_items.item_id
JOIN wr_items ON sr_items.item_id = wr_items.item_id
ORDER BY
  sr_items.item_id,
  sr_item_qty
LIMIT 100