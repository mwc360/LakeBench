SELECT
  CASE
    WHEN (
      SELECT
        COUNT(*)
      FROM store_sales
      WHERE
        ss_quantity BETWEEN 1 AND 20
    ) > 10347718
    THEN (
      SELECT
        AVG(ss_ext_tax)
      FROM store_sales
      WHERE
        ss_quantity BETWEEN 1 AND 20
    )
    ELSE (
      SELECT
        AVG(ss_net_paid_inc_tax)
      FROM store_sales
      WHERE
        ss_quantity BETWEEN 1 AND 20
    )
  END AS bucket1,
  CASE
    WHEN (
      SELECT
        COUNT(*)
      FROM store_sales
      WHERE
        ss_quantity BETWEEN 21 AND 40
    ) > 41233003
    THEN (
      SELECT
        AVG(ss_ext_tax)
      FROM store_sales
      WHERE
        ss_quantity BETWEEN 21 AND 40
    )
    ELSE (
      SELECT
        AVG(ss_net_paid_inc_tax)
      FROM store_sales
      WHERE
        ss_quantity BETWEEN 21 AND 40
    )
  END AS bucket2,
  CASE
    WHEN (
      SELECT
        COUNT(*)
      FROM store_sales
      WHERE
        ss_quantity BETWEEN 41 AND 60
    ) > 25517412
    THEN (
      SELECT
        AVG(ss_ext_tax)
      FROM store_sales
      WHERE
        ss_quantity BETWEEN 41 AND 60
    )
    ELSE (
      SELECT
        AVG(ss_net_paid_inc_tax)
      FROM store_sales
      WHERE
        ss_quantity BETWEEN 41 AND 60
    )
  END AS bucket3,
  CASE
    WHEN (
      SELECT
        COUNT(*)
      FROM store_sales
      WHERE
        ss_quantity BETWEEN 61 AND 80
    ) > 5987365
    THEN (
      SELECT
        AVG(ss_ext_tax)
      FROM store_sales
      WHERE
        ss_quantity BETWEEN 61 AND 80
    )
    ELSE (
      SELECT
        AVG(ss_net_paid_inc_tax)
      FROM store_sales
      WHERE
        ss_quantity BETWEEN 61 AND 80
    )
  END AS bucket4,
  CASE
    WHEN (
      SELECT
        COUNT(*)
      FROM store_sales
      WHERE
        ss_quantity BETWEEN 81 AND 100
    ) > 15407422
    THEN (
      SELECT
        AVG(ss_ext_tax)
      FROM store_sales
      WHERE
        ss_quantity BETWEEN 81 AND 100
    )
    ELSE (
      SELECT
        AVG(ss_net_paid_inc_tax)
      FROM store_sales
      WHERE
        ss_quantity BETWEEN 81 AND 100
    )
  END AS bucket5
FROM reason
WHERE
  r_reason_sk = 1