SELECT
    SUM(l_extendedprice * l_discount) AS revenue
FROM
    lineitem
WHERE
    l_shipdate >= CAST('1994-01-01' AS DATE)
    AND l_shipdate < ADD_MONTHS(CAST('1994-01-01' AS DATE), 12)
    AND l_discount BETWEEN 0.04 - 0.01
    AND 0.04 + 0.01
    AND l_quantity < 24