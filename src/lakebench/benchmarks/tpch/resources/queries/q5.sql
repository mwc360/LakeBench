SELECT
    n_name,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM
    customer
    INNER JOIN orders ON c_custkey = o_custkey
    INNER JOIN lineitem ON l_orderkey = o_orderkey
    INNER JOIN supplier ON l_suppkey = s_suppkey
    INNER JOIN nation ON c_nationkey = s_nationkey AND s_nationkey = n_nationkey
    INNER JOIN region ON n_regionkey = r_regionkey
WHERE
    r_name = 'ASIA'
    AND o_orderdate >= CAST('1994-01-01' AS DATE)
    AND o_orderdate < ADD_MONTHS(CAST('1994-01-01' AS DATE), 12)
GROUP BY
    n_name
ORDER BY
    revenue DESC