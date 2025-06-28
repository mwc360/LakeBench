SELECT
    c_custkey,
    c_name,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment
FROM
    customer
    INNER JOIN orders ON c_custkey = o_custkey
    INNER JOIN lineitem ON l_orderkey = o_orderkey
    INNER JOIN nation ON c_nationkey = n_nationkey
WHERE
    o_orderdate >= CAST('1993-06-01' AS DATE)
    AND o_orderdate < CAST('1993-06-01' AS DATE) + INTERVAL '3' MONTH
    AND l_returnflag = 'R'
GROUP BY
    c_custkey,
    c_name,
    c_acctbal,
    c_phone,
    n_name,
    c_address,
    c_comment
ORDER BY
    revenue DESC