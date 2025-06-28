SELECT
    l_orderkey,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue,
    o_orderdate,
    o_shippriority
FROM
    customer
    INNER JOIN orders ON customer.c_custkey = orders.o_custkey
    INNER JOIN lineitem ON orders.o_orderkey = lineitem.l_orderkey
WHERE
    customer.c_mktsegment = 'MACHINERY'
    AND orders.o_orderdate < CAST('1995-03-24' AS DATE)
    AND lineitem.l_shipdate > CAST('1995-03-24' AS DATE)
GROUP BY
    l_orderkey,
    o_orderdate,
    o_shippriority
ORDER BY
    revenue DESC,
    o_orderdate