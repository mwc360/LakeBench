SELECT
    nation,
    o_year,
    SUM(amount) AS sum_profit
FROM
    (
        SELECT
            n_name AS nation,
            EXTRACT(YEAR FROM o_orderdate) AS o_year,
            CAST(l_extendedprice AS DOUBLE) * (1 - CAST(l_discount AS DOUBLE))
            - CAST(ps_supplycost AS DOUBLE) * CAST(l_quantity AS DOUBLE) AS amount
        FROM
            part
            INNER JOIN lineitem ON p_partkey = l_partkey
            INNER JOIN partsupp ON ps_partkey = l_partkey AND ps_suppkey = l_suppkey
            INNER JOIN supplier ON s_suppkey = l_suppkey
            INNER JOIN orders ON o_orderkey = l_orderkey
            INNER JOIN nation ON s_nationkey = n_nationkey
        WHERE
            p_name LIKE '%bisque%'
    ) AS profit
GROUP BY
    nation,
    o_year
ORDER BY
    nation,
    o_year DESC