SELECT
    o_year,
    SUM(
        CASE
            WHEN nation = 'ALGERIA' THEN volume
            ELSE CAST(0 AS DOUBLE)
        END
    ) / SUM(volume) AS mkt_share
FROM
    (
        SELECT
            EXTRACT(YEAR FROM o_orderdate) AS o_year,
            CAST(l_extendedprice AS DOUBLE) * (1 - CAST(l_discount AS DOUBLE)) AS volume,
            n2.n_name AS nation
        FROM
            part
            INNER JOIN lineitem ON p_partkey = l_partkey
            INNER JOIN supplier ON s_suppkey = l_suppkey
            INNER JOIN orders ON l_orderkey = o_orderkey
            INNER JOIN customer ON o_custkey = c_custkey
            INNER JOIN nation AS n1 ON c_nationkey = n1.n_nationkey
            INNER JOIN region ON n1.n_regionkey = r_regionkey
            INNER JOIN nation AS n2 ON s_nationkey = n2.n_nationkey
        WHERE
            r_name = 'AFRICA'
            AND o_orderdate BETWEEN CAST('1995-01-01' AS DATE) AND CAST('1996-12-31' AS DATE)
            AND p_type = 'STANDARD BURNISHED STEEL'
    ) AS all_nations
GROUP BY
    o_year
ORDER BY
    o_year