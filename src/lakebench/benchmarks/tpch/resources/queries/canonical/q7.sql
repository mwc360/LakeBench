SELECT
    supp_nation,
    cust_nation,
    l_year,
    SUM(volume) AS revenue
FROM
    (
        SELECT
            n1.n_name AS supp_nation,
            n2.n_name AS cust_nation,
            EXTRACT(YEAR FROM l_shipdate) AS l_year,
            l_extendedprice * (1 - l_discount) AS volume
        FROM
            supplier
            INNER JOIN lineitem ON s_suppkey = l_suppkey
            INNER JOIN orders ON o_orderkey = l_orderkey
            INNER JOIN customer ON c_custkey = o_custkey
            INNER JOIN nation AS n1 ON s_nationkey = n1.n_nationkey
            INNER JOIN nation AS n2 ON c_nationkey = n2.n_nationkey
        WHERE
            (
                (n1.n_name = 'FRANCE' AND n2.n_name = 'ALGERIA')
                OR (n1.n_name = 'ALGERIA' AND n2.n_name = 'FRANCE')
            )
            AND l_shipdate BETWEEN CAST('1995-01-01' AS DATE) AND CAST('1996-12-31' AS DATE)
    ) AS shipping
GROUP BY
    supp_nation,
    cust_nation,
    l_year
ORDER BY
    supp_nation,
    cust_nation,
    l_year