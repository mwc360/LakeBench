SELECT
    s_name,
    s_address
FROM
    supplier
    INNER JOIN nation ON s_nationkey = n_nationkey
WHERE
    s_suppkey IN (
        SELECT
            ps_suppkey
        FROM
            partsupp
        WHERE
            ps_partkey IN (
                SELECT
                    p_partkey
                FROM
                    part
                WHERE
                    p_name LIKE 'ivory%'
            )
            AND ps_availqty > (
                SELECT
                    0.5 * SUM(l_quantity)
                FROM
                    lineitem
                WHERE
                    l_partkey = ps_partkey
                    AND l_suppkey = ps_suppkey
                    AND l_shipdate >= CAST('1997-01-01' AS DATE)
                    AND l_shipdate < DATE_ADD(CAST('1997-01-01' AS DATE), 365)
            )
    )
    AND n_name = 'ALGERIA'
ORDER BY
    s_name