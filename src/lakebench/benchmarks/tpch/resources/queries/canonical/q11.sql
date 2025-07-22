SELECT
    ps_partkey,
    SUM(ps_supplycost * ps_availqty) AS value
FROM
    partsupp
    INNER JOIN supplier ON ps_suppkey = s_suppkey
    INNER JOIN nation ON s_nationkey = n_nationkey
WHERE
    n_name = 'CHINA'
GROUP BY
    ps_partkey
HAVING
    SUM(ps_supplycost * ps_availqty) > (
        SELECT
            SUM(ps_supplycost * ps_availqty) * 0.0000001000
        FROM
            partsupp
            INNER JOIN supplier ON ps_suppkey = s_suppkey
            INNER JOIN nation ON s_nationkey = n_nationkey
        WHERE
            n_name = 'CHINA'
    )
ORDER BY
    value DESC