SELECT
    c_count,
    COUNT(*) AS custdist
FROM
    (
        SELECT
            c_custkey,
            COUNT(o_orderkey)
        FROM
            customer
            LEFT OUTER JOIN orders ON c_custkey = o_custkey
            AND NOT o_comment LIKE '%special%packages%'
        GROUP BY
            c_custkey
    ) AS c_orders(c_custkey, c_count)
GROUP BY
    c_count
ORDER BY
    custdist DESC,
    c_count DESC