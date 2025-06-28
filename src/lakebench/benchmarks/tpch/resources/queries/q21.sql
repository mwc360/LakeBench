SELECT
    s_name,
    COUNT(*) AS numwait
FROM
    supplier
    INNER JOIN lineitem AS l1 ON s_suppkey = l1.l_suppkey
    INNER JOIN orders ON o_orderkey = l1.l_orderkey
    INNER JOIN nation ON s_nationkey = n_nationkey
WHERE
    o_orderstatus = 'F'
    AND l1.l_receiptdate > l1.l_commitdate
    AND EXISTS(
        SELECT
            *
        FROM
            lineitem AS l2
        WHERE
            l2.l_orderkey = l1.l_orderkey
            AND l2.l_suppkey <> l1.l_suppkey
    )
    AND NOT EXISTS(
        SELECT
            *
        FROM
            lineitem AS l3
        WHERE
            l3.l_orderkey = l1.l_orderkey
            AND l3.l_suppkey <> l1.l_suppkey
            AND l3.l_receiptdate > l3.l_commitdate
    )
    AND n_name = 'SAUDI ARABIA'
GROUP BY
    s_name
ORDER BY
    numwait DESC,
    s_name