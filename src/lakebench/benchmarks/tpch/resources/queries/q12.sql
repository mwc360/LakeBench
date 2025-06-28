SELECT
    l_shipmode,
    SUM(
        CASE
            WHEN o_orderpriority = '1-URGENT'
            OR o_orderpriority = '2-HIGH' THEN 1
            ELSE 0
        END
    ) AS high_line_count,
    SUM(
        CASE
            WHEN o_orderpriority <> '1-URGENT'
            AND o_orderpriority <> '2-HIGH' THEN 1
            ELSE 0
        END
    ) AS low_line_count
FROM
    orders
    INNER JOIN lineitem ON o_orderkey = l_orderkey
WHERE
    l_shipmode IN ('FOB', 'REG AIR')
    AND l_commitdate < l_receiptdate
    AND l_shipdate < l_commitdate
    AND l_receiptdate >= CAST('1993-01-01' AS DATE)
    AND l_receiptdate < CAST('1993-01-01' AS DATE) + INTERVAL '1' YEAR
GROUP BY
    l_shipmode
ORDER BY
    l_shipmode