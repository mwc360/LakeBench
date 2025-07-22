SELECT
    100.00 * SUM(
        CASE
            WHEN p_type LIKE 'PROMO%' THEN CAST(l_extendedprice AS DOUBLE) * (1 - CAST(l_discount AS DOUBLE))
            ELSE CAST(0 AS DOUBLE)
        END
    ) / SUM(CAST(l_extendedprice AS DOUBLE) * (1 - CAST(l_discount AS DOUBLE))) AS promo_revenue
FROM
    lineitem
    INNER JOIN part ON l_partkey = p_partkey
WHERE
    l_shipdate >= CAST('1993-11-01' AS DATE)
    AND l_shipdate < CAST('1993-11-01' AS DATE) + INTERVAL '1' MONTH