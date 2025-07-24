SELECT
    100.00 * SUM(
        CASE
            WHEN p_type LIKE 'PROMO%' THEN l_extendedprice * (1 - l_discount)
            ELSE 0
        END
    ) / SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue
FROM
    lineitem
    INNER JOIN part ON l_partkey = p_partkey
WHERE
    l_shipdate >= CAST('1993-11-01' AS DATE)
    AND l_shipdate < CAST('1993-12-01' AS DATE)