SELECT
    p_brand,
    p_type,
    p_size,
    COUNT(DISTINCT ps_suppkey) AS supplier_cnt
FROM
    partsupp
    INNER JOIN part ON p_partkey = ps_partkey
WHERE
    p_brand <> 'Brand#15'
    AND NOT p_type LIKE 'ECONOMY BRUSHED%'
    AND p_size IN (7, 22, 42, 4, 39, 1, 41, 45)
    AND NOT ps_suppkey IN (
        SELECT
            s_suppkey
        FROM
            supplier
        WHERE
            s_comment LIKE '%Customer%Complaints%'
    )
GROUP BY
    p_brand,
    p_type,
    p_size
ORDER BY
    supplier_cnt DESC,
    p_brand,
    p_type,
    p_size