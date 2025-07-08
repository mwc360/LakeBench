SELECT
    s.s_acctbal,
    s.s_name,
    n.n_name,
    p.p_partkey,
    p.p_mfgr,
    s.s_address,
    s.s_phone,
    s.s_comment
FROM
    part p
JOIN
    partsupp ps ON p.p_partkey = ps.ps_partkey
JOIN
    supplier s ON s.s_suppkey = ps.ps_suppkey
JOIN
    nation n ON s.s_nationkey = n.n_nationkey
JOIN
    region r ON n.n_regionkey = r.r_regionkey
WHERE
    p.p_size = 38
    AND p.p_type LIKE '%TIN'
    AND r.r_name = 'EUROPE'
    AND ps.ps_supplycost = (
        SELECT
            MIN(ps_inner.ps_supplycost)
        FROM
            partsupp ps_inner
        JOIN
            supplier s_inner ON s_inner.s_suppkey = ps_inner.ps_suppkey
        JOIN
            nation n_inner ON s_inner.s_nationkey = n_inner.n_nationkey
        JOIN
            region r_inner ON n_inner.n_regionkey = r_inner.r_regionkey
        WHERE
            ps_inner.ps_partkey = p.p_partkey
            AND r_inner.r_name = 'EUROPE'
    )
ORDER BY
    s.s_acctbal DESC,
    n.n_name,
    s.s_name,
    p.p_partkey
LIMIT 100