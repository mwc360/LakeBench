SELECT
    REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '$1') AS k,
    AVG(length(Referer)) AS l,
    COUNT(*) AS c,
    MIN(Referer)
FROM
    hits
WHERE
    Referer <> ''
GROUP BY
    k
HAVING
    COUNT(*) > 100000
ORDER BY
    l DESC
LIMIT
    25;