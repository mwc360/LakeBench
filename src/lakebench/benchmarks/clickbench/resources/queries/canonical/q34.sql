SELECT
    URL,
    COUNT(*) AS c
FROM
    hits
GROUP BY
    URL
ORDER BY
    c DESC
LIMIT
    10;