SELECT
    UserID,
    COUNT(*)
FROM
    hits
GROUP BY
    UserID
ORDER BY
    COUNT(*) DESC
LIMIT
    10;