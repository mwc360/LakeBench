SELECT
    RegionID,
    COUNT(DISTINCT UserID) AS u
FROM
    hits
GROUP BY
    RegionID
ORDER BY
    u DESC
LIMIT
    10;