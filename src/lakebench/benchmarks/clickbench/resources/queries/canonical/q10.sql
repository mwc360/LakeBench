SELECT
    RegionID,
    SUM(AdvEngineID),
    COUNT(*) AS c,
    AVG(ResolutionWidth),
    COUNT(DISTINCT UserID)
FROM
    hits
GROUP BY
    RegionID
ORDER BY
    c DESC
LIMIT
    10;