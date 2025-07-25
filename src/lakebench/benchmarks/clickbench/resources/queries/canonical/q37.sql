SELECT
    URL,
    COUNT(*) AS PageViews
FROM
    hits
WHERE
    CounterID = 62
    AND EventDate >= '2013-07-01'
    AND EventDate <= '2013-07-31'
    AND DontCountHits = 0
    AND IsRefresh = 0
    AND URL <> ''
GROUP BY
    URL
ORDER BY
    PageViews DESC
LIMIT
    10;