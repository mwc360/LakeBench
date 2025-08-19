SELECT
    SearchPhrase,
    MIN(URL),
    MIN(Title),
    COUNT(*) AS c,
    COUNT(DISTINCT UserID)
FROM
    hits
WHERE
    Title LIKE '%Google%'
    AND NOT (URL LIKE '%.google.%') /*SQLGlot transpiles column NOT LIKE to NOT column LIKE which fails in Sail*/
    AND SearchPhrase <> ''
GROUP BY
    SearchPhrase
ORDER BY
    c DESC
LIMIT
    10;