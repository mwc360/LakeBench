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
    AND NOT (URL LIKE '%.google.%') /* `NOT` operator requires parenthesis https://github.com/lakehq/sail/issues/729 */
    AND SearchPhrase <> ''
GROUP BY
    SearchPhrase
ORDER BY
    c DESC
LIMIT
    10;