SELECT activity, count(*) AS observations, 'readiness_only' AS status
FROM _snapshot
GROUP BY activity
ORDER BY activity
