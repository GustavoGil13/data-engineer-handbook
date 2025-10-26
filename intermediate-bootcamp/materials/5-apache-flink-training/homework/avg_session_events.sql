-- Average events per session for Tech Creator (any host under techcreator.io)
SELECT ROUND(AVG(num_hits)::numeric, 2) AS avg_events_per_session_techcreator
FROM processed_events_by_ip_host
WHERE lower(host) = 'techcreator.io' OR lower(host) LIKE '%.techcreator.io';

-- Comparison across the specified hosts
SELECT lower(host) AS host,
     ROUND(AVG(num_hits)::numeric, 2) AS avg_events_per_session,
     COUNT(*) AS session_count,
     SUM(num_hits) AS total_events
FROM processed_events_by_ip_host
WHERE lower(host) IN ('zachwilson.techcreator.io', 'zachwilson.tech', 'lulu.techcreator.io')
GROUP BY lower(host)
ORDER BY host;