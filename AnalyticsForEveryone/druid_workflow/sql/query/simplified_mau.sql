SELECT 
    "Engagement.time_series"."date_to" AS "engagement__time_day",
    CASE 
        WHEN MAX(__time) = MAX(date_to) THEN
            APPROX_COUNT_DISTINCT_DS_HLL(users_hll)
    END AS "engagement__mau"
FROM 
    players_flat_rollup
LEFT JOIN (
    SELECT 
        TIME_PARSE(date_to) AS "date_to"
    FROM (
        VALUES ('2024-01-01T00:00:00.000')
    ) AS dates (date_to)
) AS "Engagement.time_series" 
    ON 1 = 1  -- Always true condition for cross join
WHERE 
    universe_id = '1234'
    AND (
        __time > TIMESTAMPADD(day, -30, TIME_PARSE('2024-01-01'))
        AND __time <= TIME_PARSE('2024-01-01')
    )
    AND __time > TIMESTAMPADD(day, -30, "Engagement.time_series"."date_to")
    AND __time <= "Engagement.time_series"."date_to"
GROUP BY date_to