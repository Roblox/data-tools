-- An actual MAU query we'd use to calculate 7 days of MAU for a given universe 1234

SELECT
  q_0."engagement__time_day",
  "engagement__mau" "engagement__mau"
FROM
  (
    SELECT
      "Engagement.time_series"."date_from" "engagement__time_day",
      CASE
        WHEN (
          MAX("engagement_mau_cumulative__engagement".__time) = MAX(date_from)
        ) THEN APPROX_COUNT_DISTINCT_DS_HLL(active_users_hll, 14)
      END "engagement__mau"
    FROM
      (
        SELECT
          DATE_TRUNC(
            'day',
            "engagement_mau_cumulative__engagement".__time
          ) "engagement__time_day",
          *
        FROM
          (
            SELECT
              *
            FROM
              druid.dev_engagement_low_cardinality_dims
          ) AS "engagement_mau_cumulative__engagement"
      ) AS "engagement_mau_cumulative__engagement"
      LEFT JOIN (
        SELECT
          TIME_PARSE(date_from) as "date_from",
          TIME_PARSE(date_to) as "date_to"
        FROM
          (
            VALUES
              (
                '2024-09-30T00:00:00.000',
                '2024-09-30T23:59:59.999'
              ),
              (
                '2024-10-01T00:00:00.000',
                '2024-10-01T23:59:59.999'
              ),
              (
                '2024-10-02T00:00:00.000',
                '2024-10-02T23:59:59.999'
              ),
              (
                '2024-10-03T00:00:00.000',
                '2024-10-03T23:59:59.999'
              ),
              (
                '2024-10-04T00:00:00.000',
                '2024-10-04T23:59:59.999'
              ),
              (
                '2024-10-05T00:00:00.000',
                '2024-10-05T23:59:59.999'
              ),
              (
                '2024-10-06T00:00:00.000',
                '2024-10-06T23:59:59.999'
              ),
              (
                '2024-10-07T00:00:00.000',
                '2024-10-07T23:59:59.999'
              )
          ) AS dates (date_from, date_to)
      ) AS "Engagement.time_series" ON 1 = 1
    WHERE
      (
        "engagement_mau_cumulative__engagement".universe_id = '1234'
      )
      AND (
        "engagement_mau_cumulative__engagement".__time > TIMESTAMPADD(day, -30, TIME_PARSE('2024-01-01'))
        AND "engagement_mau_cumulative__engagement".__time <= TIME_PARSE('2024-01-07')
      )
      AND "engagement_mau_cumulative__engagement"."engagement__time_day" > TIMESTAMPADD(day, -30, "Engagement.time_series"."date_to")
      AND "engagement_mau_cumulative__engagement"."engagement__time_day" <= "Engagement.time_series"."date_to"
    GROUP BY
      1
  ) as q_0
ORDER BY
  1 ASC
LIMIT
  10000