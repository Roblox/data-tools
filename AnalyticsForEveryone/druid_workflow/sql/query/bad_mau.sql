SELECT 
"time_series".date_from "created_at_day",
country,
THETA_SKETCH_ESTIMATE(DS_THETA(active_users))
FROM (
  SELECT TIME_PARSE(date_from) as "date_from", TIME_PARSE(date_to) as "date_to"
    FROM(
    VALUES
              (
                '2023-05-01T00:00:00.000',
                '2023-05-01T23:59:59.999'
              ),
              (
                '2023-05-02T00:00:00.000',
                '2023-05-02T23:59:59.999'
              ),
              (
                '2023-05-03T00:00:00.000',
                '2023-05-03T23:59:59.999'
              ),
              (
                '2023-05-04T00:00:00.000',
                '2023-05-04T23:59:59.999'
              ),
              (
                '2023-05-05T00:00:00.000',
                '2023-05-05T23:59:59.999'
              ),
              (
                '2023-05-06T00:00:00.000',
                '2023-05-06T23:59:59.999'
              ),
              (
                '2023-05-07T00:00:00.000',
                '2023-05-07T23:59:59.999'
              )
          ) AS dates (date_from, date_to)
      ) AS "time_series" LEFT JOIN (
        SELECT  DATE_TRUNC('day', "mau_test".__time) "mau_test_created_at_day", country,
        DS_THETA("mau_test".active_users_theta) "active_users" FROM(
          select * from dev_engagement_all_dims where universe_id = '123456' and country in ('0', '100', '104', '108', '112', '116', '12', '120', '124', '132')
        ) as "mau_test" WHERE
        "mau_test".__time >= TIMESTAMPADD(day, -30, TIME_PARSE('2023-05-01')) AND
        "mau_test".__time <= TIME_PARSE('2023-05-07') group by 1, 2
      ) AS "mau_test_base" ON 1 = 1 WHERE
      "mau_test_base"."mau_test_created_at_day" >= TIMESTAMPADD(day, -30, "time_series"."date_from")
      AND "mau_test_base"."mau_test_created_at_day" < "time_series"."date_from"
      GROUP BY 1, 2