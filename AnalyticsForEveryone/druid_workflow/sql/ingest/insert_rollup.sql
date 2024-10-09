REPLACE INTO "players_flat_rollup"  OVERWRITE WHERE __time >= TIMESTAMP '2024-01-01' and __time < TIMESTAMP '2024-01-02' 
WITH "ext" AS (
  SELECT *
  FROM TABLE(
    EXTERN(
      '{"type":"local","baseDir":"/opt/data/players_flat","filter":"*.parquet"}',
      '{"type":"parquet"}'
    )
  ) EXTEND ("userkey" BIGINT, "country" BIGINT, "visits" BIGINT, "time_spent_secs" BIGINT, "gender" BIGINT, "age_group" BIGINT, "is_new_user" BIGINT, "universe_id" BIGINT, "locale" BIGINT, "platform" BIGINT, "os" BIGINT, "ds" VARCHAR)
)
SELECT
  time_parse(ds, 'yyyy-MM-dd') as __time,
  "universe_id",
  "country",
  "gender",
  "age_group",
  "is_new_user",
  "locale",
  "platform",
  "os",
  SUM("visits") as "visits",
  SUM("time_spent_secs") as "time_spent_secs",
  DS_THETA("userkey") as users_theta,
  DS_HLL("userkey") as users_hll
FROM "ext"
group by 1, 2, 3, 4, 5, 6, 7, 8, 9
PARTITIONED BY DAY CLUSTERED BY universe_id, age_group, platform, os