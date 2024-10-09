REPLACE INTO "players_sketch"  OVERWRITE WHERE __time >= TIMESTAMP '2024-01-01' and __time < TIMESTAMP '2024-01-02' 
WITH "ext" AS (
  SELECT *
  FROM TABLE(
    EXTERN(
      '{"type":"local","baseDir":"/opt/data/players_sketch","filter":"*.parquet"}',
      '{"type":"parquet"}'
    )
  ) EXTEND ("userkey_sketch" VARCHAR, "country" BIGINT, "visits" BIGINT, "time_spent_secs" BIGINT, "gender" BIGINT, "age_group" BIGINT, "is_new_user" BIGINT, "universe_id" BIGINT, "locale" BIGINT, "platform" BIGINT, "userkey_hll" VARCHAR, "ds" VARCHAR)
)
SELECT
  time_parse(ds, 'yyyy-MM-dd') as __time,
  COMPLEX_DECODE_BASE64('thetaSketch', "userkey_sketch") as users_theta,
  "country",
  "visits",
  "time_spent_secs",
  "gender",
  "age_group",
  "is_new_user",
  "universe_id",
  "locale",
  "platform",
  COMPLEX_DECODE_BASE64('HLLSketch', "userkey_hll") as users_hll
FROM "ext"
PARTITIONED BY DAY CLUSTERED BY universe_id, age_group, platform