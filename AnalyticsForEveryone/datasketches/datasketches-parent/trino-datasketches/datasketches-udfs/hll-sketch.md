# HLL Sketches

An [HLL sketch](https://datasketches.apache.org/docs/HLL/HLL.html), or HyperLogLog sketch, is a sketch that can estimate the count of distinct values in a dataset.

(hll_sketch_1)=
## `hll_sketch(column)`

Parameters:
* `column` (`BIGINT`, `DOUBLE`, `REAL`, `VARCHAR`, `VARBINARY`): The column of values to create the sketch from. If inputs are `VARBINARY`, they are assumed to be serialized sketches which
are unioned to produce the output sketch.

Returns:
* (`VARBINARY`): The serialized HLL sketch.

Notes:
* This is an aggregation function, so a column will be reduced to a single `VARBINARY` value.
* When creating a new sketch from values, the output sketch will use a `lg_k` of 12. To customize this, use [](hll_sketch_2).
* When aggregating existing sketches, the output sketch will use the same `lg_k` as one of the input sketches. Ensure all input sketches have the same `lg_k` to avoid errors.

Examples:
```sql
-- user_id is a VARCHAR column
SELECT hll_sketch(user_id) AS users_sketch
-- Output: 0x... (VARBINARY)
```

```sql
-- user_id_hll is a VARBINARY column
SELECT hll_sketch(user_id_hll) AS users_sketch
-- Output: 0x... (VARBINARY)
```


(hll_sketch_2)=
## `hll_sketch(column, lg_k)`

Parameters:
* `column` (`BIGINT`, `DOUBLE`, `REAL`, `VARCHAR`): The column of values to create the sketch from.
* `lg_k` (`BIGINT`): The log2 of the desired sketch's `k` parameter. `lg_k` can be between 4 and 21.

Returns:
* (`VARBINARY`): The serialized HLL sketch with the specified `lg_k`.

Notes:
* This is an aggregation function, so a column will be reduced to a single `VARBINARY` value.

Examples:
```sql
-- user_id is a VARCHAR column
SELECT hll_sketch(user_id, 14) AS users_sketch
-- Output: 0x... (VARBINARY)
```


(hll_count_distinct)=
## `hll_count_distinct(sketch)`

Parameters:
* `sketch` (`VARBINARY`): A serialized HLL sketch.

Returns:
* (`BIGINT`): The estimated count of distinct values in `sketch`.

Examples:
```sql
-- user_id is a VARCHAR column
SELECT hll_count_distinct(hll_sketch(user_id)) AS num_distinct_users
-- Output: 9996 (BIGINT)
```


(hll_count_distinct_lb)=
## `hll_count_distinct_lb(sketch, num_std_dev)`

Parameters:
* `sketch` (`VARBINARY`): A serialized HLL sketch.
* `num_std_dev` (`BIGINT`): The number of standard deviations to use for the lower bound.

Returns:
* (`BIGINT`): The lower bound on the count of distinct values in `sketch` to `num_std_dev` standard deviations.

Examples:
```sql
-- user_id is a VARCHAR column
SELECT hll_count_distinct_lb(hll_sketch(user_id), 2) AS num_distinct_users_lb
-- Output: 9751 (BIGINT)
```


(hll_count_distinct_ub)=
## `hll_count_distinct_ub(sketch, num_std_dev)`

Parameters:
* `sketch` (`VARBINARY`): A serialized HLL sketch.
* `num_std_dev` (`BIGINT`): The number of standard deviations to use for the upper bound.

Returns:
* (`BIGINT`): The upper bound on the count of distinct values in `sketch` to `num_std_dev` standard deviations.

Examples:
```sql
-- user_id is a VARCHAR column
SELECT hll_count_distinct_ub(hll_sketch(user_id), 2) AS num_distinct_users_ub
-- Output: 10338 (BIGINT)
```