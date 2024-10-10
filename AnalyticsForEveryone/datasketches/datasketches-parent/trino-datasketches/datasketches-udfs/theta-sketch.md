# Theta Sketches

A [Theta sketch](https://datasketches.apache.org/docs/Theta/ThetaSketchFramework.html) is a sketch that can estimate the count of distinct values in a dataset. 
It is less efficient than an HLL sketch, but it supports useful set operations (e.g. intersection).

(theta_sketch_1)=
## `theta_sketch(column)`

Parameters:
* `column` (`BIGINT`, `DOUBLE`, `REAL`, `VARCHAR`, `VARBINARY`): The column of values to create the sketch from. If inputs are `VARBINARY`, they are assumed to be serialized sketches which
  are unioned to produce the output sketch.

Returns:
* (`VARBINARY`): The serialized theta sketch.

Notes:
* This is an aggregation function, so a column will be reduced to a single `VARBINARY` value.
* The output sketch will use a `k` of 4096. This can cause issues if inputs are serialized sketches with `k != 4096`. To customize this, use [](theta_sketch_2).

Examples:
```sql
-- user_id is a VARCHAR column
SELECT theta_sketch(user_id) AS users_sketch
-- Output: 0x... (VARBINARY)
```

```sql
-- user_id_hll is a VARBINARY column
SELECT theta_sketch(user_id_theta) AS users_sketch
-- Output: 0x... (VARBINARY)
```


(theta_sketch_2)=
## `theta_sketch(column, k)`

Parameters:
* `column` (`BIGINT`, `DOUBLE`, `REAL`, `VARCHAR`, `VARBINARY`): The column of values to create the sketch from. If inputs are `VARBINARY`, they are assumed to be serialized sketches which
  are unioned to produce the output sketch.
* `k` (`BIGINT`): The desired sketch's `k` parameter. `k` must be a power of 2 at most 16384.

Returns:
* (`VARBINARY`): The serialized theta sketch with the specified `k`.

Notes:
* This is an aggregation function, so a column will be reduced to a single `VARBINARY` value.

Examples:
```sql
-- user_id is a VARCHAR column
SELECT theta_sketch(user_id, 2048) AS users_sketch
-- Output: 0x... (VARBINARY)
```

```sql
-- user_id_hll is a VARBINARY column
SELECT theta_sketch(user_id_theta, 2048) AS users_sketch
-- Output: 0x... (VARBINARY)
```


(theta_count_distinct_1)=
## `theta_count_distinct(sketch)`

Parameters:
* `sketch` (`VARBINARY`): A serialized theta sketch with default `k`.

Returns:
* (`BIGINT`): The estimated count of distinct values in `sketch`.

Examples:
```sql
-- user_id is a VARCHAR column
SELECT theta_count_distinct(theta_sketch(user_id)) AS num_distinct_users
-- Output: 9996 (BIGINT)
```

Notes:
* This assumes the sketch was built with the default `k = 4096`. If the sketch was built with a different `k`, use [](theta_count_distinct_2).


(theta_count_distinct_2)=
## `theta_count_distinct(sketch, k)`

Parameters:
* `sketch` (`VARBINARY`): A serialized theta sketch.
* `k` (`BIGINT`): The desired sketch's `k` parameter.

Returns:
* (`BIGINT`): The estimated count of distinct values in `sketch`.

Examples:
```sql
-- user_id is a VARCHAR column
SELECT theta_count_distinct(theta_sketch(user_id, 2048), 2048) AS num_distinct_users
-- Output: 9996 (BIGINT)
```


(theta_count_distinct_lb_1)=
## `theta_count_distinct_lb(sketch, num_std_dev)`

Parameters:
* `sketch` (`VARBINARY`): A serialized theta sketch with default `k`.
* `num_std_dev` (`BIGINT`): The number of standard deviations to use for the lower bound.

Returns:
* (`BIGINT`): The lower bound on the count of distinct values in `sketch` to `num_std_dev` standard deviations.

Examples:
```sql
-- user_id is a VARCHAR column
SELECT theta_count_distinct_lb(theta_sketch(user_id), 2) AS num_distinct_users_lb
-- Output: 9690 (BIGINT)
```

Notes:
* This assumes the sketch was built with the default `k = 4096`. If the sketch was built with a different `k`, use [](theta_count_distinct_lb_2).


(theta_count_distinct_lb_2)=
## `theta_count_distinct_lb(sketch, num_std_dev, k)`

Parameters:
* `sketch` (`VARBINARY`): A serialized theta sketch.
* `num_std_dev` (`BIGINT`): The number of standard deviations to use for the lower bound.
* `k` (`BIGINT`): The input sketch's `k` parameter.

Returns:
* (`BIGINT`): The lower bound on the count of distinct values in `sketch` to `num_std_dev` standard deviations.

Examples:
```sql
-- user_id is a VARCHAR column
SELECT theta_count_distinct_lb(theta_sketch(user_id, 2048), 2, 2048) AS num_distinct_users_lb
-- Output: 9690 (BIGINT)
```


(theta_count_distinct_ub_1)=
## `theta_count_distinct_ub(sketch, num_std_dev)`

Parameters:
* `sketch` (`VARBINARY`): A serialized theta sketch with default `k`.
* `num_std_dev` (`BIGINT`): The number of standard deviations to use for the upper bound.

Returns:
* (`BIGINT`): The upper bound on the count of distinct values in `sketch` to `num_std_dev` standard deviations.

Examples:
```sql
-- user_id is a VARCHAR column
SELECT theta_count_distinct_ub(theta_sketch(user_id), 2) AS num_distinct_users_ub
-- Output: 10398 (BIGINT)
```

Notes:
* This assumes the sketch was built with the default `k = 4096`. If the sketch was built with a different `k`, use [](theta_count_distinct_ub_2).


(theta_count_distinct_ub_2)=
## `theta_count_distinct_ub(sketch, num_std_dev, k)`

Parameters:
* `sketch` (`VARBINARY`): A serialized theta sketch.
* `num_std_dev` (`BIGINT`): The number of standard deviations to use for the upper bound.
* `k` (`BIGINT`): The input sketch's `k` parameter.

Returns:
* (`BIGINT`): The upper bound on the count of distinct values in `sketch` to `num_std_dev` standard deviations.

Examples:
```sql
-- user_id is a VARCHAR column
SELECT theta_count_distinct_ub(theta_sketch(user_id, 2048), 2, 2048) AS num_distinct_users_ub
-- Output: 10398 (BIGINT)
```


(theta_union_1)=
## `theta_union(sketch1, sketch2)`

Parameters:
* `sketch1` (`VARBINARY`): A serialized theta sketch with default `k`.
* `sketch2` (`VARBINARY`): A serialized theta sketch with default `k`.

Returns:
* (`VARBINARY`): The union of `sketch1` and `sketch2`.

Examples:
```sql
-- user_id_1, user_id_2 are VARCHAR columns
SELECT theta_union(theta_sketch(user_id_1), theta_sketch(user_id_2)) AS users_sketch
-- Output: 0x... (VARBINARY)
```

Notes:
* This assumes the input sketches were built with the default `k = 4096`. If the input sketches were built with a different `k`, use [](theta_union_2).


(theta_union_2)=
## `theta_union(sketch1, sketch2, k)`

Parameters:
* `sketch1` (`VARBINARY`): A serialized theta sketch.
* `sketch2` (`VARBINARY`): A serialized theta sketch.
* `k` (`BIGINT`): The input sketches' `k` parameter.

Returns:
* (`VARBINARY`): The union of `sketch1` and `sketch2` with given `k`.

Examples:
```sql
-- user_id_1, user_id_2 are VARCHAR columns
SELECT theta_union(theta_sketch(user_id_1, 2048), theta_sketch(user_id_2, 2048), 2048) AS users_sketch
-- Output: 0x... (VARBINARY)
```


(theta_intersection_1)=
## `theta_intersection(sketch1, sketch2)`

Parameters:
* `sketch1` (`VARBINARY`): A serialized theta sketch with default `k`.
* `sketch2` (`VARBINARY`): A serialized theta sketch with default `k`.

Returns:
* (`VARBINARY`): The intersection of `sketch1` and `sketch2`.

Examples:
```sql
-- user_id_1, user_id_2 are VARCHAR columns
SELECT theta_intersection(theta_sketch(user_id_1), theta_sketch(user_id_2)) AS users_sketch
-- Output: 0x... (VARBINARY)
```

Notes:
* This assumes the input sketches were built with the default `k = 4096`. If the input sketches were built with a different `k`, use [](theta_intersection_2).


(theta_intersection_2)=
## `theta_intersection(sketch1, sketch2, k)`

Parameters:
* `sketch1` (`VARBINARY`): A serialized theta sketch.
* `sketch2` (`VARBINARY`): A serialized theta sketch.
* `k` (`BIGINT`): The input sketches' `k` parameter.

Returns:
* (`VARBINARY`): The intersection of `sketch1` and `sketch2` with given `k`.

Examples:
```sql
-- user_id_1, user_id_2 are VARCHAR columns
SELECT theta_intersection(theta_sketch(user_id_1, 2048), theta_sketch(user_id_2, 2048), 2048) AS users_sketch
-- Output: 0x... (VARBINARY)
```