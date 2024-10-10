# KLL Floats Sketches

A KLL Floats sketch is a [KLL sketch](https://datasketches.apache.org/docs/KLL/KLLSketch.html) that stores float values. It can be used to estimate quantiles and ranks. It has lower precision than a KLL Doubles sketch, but [requires less memory](https://datasketches.apache.org/docs/KLL/KLLAccuracyAndSize.html).


(kll_floats_sketch_1)=
## `kll_floats_sketch(column)`

Parameters:
* `column` (`BIGINT`, `DOUBLE`, `REAL`, `VARBINARY`): The column of values to create the sketch from. If inputs are `VARBINARY`, they are assumed to be serialized sketches which
  are unioned to produce the output sketch.

Returns:
* (`VARBINARY`): The serialized KLL floats sketch.

Notes:
* This is an aggregation function, so a column will be reduced to a single `VARBINARY` value.
* When creating a new sketch from values, the output sketch will use a `k` of 200. To customize this, use [](kll_floats_sketch_2).
* When aggregating existing sketches, the output sketch will use the same `k` as one of the input sketches. Ensure all input sketches have the same `k` to avoid errors.

Examples:
```sql
-- hours_played is a DOUBLE column
SELECT kll_floats_sketch(hours_played) AS playtime_sketch
-- Output: 0x... (VARBINARY)
```

```sql
-- hours_played_kll is a VARBINARY column
SELECT kll_floats_sketch(hours_played_kll) AS playtime_sketch
-- Output: 0x... (VARBINARY)
```


(kll_floats_sketch_2)=
## `kll_floats_sketch(column, k)`

Parameters:
* `column` (`BIGINT`, `DOUBLE`, `REAL`): The column of values to create the sketch from.
* `k` (`BIGINT`): The desired sketch's `k` parameter.

Returns:
* (`VARBINARY`): The serialized KLL floats sketch with the specified `k`.

Notes:
* This is an aggregation function, so a column will be reduced to a single `VARBINARY` value.

Examples:
```sql
-- hours_played is a DOUBLE column
SELECT kll_floats_sketch(hours_played, 150) AS playtime_sketch
-- Output: 0x... (VARBINARY)
```


(kll_floats_estimate_quantile)=
## `kll_floats_estimate_quantile(sketch, rank)`

Parameters:
* `sketch` (`VARBINARY`): A serialized KLL floats sketch.
* `rank` (`DOUBLE`, `ARRAY[DOUBLE]`): The desired normalized rank (0-1) or list of ranks to estimate the quantile for.

Returns:
* (`DOUBLE` or `ARRAY[DOUBLE]`): The estimated quantile or list of quantiles at the given rank in `sketch`.

Examples:
```sql
-- hours_played is a DOUBLE column
SELECT kll_floats_estimate_quantile(kll_floats_sketch(hours_played), 0.5) AS median_playtime
-- Output: 1.65 (DOUBLE)
```

```sql
-- hours_played is a DOUBLE column
SELECT kll_floats_estimate_quantile(kll_floats_sketch(hours_played), ARRAY[0.25, 0.5, 0.75]) AS quartiles
-- Output: [1.0, 1.65, 2.5] (ARRAY[DOUBLE])
```


(kll_floats_estimate_quantile_lb)=
## `kll_floats_estimate_quantile_lb(sketch, rank)`

Parameters:
* `sketch` (`VARBINARY`): A serialized KLL floats sketch.
* `rank` (`DOUBLE`, `ARRAY[DOUBLE]`): The desired normalized rank (0-1) or list of ranks to find the lower bound of the quantile for.

Returns:
* (`DOUBLE` or `ARRAY[DOUBLE]`): The lower bound of the quantile or list of quantiles at the given rank in `sketch`.

Examples:
```sql
-- hours_played is a DOUBLE column
SELECT kll_floats_estimate_quantile_lb(kll_floats_sketch(hours_played), 0.5) AS median_playtime_lb
-- Output: 1.51 (DOUBLE)
```

```sql
-- hours_played is a DOUBLE column
SELECT kll_floats_estimate_quantile_lb(kll_floats_sketch(hours_played), ARRAY[0.25, 0.5, 0.75]) AS quartiles_lb
-- Output: [0.81, 1.51, 2.12] (ARRAY[DOUBLE])
```


(kll_floats_estimate_quantile_ub)=
## `kll_floats_estimate_quantile_ub(sketch, rank)`

Parameters:
* `sketch` (`VARBINARY`): A serialized KLL floats sketch.
* `rank` (`DOUBLE`, `ARRAY[DOUBLE]`): The desired normalized rank (0-1) or list of ranks to find the upper bound of the quantile for.

Returns:
* (`DOUBLE` or `ARRAY[DOUBLE]`): The upper bound of the quantile or list of quantiles at the given rank in `sketch`.

Examples:
```sql
-- hours_played is a DOUBLE column
SELECT kll_floats_estimate_quantile_ub(kll_floats_sketch(hours_played), 0.5) AS median_playtime_ub
-- Output: 2.79 (DOUBLE)
```

```sql
-- hours_played is a DOUBLE column
SELECT kll_floats_estimate_quantile_ub(kll_floats_sketch(hours_played), ARRAY[0.25, 0.5, 0.75]) AS quartiles_ub
-- Output: [1.19, 2.79, 3.0] (ARRAY[DOUBLE])
```


(kll_floats_estimate_rank)=
## `kll_floats_estimate_rank(sketch, value)`

Parameters:
* `sketch` (`VARBINARY`): A serialized KLL floats sketch.
* `value` (`DOUBLE`, `REAL`, `ARRAY[DOUBLE]`, `ARRAY[REAL]`): The desired value or list of values to estimate the rank for.

Returns:
* (`DOUBLE` or `ARRAY[DOUBLE]`): The estimated normalized rank (0-1) or list of ranks of `value` in `sketch`.

Examples:
```sql
-- hours_played is a DOUBLE column
SELECT kll_floats_estimate_rank(kll_floats_sketch(hours_played), 1.0) AS rank_one_hr
-- Output: 0.39 (DOUBLE)
```

```sql
-- hours_played is a DOUBLE column
SELECT kll_floats_estimate_rank(kll_floats_sketch(hours_played), ARRAY[1.0, 2.0, 3.0]) AS ranks
-- Output: [0.39, 0.75, 0.92] (ARRAY[DOUBLE])
```


(kll_floats_estimate_rank_lb)=
## `kll_floats_estimate_rank_lb(sketch, value)`

Parameters:
* `sketch` (`VARBINARY`): A serialized KLL floats sketch.
* `value` (`DOUBLE`, `REAL`, `ARRAY[DOUBLE]`, `ARRAY[REAL]`): The desired value or list of values to find the lower bound of the rank for.

Returns:
* (`DOUBLE` or `ARRAY[DOUBLE]`): The lower bound of the normalized rank (0-1) or list of ranks of `value` in `sketch`.

Examples:
```sql
-- hours_played is a DOUBLE column
SELECT kll_floats_estimate_rank_lb(kll_floats_sketch(hours_played), 1.0) AS rank_one_hr_lb
-- Output: 0.32 (DOUBLE)
```

```sql
-- hours_played is a DOUBLE column
SELECT kll_floats_estimate_rank_lb(kll_floats_sketch(hours_played), ARRAY[1.0, 2.0, 3.0]) AS ranks_lb
-- Output: [0.32, 0.65, 0.82] (ARRAY[DOUBLE])
```


(kll_floats_estimate_rank_ub)=
## `kll_floats_estimate_rank_ub(sketch, value)`

Parameters:
* `sketch` (`VARBINARY`): A serialized KLL floats sketch.
* `value` (`DOUBLE`, `REAL`, `ARRAY[DOUBLE]`, `ARRAY[REAL]`): The desired value or list of values to find the upper bound of the rank for.

Returns:
* (`DOUBLE` or `ARRAY[DOUBLE]`): The upper bound of the normalized rank (0-1) or list of ranks of `value` in `sketch`.

Examples:
```sql
-- hours_played is a DOUBLE column
SELECT kll_floats_estimate_rank_ub(kll_floats_sketch(hours_played), 1.0) AS rank_one_hr_ub
-- Output: 0.45 (DOUBLE)
```

```sql
-- hours_played is a DOUBLE column
SELECT kll_floats_estimate_rank_ub(kll_floats_sketch(hours_played), ARRAY[1.0, 2.0, 3.0]) AS ranks_ub
-- Output: [0.45, 0.78, 0.95] (ARRAY[DOUBLE])
```