# Long Items Sketches

A Long Items sketch is an [Items Sketch](https://datasketches.apache.org/docs/Frequency/FrequentItemsOverview.html) that stores Long values. It can be used to estimate the frequency of items in a dataset.


(long_items_sketch_1)=
## `long_items_sketch(column)`

Parameters:
* `column` (`BIGINT`, `VARBINARY`): The column of values to create the sketch from. If inputs are `VARBINARY`, they are assumed to be serialized sketches which
  are unioned to produce the output sketch.

Returns:
* (`VARBINARY`): The serialized Long Items sketch.

Notes:
* This is an aggregation function, so a column will be reduced to a single `VARBINARY` value.
* When creating a new sketch from values, the output sketch will use a maximum map size of 64. To customize this, use [](long_items_sketch_2).
* When aggregating existing sketches, the output sketch will use the same maximum map size as one of the input sketches. Ensure all input sketches have the same maximum map size to avoid errors.

Examples:
```sql
-- game_id is a BIGINT column
SELECT long_items_sketch(game_id) AS game_id_sketch
-- Output: 0x... (VARBINARY)
```

```sql
-- game_id_items_sketch is a BIGINT column
SELECT long_items_sketch(game_id_items_sketch) AS game_id_sketch
-- Output: 0x... (VARBINARY)
```


(long_items_sketch_2)=
## `long_items_sketch(column, size)`

Parameters:
* `column` (`VARBINARY`): The column of values to create the sketch from.
* `size` (`BIGINT`): The desired sketch's maximum map size. Must be a power of 2.

Returns:
* (`VARBINARY`): The serialized Long Items sketch with the specified maximum map size.

Notes:
* This is an aggregation function, so a column will be reduced to a single `VARBINARY` value.

Examples:
```sql
-- game_id is a BIGINT column
SELECT long_items_sketch(game_id, 128) AS game_id_sketch
-- Output: 0x... (VARBINARY)
```


(long_items_sketch_estimate)=
## `long_items_sketch_estimate(sketch, item)`

Parameters:
* `sketch` (`VARBINARY`): A serialized Long Items sketch.
* `item` (`BIGINT`, `ARRAY[BIGINT]`): The item or list of items to estimate the frequency for.

Returns:
* (`BIGINT` or `ARRAY[BIGINT]`): The estimated frequency or list of frequencies of `item` in `sketch`.

Examples:
```sql
-- game_id is a BIGINT column
SELECT long_items_sketch_estimate(long_items_sketch(game_id), 67319) AS game_frequency
-- Output: 126 (BIGINT)
```

```sql
-- game_id is a BIGINT column
SELECT long_items_sketch_estimate(long_items_sketch(game_id), ARRAY[67319, 8812, 91043]) AS game_frequencies
-- Output: [126, 83, 144] (ARRAY[BIGINT])
```


(long_items_sketch_estimate_lb)=
## `long_items_sketch_estimate_lb(sketch, item)`

Parameters:
* `sketch` (`VARBINARY`): A serialized Long Items sketch.
* `item` (`BIGINT`, `ARRAY[BIGINT]`): The item or list of items to estimate the lower bound of the frequency for.

Returns:
* (`BIGINT` or `ARRAY[BIGINT]`): The estimated lower bound of the frequency or list of frequencies of `item` in `sketch`.

Examples:
```sql
-- game_id is a BIGINT column
SELECT long_items_sketch_estimate_lb(long_items_sketch(game_id), 67319) AS game_frequency_lb
-- Output: 112 (BIGINT)
```

```sql
-- game_id is a BIGINT column
SELECT long_items_sketch_estimate_lb(long_items_sketch(game_id), ARRAY[67319, 8812, 91043]) AS game_frequencies_lb
-- Output: [112, 70, 121] (ARRAY[BIGINT])
```


(long_items_sketch_estimate_ub)=
## `long_items_sketch_estimate_ub(sketch, item)`

Parameters:
* `sketch` (`VARBINARY`): A serialized Long Items sketch.
* `item` (`BIGINT`, `ARRAY[BIGINT]`): The item or list of items to estimate the upper bound of the frequency for.

Returns:
* (`BIGINT` or `ARRAY[BIGINT]`): The estimated upper bound of the frequency or list of frequencies of `item` in `sketch`.

Examples:
```sql
-- game_id is a BIGINT column
SELECT long_items_sketch_estimate_ub(long_items_sketch(game_id), 67319) AS game_frequency_ub
-- Output: 138 (BIGINT)
```

```sql
-- game_id is a BIGINT column
SELECT long_items_sketch_estimate_ub(long_items_sketch(game_id), ARRAY[67319, 8812, 91043]) AS game_frequencies_ub
-- Output: [138, 95, 161] (ARRAY[BIGINT])
```


(long_items_sketch_frequent_items)=
## `long_items_sketch_frequent_items(sketch, false_positives)`

Parameters:
* `sketch` (`VARBINARY`): A serialized Long Items sketch.
* `false_positives` (`BOOLEAN`): Whether to include potential false positives in the output. If this is true, the output will include all items that may be frequent. If this is
  false, the output will only include items that are guaranteed to be frequent.

Returns:
* (`ARRAY[BIGINT]`): The frequent items in the sketch.

Examples:
```sql
-- game_id is a BIGINT column
SELECT long_items_sketch_frequent_items(long_items_sketch(game_id), true) AS frequent_games
-- Output: [67319, 8812, 91043, 3340]
```

```sql
-- game_id is a BIGINT column
SELECT long_items_sketch_frequent_items(long_items_sketch(game_id), false) AS frequent_games
-- Output: [67319, 8812]
```


