# String Items Sketches

A String Items sketch is an [Items Sketch](https://datasketches.apache.org/docs/Frequency/FrequentItemsOverview.html) that stores String values. It can be used to estimate the frequency of items in a dataset.


(string_items_sketch_1)=
## `string_items_sketch(column)`

Parameters:
* `column` (`VARCHAR`, `VARBINARY`): The column of values to create the sketch from. If inputs are `VARBINARY`, they are assumed to be serialized sketches which
  are unioned to produce the output sketch.

Returns:
* (`VARBINARY`): The serialized String Items sketch.

Notes:
* This is an aggregation function, so a column will be reduced to a single `VARBINARY` value.
* When creating a new sketch from values, the output sketch will use a maximum map size of 64. To customize this, use [](string_items_sketch_2).
* When aggregating existing sketches, the output sketch will use the same maximum map size as one of the input sketches. Ensure all input sketches have the same maximum map size to avoid errors.

Examples:
```sql
-- game_reported is a VARCHAR column
SELECT string_items_sketch(game_reported) AS game_reports_sketch
-- Output: 0x... (VARBINARY)
```

```sql
-- game_reported_items_sketch is a VARBINARY column
SELECT string_items_sketch(game_reported_items_sketch) AS game_reports_sketch
-- Output: 0x... (VARBINARY)
```


(string_items_sketch_2)=
## `string_items_sketch(column, size)`

Parameters:
* `column` (`VARBINARY`): The column of values to create the sketch from.
* `size` (`BIGINT`): The desired sketch's maximum map size. Must be a power of 2.

Returns:
* (`VARBINARY`): The serialized String Items sketch with the specified maximum map size.

Notes:
* This is an aggregation function, so a column will be reduced to a single `VARBINARY` value.

Examples:
```sql
-- game_reported is a VARCHAR column
SELECT string_items_sketch(game_reported, 128) AS game_reports_sketch
-- Output: 0x... (VARBINARY)
```


(string_items_sketch_estimate)=
## `string_items_sketch_estimate(sketch, item)`

Parameters:
* `sketch` (`VARBINARY`): A serialized String Items sketch.
* `item` (`VARCHAR`, `ARRAY[VARCHAR]`): The item or list of items to estimate the frequency for.

Returns:
* (`BIGINT` or `ARRAY[BIGINT]`): The estimated frequency or list of frequencies of `item` in `sketch`.

Examples:
```sql
-- game_reported is a VARCHAR column
SELECT string_items_sketch_estimate(string_items_sketch(game_reported), 'obby') AS obby_report_frequency
-- Output: 106 (BIGINT)
```

```sql
-- game_reported is a VARCHAR column
SELECT string_items_sketch_estimate(string_items_sketch(game_reported), ARRAY['obby', 'adopt_me', 'jailbreak']) AS game_report_frequencies
-- Output: [106, 55, 239] (ARRAY[BIGINT])
```


(string_items_sketch_estimate_lb)=
## `string_items_sketch_estimate_lb(sketch, item)`

Parameters:
* `sketch` (`VARBINARY`): A serialized String Items sketch.
* `item` (`VARCHAR`, `ARRAY[VARCHAR]`): The item or list of items to estimate the lower bound of the frequency for.

Returns:
* (`BIGINT` or `ARRAY[BIGINT]`): The estimated lower bound of the frequency or list of frequencies of `item` in `sketch`.

Examples:
```sql
-- game_reported is a VARCHAR column
SELECT string_items_sketch_estimate_lb(string_items_sketch(game_reported), 'obby') AS obby_report_frequency_lb
-- Output: 99 (BIGINT)
```

```sql
-- game_reported is a VARCHAR column
SELECT string_items_sketch_estimate_lb(string_items_sketch(game_reported), ARRAY['obby', 'adopt_me', 'jailbreak']) AS game_report_frequencies_lb
-- Output: [99, 50, 220] (ARRAY[BIGINT])
```


(string_items_sketch_estimate_ub)=
## `string_items_sketch_estimate_ub(sketch, item)`

Parameters:
* `sketch` (`VARBINARY`): A serialized String Items sketch.
* `item` (`VARCHAR`, `ARRAY[VARCHAR]`): The item or list of items to estimate the upper bound of the frequency for.

Returns:
* (`BIGINT` or `ARRAY[BIGINT]`): The estimated upper bound of the frequency or list of frequencies of `item` in `sketch`.

Examples:
```sql
-- game_reported is a VARCHAR column
SELECT string_items_sketch_estimate_ub(string_items_sketch(game_reported), 'obby') AS obby_report_frequency_ub
-- Output: 113 (BIGINT)
```

```sql
-- game_reported is a VARCHAR column
SELECT string_items_sketch_estimate_ub(string_items_sketch(game_reported), ARRAY['obby', 'adopt_me', 'jailbreak']) AS game_report_frequencies_ub
-- Output: [113, 60, 250] (ARRAY[BIGINT])
```


(string_items_sketch_frequent_items)=
## `string_items_sketch_frequent_items(sketch, false_positives)`

Parameters:
* `sketch` (`VARBINARY`): A serialized String Items sketch.
* `false_positives` (`BOOLEAN`): Whether to include potential false positives in the output. If this is true, the output will include all items that may be frequent. If this is 
false, the output will only include items that are guaranteed to be frequent.

Returns:
* (`ARRAY[VARCHAR]`): The frequent items in the sketch.

Examples:
```sql
-- game_reported is a VARCHAR column
SELECT string_items_sketch_frequent_items(string_items_sketch(game_reported), true) AS frequently_reported_games
-- Output: ['obby', 'adopt_me', 'jailbreak', 'blox_fruits']
```

```sql
-- game_reported is a VARCHAR column
SELECT string_items_sketch_frequent_items(string_items_sketch(game_reported), false) AS frequently_reported_games
-- Output: ['obby', 'adopt_me']
```


