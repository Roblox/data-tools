# Double Items Sketches

A Double Items sketch is an [Items Sketch](https://datasketches.apache.org/docs/Frequency/FrequentItemsOverview.html) that stores Double values. It can be used to estimate the frequency of items in a dataset.

Note that `DOUBLE` and `REAL` items should not be used together in a Double Items sketch. The difference in precision causes items that should be the same to be considered distinct (e.g. `DOUBLE 4 != REAL 4`).


(double_items_sketch_1)=
## `double_items_sketch(column)`

Parameters:
* `column` (`DOUBLE`, `REAL`, `VARBINARY`): The column of values to create the sketch from. If inputs are `VARBINARY`, they are assumed to be serialized sketches which
  are unioned to produce the output sketch.

Returns:
* (`VARBINARY`): The serialized Double Items sketch.

Notes:
* This is an aggregation function, so a column will be reduced to a single `VARBINARY` value.
* When creating a new sketch from values, the output sketch will use a maximum map size of 64. To customize this, use [](double_items_sketch_2).
* When aggregating existing sketches, the output sketch will use the same maximum map size as one of the input sketches. Ensure all input sketches have the same maximum map size to avoid errors.

Examples:
```sql
-- item_price is a DOUBLE column
SELECT double_items_sketch(item_price) AS item_price_sketch
-- Output: 0x... (VARBINARY)
```

```sql
-- item_price_items_sketch is a VARBINARY column
SELECT double_items_sketch(item_price_items_sketch) AS item_price_sketch
-- Output: 0x... (VARBINARY)
```


(double_items_sketch_2)=
## `double_items_sketch(column, size)`

Parameters:
* `column` (`VARBINARY`): The column of values to create the sketch from.
* `size` (`BIGINT`): The desired sketch's maximum map size. Must be a power of 2.

Returns:
* (`VARBINARY`): The serialized Double Items sketch with the specified maximum map size.

Notes:
* This is an aggregation function, so a column will be reduced to a single `VARBINARY` value.

Examples:
```sql
-- item_price is a DOUBLE column
SELECT double_items_sketch(item_price, 128) AS item_price_sketch
-- Output: 0x... (VARBINARY)
```


(double_items_sketch_estimate)=
## `double_items_sketch_estimate(sketch, item)`

Parameters:
* `sketch` (`VARBINARY`): A serialized Double Items sketch.
* `item` (`DOUBLE`, `REAL`, `ARRAY[DOUBLE]`, `ARRAY[REAL]`): The item or list of items to estimate the frequency for.

Returns:
* (`BIGINT` or `ARRAY[BIGINT]`): The estimated frequency or list of frequencies of `item` in `sketch`.

Examples:
```sql
-- item_price is a DOUBLE column
SELECT double_items_sketch_estimate(double_items_sketch(item_price), 3.99) AS price1_frequency
-- Output: 14 (BIGINT)
```

```sql
-- item_price is a DOUBLE column
SELECT double_items_sketch_estimate(double_items_sketch(item_price), ARRAY[3.99, 7.49, 12.95]) AS price_frequencies
-- Output: [14, 9, 11] (ARRAY[BIGINT])
```


(double_items_sketch_estimate_lb)=
## `double_items_sketch_estimate_lb(sketch, item)`

Parameters:
* `sketch` (`VARBINARY`): A serialized Double Items sketch.
* `item` (`DOUBLE`, `REAL`, `ARRAY[DOUBLE]`, `ARRAY[REAL]`): The item or list of items to estimate the lower bound of the frequency for.

Returns:
* (`BIGINT` or `ARRAY[BIGINT]`): The estimated lower bound of the frequency or list of frequencies of `item` in `sketch`.

Examples:
```sql
-- item_price is a DOUBLE column
SELECT double_items_sketch_estimate_lb(double_items_sketch(item_price), 3.99) AS price1_frequency_lb
-- Output: 13 (BIGINT)
```

```sql
-- item_price is a DOUBLE column
SELECT double_items_sketch_estimate_lb(double_items_sketch(item_price), ARRAY[3.99, 7.49, 12.95]) AS price_frequencies_lb
-- Output: [13, 8, 9] (ARRAY[BIGINT])
```


(double_items_sketch_estimate_ub)=
## `double_items_sketch_estimate_ub(sketch, item)`

Parameters:
* `sketch` (`VARBINARY`): A serialized Double Items sketch.
* `item` (`DOUBLE`, `REAL`, `ARRAY[DOUBLE]`, `ARRAY[REAL]`): The item or list of items to estimate the upper bound of the frequency for.

Returns:
* (`BIGINT` or `ARRAY[BIGINT]`): The estimated upper bound of the frequency or list of frequencies of `item` in `sketch`.

Examples:
```sql
-- item_price is a DOUBLE column
SELECT double_items_sketch_estimate_ub(double_items_sketch(item_price), 3.99) AS price1_frequency_ub
-- Output: 16 (BIGINT)
```

```sql
-- item_price is a DOUBLE column
SELECT double_items_sketch_estimate_ub(double_items_sketch(item_price), ARRAY[3.99, 7.49, 12.95]) AS price_frequencies_ub
-- Output: [16, 11, 14] (ARRAY[BIGINT])
```


(double_items_sketch_frequent_items)=
## `double_items_sketch_frequent_items(sketch, false_positives)`

Parameters:
* `sketch` (`VARBINARY`): A serialized Double Items sketch.
* `false_positives` (`BOOLEAN`): Whether to include potential false positives in the output. If this is true, the output will include all items that may be frequent. If this is
  false, the output will only include items that are guaranteed to be frequent.

Returns:
* (`ARRAY[DOUBLE]`, `ARRAY[REAL]`): The frequent items in the sketch.

Examples:
```sql
-- item_price is a DOUBLE column
SELECT double_items_sketch_frequent_items(double_items_sketch(item_price), true) AS frequent_prices
-- Output: [3.99, 7.49, 12.95, 0.99]
```

```sql
-- item_price is a DOUBLE column
SELECT double_items_sketch_frequent_items(double_items_sketch(item_price), false) AS frequent_prices
-- Output: [3.99, 7.49]
```


