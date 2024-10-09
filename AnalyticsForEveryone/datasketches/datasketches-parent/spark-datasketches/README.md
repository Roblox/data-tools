# Overview
The goal of this project is to seamlessly integrate probabilistic algorithms, in the form of the
[DataSketches](https://datasketches.apache.org/) into [Spark](https://spark.apache.org/). This can be an excellent 
solution for both real-time analytics and incremental historical data updates, as long as the results can tolerate 
slight inaccuracies (which have mathematically proven error bounds). This repository is based off the 
[Gelerion](https://github.com/Gelerion/spark-sketches) repository.
  
The project currently supports 3 types of sketches:
-   **HyperLogLog Sketch**: It is usually smaller in size due to the underlying implementation. However, this family does not support intersection operations.
-   **Theta Sketch**: It is usually a bit larger than the HLL Sketch, but it supports intersection and difference operations.
-   **KLL Sketch**

# Highlights

-   Supports Spark 3.1.0 and 3.2.0+
-   Provides a set of functions that are identical to the native ones, eliminating the need for `.toRdd` casts or error-prone methods such as `map`, `mapPartition`, etc.
-   Easy to use with both `SQL` and `DataFrame`/`Dataset` APIs
-   Flexible and accessible with sketches as BinaryType
-   Code generation
-   High-performance aggregations leveraging `TypedImperativeAggregate`.

# Get it!

## Maven

### Spark 3.2.0
```xml
<dependency> 
 <groupId>com.roblox.spark.sketches</groupId> 
 <artifactId>roblox-data-sketches-functions</artifactId> 
 <version>3.2.0</version> 
</dependency>
```

### Spark 3.1.0
```xml
<dependency> 
 <groupId>com.roblox.spark.sketches</groupId> 
 <artifactId>roblox-data-sketches-functions</artifactId> 
 <version>3.1.0</version> 
</dependency>
```
  
# Use It!

## In SQL
First, register the functions. This only needs to be done once.

```scala
import org.apache.spark.sql.registrar.SketchFunctionsRegistrar

SketchFunctionsRegistrar.registerFunctions(spark)
```

## Attaching the JAR


```
If attaching to pyspark you need to run

```spark._jvm.org.apache.spark.sql.registrar.SketchFunctionsRegistrar.registerFunctions(spark._jsparkSession)```

that registers all the functions listed in the data-sketch-functions repo. With this you should be able to write sql that has column definitions like
theta_sketch_build(userkey, 16384) as active_users_theta
or the same kll sketch functions.

#### Aggregating
The sketch will be created in the form of `raw bytes`. This allows us to save it into a file and recreate
the sketch after loading it. Alternatively, we can feed these files to our real-time analytic layer, such as Druid.
```sql
SELECT dimension_1, dimension_2, theta_sketch_build(metric)
      FROM table
      GROUP BY dimension_1, dimension_2
```
#### Merging sketches
One of the most fascinating features about the sketches is that they are mergeable.
The function takes other sketch as an input. Merging theta sketches in spark has to also include
a value for K if you don't want default accuracy. This is done with `theta_sketch_merge("col_name", 16384)`
```sql
SELECT dim_1, theta_sketch_merge(sketch)
      FROM table
      GROUP BY dimension_1
```
#### Union two sketches
Different data sketches can also be merged through union set operation.
The functions taking two sketches and returns a union sketch.
```sql
SELECT ..., theta_sketch_union(sketch1, sketch2)
      FROM table
```
#### Getting results
```sql
SELECT theta_sketch_get_estimate(sketch) FROM table
```
  
## In Scala
First, import the functions.
```scala
import org.apache.spark.sql.functions_ex._
```
#### Aggregating
```scala
df.groupBy($"dimension")
  .agg(theta_sketch_build($"metric", lit(4096)))
```
#### Merging sketches
```scala
df.groupBy($"dimension")
  .agg(theta_sketch_merge($"sketch"))
```
#### Union sketches
```scala
df.withColumn("sketch_union", theta_sketch_union($"sketch1", $"sketch2"))
```
#### Getting results
```scala
df.select(theta_sketch_get_estimate($"sketch"))
```

# Notes
## Theta Sketch
### theta_sketch_build
**Inputs**: column_name (Column), nominal_entities (Column) where it is log_base2 value, the minimum value is 2^4 and the maximum value is 2^26

**How to use**: When calling theta_sketch_build, make sure to wrap the nominal_entities value with lit().
```scala
theta_sketch_build(name, lit(16384))
```

### theta_sketch_merge
**Inputs**: column_name (Column)

```scala
theta_sketch_merge(name, lit(16384))
```
### theta_sketch_union
**Inputs**: column_name1 (Column), column_name2 (Column), nominal_entities (Column) where it is log_base2 value
from 2^4 to 2^26.

```scala
theta_sketch_union(name1, name2, lit(4096))
```

### theta_sketch_get_estimate
**Inputs**: column_name (Column/Sketch)

```scala
theta_sketch_get_estimate(theta_sketch_merge(id, lit(12)))
```

## HLL Sketch
### hll_sketch_build
**Inputs**: column_name (Column), lgConfigK (Column) where it is the Log2 of K for the target HLL sketch. This value must be between 4 and 21 inclusively.

**How to use**: When calling hll_sketch_build, make sure to wrap the lgConfigK value with lit().
```scala
hll_sketch_build(name, lit(12))
```

### hll_sketch_merge
**Inputs**: column_name (Column)

```scala
hll_sketch_merge(name, lit(12))
```

### hll_sketch_union
**Inputs**: column_name1 (Column), column_name2 (Column), lgConfigK (Column) where it is the Log2 of K for the target HLL sketch.
This value must be between 4 and 21 inclusively.

```scala
hll_sketch_union(name1, name2, lit(12))
```

### hll_sketch_get_estimate
**Inputs**: column_name (Column/Sketch)

```scala
hll_sketch_get_estimate(hll_sketch_merge(id, lit(12)))
```

## Kll Sketch
### kll_sketch_build
**Inputs**: column_name (Column), k (Column) where it is the k value of the sketch, controlling the size and accuracy of it.

**How to use**: When calling kll_sketch_build, make sure to wrap the k value with lit().
```scala
kll_sketch_build(name, lit(200))
```

### kll_sketch_merge
**Inputs**: column_name (Column)

```scala
kll_sketch_merge(name)
```

### kll_sketch_union
**Inputs**: column_name1 (Column), column_name2 (Column), k (Column) where it is the k value
of the sketch, controlling the size and accuracy of it.

```scala
kll_sketch_union(name1, name2, lit(12))
```

### kll_sketch_get_quantile
**Inputs**: column_name (Column/Sketch), quantile_value (Double)

**How to use**: When calling kll_sketch_get_quantile, make sure to wrap the quantile_value with lit().

```scala
kll_sketch_get_quantile(kll_sketch_merge(id), lit(0.3))
```

### kll_sketch_get_rank
**Inputs**: column_name (Column/Sketch), value (Double)

**How to use**: When calling kll_sketch_get_rank, make sure to wrap the value with lit(). Please note that the value you are inputting has to exist. Unlike get_quantile, get_rank finds the quantile of a specific value that exists.

```scala
kll_sketch_get_rank(kll_sketch_merge(id), lit(4321.0))
```

### Available functions
- Theta sketch
  - theta_sketch_build
  - theta_sketch_merge
  - theta_sketch_union
  - theta_sketch_get_estimate
- HLL sketch
  - hll_sketch_build
  - hll_sketch_merge
  - hll_sketch_union
  - hll_sketch_get_estimate
- KLL sketch
  - kll_sketch_build
  - kll_sketch_merge
  - kll_sketch_union
  - kll_sketch_get_quantile
  - kll_sketch_get_rank
