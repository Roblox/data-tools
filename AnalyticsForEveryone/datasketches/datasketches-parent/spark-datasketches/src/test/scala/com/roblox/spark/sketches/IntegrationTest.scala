package com.roblox.spark.sketches

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{concat, lit, rank}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.types.{BinaryType, StructField}

import java.nio.file.Files
import scala.collection.mutable
import scala.util.Random
import org.apache.spark.sql.functions_ex._
import org.apache.spark.sql.registrar.SketchFunctionsRegistrar

import scala.collection.convert.ImplicitConversions.`buffer AsJavaList`
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
class IntegrationTest extends SharedSparkSessionSuite {

  import spark.implicits._

  test("test theta-sketches with error bounds") {
    val data: Seq[User] = generateUsers()
    val userStatistics = UserStatistics()
    data.foreach(user => userStatistics.addUser(user))

    val users = spark.createDataset(data)

    assert(users.count() == userStatistics.totalRows)

    val distinctUserIdsNoNum = users
      .groupBy($"name")
      .agg(theta_sketch_build($"id").as("distinct_ids_sketch"))

    // ---- Build Sketch
    val distinctUserIdsTheta = users
      .groupBy($"name")
      .agg(theta_sketch_build($"id", lit(4096)).as("distinct_ids_sketch"))

    val schemaFields = distinctUserIdsTheta.schema.fields
    assert(schemaFields.length == 2)

    val thetaSketchField: StructField = distinctUserIdsTheta.schema.fields(1)
    assert(thetaSketchField.dataType == BinaryType)

    // ---- Evaluate Sketch
    val evaluatedValues = distinctUserIdsTheta
      .select(
        $"name",
        theta_sketch_get_estimate($"distinct_ids_sketch").as("count_distinct"),
        theta_sketch_get_error_bounds($"distinct_ids_sketch").as("count_distinct_error_bounds")
      )

    val approxDistinctUserIdsPairs = evaluatedValues.collect()
      .map(row => (
        row.getAs[String]("name"),
        row.getAs[Long]("count_distinct"),
        row.getAs[GenericRowWithSchema]("count_distinct_error_bounds")
        )
      )

    approxDistinctUserIdsPairs.foreach(userAgg => {
      println(userAgg)
      val userName = userAgg._1
      val approxDistinctIds = userAgg._2
      val distinctLowerBound = userAgg._3.getAs("lower_bound").asInstanceOf[Double]
      val distinctEstimate = userAgg._3.getAs("estimate").asInstanceOf[Long]
      val distinctUpperBound = userAgg._3.getAs("upper_bound").asInstanceOf[Double]
      print(userName, approxDistinctIds, distinctLowerBound, distinctUpperBound)
      assert(userStatistics.contains(userName))
      // sanity check that the estimate from error bounds equal to get estimate
      assert(approxDistinctIds==distinctEstimate)
      assert(isWithinBounds(
        boundaryPercentage = 10,
        exactCount = userStatistics.exactUniqueIds(userName)
      )(approxDistinctIds))
      assert(isWithinBounds(
        boundaryPercentage = 10,
        exactCount = (userStatistics.exactUniqueIds(userName)*0.95).toInt
      )(distinctLowerBound.toInt))
      assert(isWithinBounds(
        boundaryPercentage = 10,
        exactCount = (userStatistics.exactUniqueIds(userName) * 1.05).toInt
      )(distinctUpperBound.toInt))
    })
  }

  test("test theta-sketches with union") {
    val data: Seq[User] = generateUsers()
    val userStatistics = UserStatistics()
    data.foreach(user => userStatistics.addUser(user))

    val users = spark.createDataset(data)

    assert(users.count() == userStatistics.totalRows)

    val sketch1 = theta_sketch_build($"id", lit(4096)).as("distinct_ids_sketch")
    val sketch2 = theta_sketch_build($"id2", lit(4096)).as("distinct_ids_sketch2")

    // ---- Build Sketch
    // two sketches from different columns
    val distinctUserIdsTheta = users
      .withColumn("id2", concat($"id", lit("_1")))
      .groupBy($"name")
      .agg(sketch1, sketch2)


    val schemaFields = distinctUserIdsTheta.schema.fields
    assert(schemaFields.length == 3)

    val thetaSketchField: StructField = distinctUserIdsTheta.schema.fields(1)
    assert(thetaSketchField.dataType == BinaryType)

    val thetaSketchField2: StructField = distinctUserIdsTheta.schema.fields(2)
    assert(thetaSketchField2.dataType == BinaryType)

    // ---- Evaluate first Sketch
    val evaluatedValues = distinctUserIdsTheta
      .select($"name", theta_sketch_get_estimate($"distinct_ids_sketch").as("count_distinct"))

    val approxDistinctUserIdsPairs = evaluatedValues.collect()
      .map(row => (row.getAs[String]("name"), row.getAs[Long]("count_distinct")))

    approxDistinctUserIdsPairs.foreach(userAgg => {
      val userName = userAgg._1
      val approxDistinctIds = userAgg._2

      assert(userStatistics.contains(userName))
      assert(
        isWithinBounds(boundaryPercentage = 10, exactCount = userStatistics.exactUniqueIds(userName))(
          approxDistinctIds
        )
      )
    })

    // ---- Evaluate second Sketch
    val evaluatedValuesSecond = distinctUserIdsTheta
      .select($"name", theta_sketch_get_estimate($"distinct_ids_sketch2").as("count_distinct"))

    val approxDistinctUserIdsPairsSecond = evaluatedValuesSecond.collect()
      .map(row => (row.getAs[String]("name"), row.getAs[Long]("count_distinct")))

    approxDistinctUserIdsPairsSecond.foreach(userAgg => {
      val userName = userAgg._1
      val approxDistinctIds = userAgg._2

      assert(userStatistics.contains(userName))
      assert(
        isWithinBounds(
          boundaryPercentage = 10, exactCount = userStatistics.exactUniqueIds(userName)
        )(approxDistinctIds)
      )
    })

    // ---- Evaluate union of two data sketches
    val distinctUserIdsThetaUnion = distinctUserIdsTheta.withColumn(
      "combined_sketches", theta_sketch_union($"distinct_ids_sketch", $"distinct_ids_sketch2")
    )

    val thetaSketchFieldDup: StructField = distinctUserIdsThetaUnion.schema.fields(3)
    assert(thetaSketchFieldDup.dataType == BinaryType)

    val evaluatedValuesDup = distinctUserIdsThetaUnion
      .select($"name", theta_sketch_get_estimate($"combined_sketches").as("count_distinct_dup"))

    val approxDistinctUserIdsPairsDup = evaluatedValuesDup.collect()
      .map(row => (row.getAs[String]("name"), row.getAs[Long]("count_distinct_dup")))

    approxDistinctUserIdsPairsDup.foreach(userAgg => {
      val userName = userAgg._1
      val approxDistinctIds = userAgg._2
      assert(userStatistics.contains(userName))
      // should have two times of user count since we union two sketches
      assert(
        isWithinBounds(boundaryPercentage = 10, exactCount = userStatistics.exactUniqueIds(userName) * 2)(
          approxDistinctIds
        )
      )
    })

    // check union value is approximately sum of two sketch values
    (approxDistinctUserIdsPairs.sortBy(_._2) zip
      approxDistinctUserIdsPairsSecond.sortBy(_._2) zip
      approxDistinctUserIdsPairsDup.sortBy(_._2)).foreach {
      case ((first, second), union) =>
        assert(
          isWithinBounds(boundaryPercentage = 2, exactCount = first._2 + second._2)(union._2)
        )
    }

  }

  test("test theta-sketches intersection") {
    val data: Seq[User] = generateUsers(toGenerate = 100000, idsBound = 5000)
    val userStatistics = UserStatistics()
    data.foreach(user => userStatistics.addUser(user))

    val users = spark.createDataset(data)

    assert(users.count() == userStatistics.totalRows)

    // ---- Build Sketch
    val distinctUserIdsTheta = users
      .withColumn("id2", $"id"+lit(2500))
      .groupBy($"name")
      .agg(
        theta_sketch_build($"id", lit(8192)).as("distinct_ids_sketch"),
        theta_sketch_build($"id2", lit(8192)).as("distinct_ids_sketch2"),
      )

    val schemaFields = distinctUserIdsTheta.schema.fields
    assert(schemaFields.length == 3)

    val thetaSketchField: StructField = distinctUserIdsTheta.schema.fields(1)
    assert(thetaSketchField.dataType == BinaryType)

    val thetaSketchField2: StructField = distinctUserIdsTheta.schema.fields(2)
    assert(thetaSketchField2.dataType == BinaryType)

    // ---- Evaluate First Sketch
    val evaluatedValues = distinctUserIdsTheta
      .select($"name", theta_sketch_get_estimate($"distinct_ids_sketch").as("count_distinct"))

    val approxDistinctUserIdsPairs = evaluatedValues.collect()
      .map(row => (row.getAs[String]("name"), row.getAs[Long]("count_distinct")))

    approxDistinctUserIdsPairs.foreach(userAgg => {
      val userName = userAgg._1
      val approxDistinctIds = userAgg._2

      assert(userStatistics.contains(userName))
      assert(isWithinBounds(boundaryPercentage = 10, exactCount = userStatistics.exactUniqueIds(userName))(approxDistinctIds))
    })

    val evaluatedValues2 = distinctUserIdsTheta
      .select($"name", theta_sketch_get_estimate($"distinct_ids_sketch2").as("count_distinct2"))

    val approxDistinctUserIdsPairs2 = evaluatedValues2.collect()
      .map(row => (row.getAs[String]("name"), row.getAs[Long]("count_distinct2")))

    approxDistinctUserIdsPairs2.foreach(userAgg => {
      val userName = userAgg._1
      val approxDistinctIds = userAgg._2

      assert(userStatistics.contains(userName))
      assert(isWithinBounds(boundaryPercentage = 10, exactCount = userStatistics.exactUniqueIds(userName))(approxDistinctIds))
    })

    val intersectedValues = distinctUserIdsTheta
      .select($"name",
        theta_sketch_intersection($"distinct_ids_sketch", $"distinct_ids_sketch2", lit(8192)
        ).as("count_distinct_intersection_sketch")
      )

    val intersectedValues2 =  intersectedValues
      .select($"name",
        theta_sketch_get_estimate($"count_distinct_intersection_sketch").as("count_distinct_intersection"),
        theta_sketch_get_error_bounds($"count_distinct_intersection_sketch").as("intersection_error_bounds")
      )

    val approxDistinctUserIdsIntersectionPairs = intersectedValues2.collect()
      .map(row => (
        row.getAs[String]("name"),
        row.getAs[Long]("count_distinct_intersection"),
        row.getAs[GenericRowWithSchema]("intersection_error_bounds")
        )
      )

    approxDistinctUserIdsIntersectionPairs.foreach(userAgg => {
      val userName = userAgg._1
      val approxDistinctIds = userAgg._2
      val distinctLowerBound = userAgg._3.getAs("lower_bound").asInstanceOf[Double]
      val distinctUpperBound = userAgg._3.getAs("upper_bound").asInstanceOf[Double]
      println(userName, approxDistinctIds, distinctLowerBound, distinctUpperBound, userStatistics.exactUniqueIds(userName) / 2)
      assert(userStatistics.contains(userName))
      assert(isWithinBounds(
        boundaryPercentage = 10,
        exactCount = userStatistics.exactUniqueIds(userName)/2
      )(approxDistinctIds))
//      // Theta sketch after intersection does not give meaningful error bounds
//      assert(isWithinBounds(
//        boundaryPercentage = 10,
//        exactCount = (userStatistics.exactUniqueIds(userName) / 2 * 0.9).toInt
//      )(distinctLowerBound.toInt))
//      assert(isWithinBounds(
//        boundaryPercentage = 10,
//        exactCount = (userStatistics.exactUniqueIds(userName) / 2 * 0.9).toInt
//      )(distinctUpperBound.toInt))
    })
  }

  test("test hll-sketches with error bounds") {
    val data: Seq[User] = generateUsers()
    val userStatistics = UserStatistics()
    data.foreach(user => userStatistics.addUser(user))

    val users = spark.createDataset(data)

    assert(users.count() == userStatistics.totalRows)

    // ---- Build Sketch
    val distinctUserIdsHll = users
      .groupBy($"name")
      .agg(hll_sketch_build($"id", lit(12)).as("distinct_ids_sketch"))

    val schemaFields = distinctUserIdsHll.schema.fields
    assert(schemaFields.length == 2)

    val hllSketchField: StructField = distinctUserIdsHll.schema.fields(1)
    assert(hllSketchField.dataType == BinaryType)

    // ---- Evaluate Sketch
    val evaluatedValues = distinctUserIdsHll
      .select(
        $"name",
        hll_sketch_get_estimate($"distinct_ids_sketch").as("count_distinct"),
        hll_sketch_get_error_bounds($"distinct_ids_sketch").as("count_distinct_error_bounds")
      )

    val approxDistinctUserIdsPairs = evaluatedValues.collect()
      .map(row => (
          row.getAs[String]("name"),
          row.getAs[Long]("count_distinct"),
          row.getAs[GenericRowWithSchema]("count_distinct_error_bounds")
        )
      )

    approxDistinctUserIdsPairs.foreach(userAgg => {
      val userName = userAgg._1
      val approxDistinctIds = userAgg._2
      val distinctLowerBound = userAgg._3.getAs("lower_bound").asInstanceOf[Double]
      val distinctEstimate = userAgg._3.getAs("estimate").asInstanceOf[Long]
      val distinctUpperBound = userAgg._3.getAs("upper_bound").asInstanceOf[Double]
      print(userName, approxDistinctIds, distinctLowerBound, distinctUpperBound)
      assert(userStatistics.contains(userName))
      assert(approxDistinctIds==distinctEstimate)
      assert(isWithinBounds(
        boundaryPercentage = 10,
        exactCount = userStatistics.exactUniqueIds(userName)
      )(approxDistinctIds))
      assert(isWithinBounds(
        boundaryPercentage = 10,
        exactCount = (userStatistics.exactUniqueIds(userName) * 0.95).toInt
      )(distinctLowerBound.toInt))
      assert(isWithinBounds(
        boundaryPercentage = 10,
        exactCount = (userStatistics.exactUniqueIds(userName) * 1.05).toInt
      )(distinctUpperBound.toInt))

      assert(userStatistics.contains(userName))
      assert(isWithinBounds(boundaryPercentage = 10, exactCount = userStatistics.exactUniqueIds(userName))(approxDistinctIds))
    })
  }

  test("test hll-sketches with union") {
    val data: Seq[User] = generateUsers()
    val userStatistics = UserStatistics()
    data.foreach(user => userStatistics.addUser(user))

    val users = spark.createDataset(data)

    assert(users.count() == userStatistics.totalRows)

    val sketch1 = hll_sketch_build($"id", lit(14)).as("distinct_ids_sketch")
    val sketch2 = hll_sketch_build($"id2", lit(14)).as("distinct_ids_sketch2")

    // ---- Build Sketch
    val distinctUserIdsHll = users
      .withColumn("id2", $"id"+300000)
      .groupBy($"name")
      .agg(sketch1, sketch2)

    val schemaFields = distinctUserIdsHll.schema.fields
    assert(schemaFields.length == 3)

    val hllSketchField: StructField = distinctUserIdsHll.schema.fields(1)
    assert(hllSketchField.dataType == BinaryType)

    val hllSketchFieldSecond: StructField = distinctUserIdsHll.schema.fields(2)
    assert(hllSketchFieldSecond.dataType == BinaryType)

    // ---- Evaluate First Sketch
    val evaluatedValues = distinctUserIdsHll
      .select($"name", hll_sketch_get_estimate($"distinct_ids_sketch").as("count_distinct"))

    val approxDistinctUserIdsPairs = evaluatedValues.collect()
      .map(row => (row.getAs[String]("name"), row.getAs[Long]("count_distinct")))

    approxDistinctUserIdsPairs.foreach(userAgg => {
      val userName = userAgg._1
      val approxDistinctIds = userAgg._2

      assert(userStatistics.contains(userName))
      assert(isWithinBounds(boundaryPercentage = 10, exactCount = userStatistics.exactUniqueIds(userName))(approxDistinctIds))
    })

    // ---- Evaluate Second Sketch
    val evaluatedValuesSecond = distinctUserIdsHll
      .select($"name", hll_sketch_get_estimate($"distinct_ids_sketch2").as("count_distinct"))

    val approxDistinctUserIdsPairsSecond = evaluatedValuesSecond.collect()
      .map(row => (row.getAs[String]("name"), row.getAs[Long]("count_distinct")))

    approxDistinctUserIdsPairsSecond.foreach(userAgg => {
      val userName = userAgg._1
      val approxDistinctIds = userAgg._2

      assert(userStatistics.contains(userName))
      assert(isWithinBounds(boundaryPercentage = 10, exactCount = userStatistics.exactUniqueIds(userName))(approxDistinctIds))
    })

    // ---- Evaluate union of two sketches
    val distinctUserIdsHllUnion = distinctUserIdsHll.withColumn(
      "combined_sketches", hll_sketch_union($"distinct_ids_sketch", $"distinct_ids_sketch2")
    )

    val thetaSketchFieldDup: StructField = distinctUserIdsHllUnion.schema.fields(3)
    assert(thetaSketchFieldDup.dataType == BinaryType)

    val evaluatedValuesDup = distinctUserIdsHllUnion
      .select($"name", hll_sketch_get_estimate($"combined_sketches").as("count_distinct_dup"))

    val approxDistinctUserIdsPairsDup = evaluatedValuesDup.collect()
      .map(row => (row.getAs[String]("name"), row.getAs[Long]("count_distinct_dup")))

    // check union value is approximately two times of original exact distinct value
    approxDistinctUserIdsPairsDup.foreach(userAgg => {
      val userName = userAgg._1
      val approxDistinctIds = userAgg._2
      assert(userStatistics.contains(userName))
      // should have two times of user count since we union two sketches
      assert(
        isWithinBounds(boundaryPercentage = 10, exactCount = userStatistics.exactUniqueIds(userName) * 2)(
          approxDistinctIds
        )
      )
    })

    // check union value is approximately sum of two sketch values
    (approxDistinctUserIdsPairs.sortBy(_._2) zip
      approxDistinctUserIdsPairsSecond.sortBy(_._2) zip
      approxDistinctUserIdsPairsDup.sortBy(_._2)).foreach {
      case ((first, second), union) =>
        assert(
          isWithinBounds(boundaryPercentage = 2, exactCount = first._2+second._2)(union._2)
        )
    }
  }

  test("test kll-sketches") {
    val data: Seq[Universe] = generateUniverses()
    val universeStatistics = UniverseStatistics()
    data.foreach(universe => {
      universeStatistics.addUniverse(universe)
    })

    val universes = spark.createDataset(data)

    assert(universes.count() == universeStatistics.totalRows)

    // ---- Build Sketch
    val distinctUserIdsTheta = universes
      .groupBy($"name")
      .agg(kll_sketch_build($"userCount", lit(200)).as("user_count_sketch"))

    // ---- Make sure that built sketch is built right
    val schemaFields = distinctUserIdsTheta.schema.fields
    assert(schemaFields.length == 2)

    val thetaSketchField: StructField = distinctUserIdsTheta.schema.fields(1)
    assert(thetaSketchField.dataType == BinaryType)

    // ---- Evaluate Sketch
    val evaluatedQuantile = distinctUserIdsTheta
      .select($"name", kll_sketch_get_quantile($"user_count_sketch", lit(0.5)).as("quantile"))

    val evaluatedRank = distinctUserIdsTheta
      .select($"name", kll_sketch_get_rank($"user_count_sketch", lit(4321.0)).as("rank"))

    val approxQuantileUserIdsPairs = evaluatedQuantile.collect()
      .map(row => (row.getAs[String]("name"), row.getAs[Double]("quantile")))

    val approxRankUserIdsPairs = evaluatedRank.collect()
      .map(row => (row.getAs[String]("name"), row.getAs[Double]("rank")))

    approxQuantileUserIdsPairs.foreach(userAgg => {
      val userName = userAgg._1
      val quantile = userAgg._2

      assert(universeStatistics.contains(userName))
      assert(isWithinKllBounds(5, universeStatistics.getExactQuantile(userName, 0.5))(quantile))
    })

    approxRankUserIdsPairs.foreach(userAgg => {
      val userName = userAgg._1
      val rank = userAgg._2

      assert(universeStatistics.contains(userName))
      assert(isWithinKllBounds(5, universeStatistics.getExactRank(userName, 4321.0))(rank))
    })
  }

  test("test kll-sketches union") {
    val data: Seq[Universe] = generateUniverses()
    val universeStatistics = UniverseStatistics()
    data.foreach(universe => {
      universeStatistics.addUniverse(universe)
    })

    val universes = spark.createDataset(data)

    assert(universes.count() == universeStatistics.totalRows)

    // ---- Build Sketch
    val distinctUserIdsKll = universes
      .groupBy($"name")
      .agg(
        kll_sketch_build($"userCount", lit(200)).as("user_count_sketch"),
        kll_sketch_build($"userCount" + lit(1000000), lit(200)).as("user_count_sketch_upper")
      )
      .withColumn(
        "user_count_sketch_union",
        kll_sketch_union($"user_count_sketch", $"user_count_sketch_upper")
      )

    // ---- Make sure that built sketch is built right
    val schemaFields = distinctUserIdsKll.schema.fields
    assert(schemaFields.length == 4)

    val thetaSketchUnionField: StructField = distinctUserIdsKll.schema.fields(3)
    assert(thetaSketchUnionField.dataType == BinaryType)

    // ---- Evaluate Sketch
    // use p25 since we have double the number of records, with second copy be all higher than original values
    val evaluatedUnionQuantile = distinctUserIdsKll
      .select($"name", kll_sketch_get_quantile($"user_count_sketch_union", lit(0.25)).as("quantile"))

    val evaluatedUnionRank = distinctUserIdsKll
      .select($"name", kll_sketch_get_rank($"user_count_sketch_union", lit(4321.0)).as("rank"))

    val approxQuantileUserIdsPairs = evaluatedUnionQuantile.collect()
      .map(row => (row.getAs[String]("name"), row.getAs[Double]("quantile")))

    val approxRankUserIdsPairs = evaluatedUnionRank.collect()
      .map(row => (row.getAs[String]("name"), row.getAs[Double]("rank")))

    approxQuantileUserIdsPairs.foreach(userAgg => {
      val userName = userAgg._1
      val quantile = userAgg._2

      assert(universeStatistics.contains(userName))
      assert(isWithinKllBounds(5, universeStatistics.getExactQuantile(userName, 0.5))(quantile))
    })

    approxRankUserIdsPairs.foreach(userAgg => {
      val userName = userAgg._1
      val rank = userAgg._2
      assert(universeStatistics.contains(userName))
      // need to divide the rank by 2 since we have doubled the original data size
      assert(isWithinKllBounds(5, universeStatistics.getExactRank(userName, 4321.0) / 2)(rank))
    })
  }

  test("merge theta-sketches") {
    val data: Seq[User] = generateUsers()
    val userStatistics = UserStatistics()
    data.foreach(user => userStatistics.addUser(user))

    val approxDistinctUserIdsPairs = spark.createDataset(data)
      .groupBy($"name", $"age")
      .agg(theta_sketch_build($"id", lit(4096)).as("distinct_ids_sketch"))
      // drop dimension, merge sketches
      .groupBy($"name")
      .agg(theta_sketch_merge($"distinct_ids_sketch").as("distinct_ids_sketch"))
      // evaluate results
      .select($"name", theta_sketch_get_estimate($"distinct_ids_sketch").as("count_distinct"))
      .collect()
      //to scala Pair
      .map(row => (row.getAs[String]("name"), row.getAs[Long]("count_distinct")))

    approxDistinctUserIdsPairs.foreach(userAgg => {
      val userName = userAgg._1
      val approxDistinctIds = userAgg._2

      assert(userStatistics.contains(userName))
      assert(isWithinBounds(boundaryPercentage = 10, exactCount = userStatistics.exactUniqueIds(userName))(approxDistinctIds))
    })
  }

  test("merge hll-sketches") {
    val data: Seq[User] = generateUsers()
    val userStatistics = UserStatistics()
    data.foreach(user => userStatistics.addUser(user))

    val approxDistinctUserIdsPairs = spark.createDataset(data)
      .groupBy($"name", $"age")
      .agg(hll_sketch_build($"id", lit(12)).as("distinct_ids_sketch"))
      // drop dimension, merge sketches
      .groupBy($"name")
      .agg(hll_sketch_merge($"distinct_ids_sketch").as("distinct_ids_sketch"))
      // evaluate results
      .select($"name", hll_sketch_get_estimate($"distinct_ids_sketch").as("count_distinct"))
      .collect()
      //to scala Pair
      .map(row => (row.getAs[String]("name"), row.getAs[Long]("count_distinct")))

    approxDistinctUserIdsPairs.foreach(userAgg => {
      val userName = userAgg._1
      val approxDistinctIds = userAgg._2

      assert(userStatistics.contains(userName))
      assert(isWithinBounds(boundaryPercentage = 10, exactCount = userStatistics.exactUniqueIds(userName))(approxDistinctIds))
    })
  }

  test("merge kll-sketches") {
    val data: Seq[Universe] = generateUniverses()
    val universeStatistics = UniverseStatistics()
    data.foreach(universe => universeStatistics.addUniverse(universe))

    // Quantile
    val approxQuantilePairs = spark.createDataset(data)
      .groupBy($"name", $"id")
      .agg(kll_sketch_build($"userCount", lit(200)).as("user_count_sketch"))
      // drop dimension, merge sketches
      .groupBy($"name")
      .agg(kll_sketch_merge($"user_count_sketch").as("user_count_sketch"))
      // evaluate results
      .select($"name", kll_sketch_get_quantile($"user_count_sketch", lit(0.5)).as("quantile"))
      .collect()
      //to scala Pair
      .map(row => (row.getAs[String]("name"), row.getAs[Double]("quantile")))

    // Rank
    val approxRankPair = spark.createDataset(data)
      .groupBy($"name", $"id")
      .agg(kll_sketch_build($"userCount", lit(200)).as("user_count_sketch"))
      // drop dimension, merge sketches
      .groupBy($"name")
      .agg(kll_sketch_merge($"user_count_sketch").as("user_count_sketch"))
      // evaluate results
      .select($"name", kll_sketch_get_rank($"user_count_sketch", lit(4312.0)).as("rank"))
      .collect()
      //to scala Pair
      .map(row => (row.getAs[String]("name"), row.getAs[Double]("rank")))

    approxQuantilePairs.foreach(userAgg => {
      val userName = userAgg._1
      val quantile = userAgg._2

      assert(universeStatistics.contains(userName))
      assert(isWithinKllBounds(boundaryPercentage = 10, exactCount = universeStatistics.getExactQuantile(userName, 0.5))(quantile))
    })

    approxRankPair.foreach(userAgg => {
      val userName = userAgg._1
      val rank = userAgg._2

      assert(universeStatistics.contains(userName))
      assert(isWithinKllBounds(boundaryPercentage = 10, exactCount = universeStatistics.getExactRank(userName, 4321.0))(rank))
    })
  }

  test("usable in SQL, theta sketch") {
    //Register functions
    SketchFunctionsRegistrar.registerFunctions(spark)
    val data: Seq[User] = generateUsers()

    val users = spark.createDataset(data)
    users.createTempView("users_theta")

    validateThetaAndHll(spark.sql(
      """
        | SELECT tmp.name, theta_sketch_get_estimate(theta_sketch_merge(tmp.distinct_ids_sketch)) as approx_count_distinct
        | FROM (SELECT name, age, theta_sketch_build(id) as distinct_ids_sketch
        |       FROM users_theta
        |       GROUP BY name, age) as tmp
        | GROUP BY tmp.name
        |""".stripMargin),
      initial = data)
  }

  test("usable in SQL, hll sketch") {
    //Register functions
    SketchFunctionsRegistrar.registerFunctions(spark)
    val data: Seq[User] = generateUsers()

    val users = spark.createDataset(data)
    users.createTempView("users_hll")

    validateThetaAndHll(spark.sql(
      """
        | SELECT tmp.name, hll_sketch_get_estimate(hll_sketch_merge(tmp.distinct_ids_sketch)) as approx_count_distinct
        | FROM (SELECT name, age, hll_sketch_build(id, 12) as distinct_ids_sketch
        |       FROM users_hll
        |       GROUP BY name, age) as tmp
        | GROUP BY tmp.name
        |""".stripMargin),
      initial = data)
  }

  test("usable in SQL, kll sketch") {
    //Register functions
    SketchFunctionsRegistrar.registerFunctions(spark)
    val data: Seq[Universe] = generateUniverses()

    val users = spark.createDataset(data)
    users.createTempView("universes_kll")
    spark.sql(
      """
       SELECT tmp.name, kll_sketch_get_quantile(kll_sketch_merge(tmp.user_count_sketch), 0.3) as quantile
       FROM (SELECT name, id, kll_sketch_build(userCount, 200) as user_count_sketch
          FROM universes_kll
            GROUP BY name, id) as tmp
       GROUP BY tmp.name
       """)
    spark.sql(
      """
       SELECT tmp.name, kll_sketch_get_rank(kll_sketch_merge(tmp.user_count_sketch), 4321.0) as rank
       FROM (SELECT name, id, kll_sketch_build(userCount, 200) as user_count_sketch
          FROM universes_kll
            GROUP BY name, id) as tmp
       GROUP BY tmp.name
       """)
  }

  test("writing and reading hll sketches as parquet files") {
    val data: Seq[User] = generateUsers()
    val output = Files.createTempDirectory("spark-tmp").toFile
    output.deleteOnExit()

    spark.createDataset(data)
      .groupBy($"name")
      .agg(hll_sketch_build($"id", lit(12)).as("distinct_sketch"))
      .write.mode(SaveMode.Overwrite).parquet(output.getAbsolutePath)

    val reread = spark.read.parquet(output.getAbsolutePath)
    validateThetaAndHll(
      reread.select($"name", hll_sketch_get_estimate($"distinct_sketch").as("approx_count_distinct")),
      data)
  }

  test("writing and reading theta sketches as parquet files") {
    val data: Seq[User] = generateUsers()
    val output = Files.createTempDirectory("spark-tmp").toFile
    output.deleteOnExit()

    spark.createDataset(data)
      .groupBy($"name")
      .agg(theta_sketch_build($"id", lit(4096)).as("distinct_sketch"))
      .write.mode(SaveMode.Overwrite).parquet(output.getAbsolutePath)

    val reread = spark.read.parquet(output.getAbsolutePath)
    validateThetaAndHll(
      reread.select($"name", theta_sketch_get_estimate($"distinct_sketch").as("approx_count_distinct")),
      data)
  }

  test("writing and reading kll sketches as parquet files for quantile") {
    val data: Seq[Universe] = generateUniverses()
    val output = Files.createTempDirectory("spark-tmp").toFile
    output.deleteOnExit()

    spark.createDataset(data)
      .groupBy($"name")
      .agg(kll_sketch_build($"userCount", lit(200)).as("user_count_sketch"))
      .write.mode(SaveMode.Overwrite).parquet(output.getAbsolutePath)

    val reread = spark.read.parquet(output.getAbsolutePath)
    validateKll(
      reread.select($"name", kll_sketch_get_quantile($"user_count_sketch", lit(0.5)).as("quantile")),
      data, "quantile")
  }

  test("writing and reading kll sketches as parquet files for rank") {
    val data: Seq[Universe] = generateUniverses()
    val output = Files.createTempDirectory("spark-tmp").toFile
    output.deleteOnExit()

    spark.createDataset(data)
      .groupBy($"name")
      .agg(kll_sketch_build($"userCount", lit(200)).as("user_count_sketch"))
      .write.mode(SaveMode.Overwrite).parquet(output.getAbsolutePath)

    val reread = spark.read.parquet(output.getAbsolutePath)
    validateKll(
      reread.select($"name", kll_sketch_get_rank($"user_count_sketch", lit(4321.0)).as("rank")),
      data, "rank")
  }

  def isWithinBounds(boundaryPercentage: Int, exactCount: Long)(approximateCount: Long): Boolean = {
    val minBoundary = exactCount - (exactCount * (boundaryPercentage.toDouble / 100))
    val maxBoundary = exactCount + (exactCount * (boundaryPercentage.toDouble / 100))
    approximateCount >= minBoundary && approximateCount <= maxBoundary
  }

  def isWithinKllBounds(boundaryPercentage: Int, exactCount: Double)(approximateCount: Double): Boolean = {
    val minBoundary = exactCount - (exactCount * (boundaryPercentage.toDouble / 100))
    val maxBoundary = exactCount + (exactCount * (boundaryPercentage.toDouble / 100))
    approximateCount >= minBoundary && approximateCount <= maxBoundary
  }

  def validateThetaAndHll(calculated: DataFrame, initial: Seq[User]): Unit = {
    val userStatistics = UserStatistics()
    initial.foreach(user => userStatistics.addUser(user))

    calculated.cache()
    assert(calculated.count() > 1)

    val approxDistinctUserIdsPairs = calculated.collect()
      //to scala Pair
      .map(row => (row.getAs[String]("name"), row.getAs[Long]("approx_count_distinct")))

    approxDistinctUserIdsPairs.foreach(userAgg => {
      val userName = userAgg._1
      val approxDistinctIds = userAgg._2

      assert(userStatistics.contains(userName))
      assert(isWithinBounds(boundaryPercentage = 10, exactCount = userStatistics.exactUniqueIds(userName))(approxDistinctIds))
    })
  }

  def validateKll(calculated: DataFrame, initial: Seq[Universe], mode: String): Unit = {
    val universeStatistics = UniverseStatistics()
    initial.foreach(universe => universeStatistics.addUniverse(universe))

    calculated.cache()
    assert(calculated.count() > 1)

    mode match {
      case "quantile" => {
        val approxQuantilePairs = calculated.collect()
          //to scala Pair
          .map(row => (row.getAs[String]("name"), row.getAs[Double]("quantile")))

        approxQuantilePairs.foreach(userAgg => {
          val userName = userAgg._1
          val quantile = userAgg._2

          assert(universeStatistics.contains(userName))
          assert(isWithinKllBounds(boundaryPercentage = 10, exactCount = universeStatistics.getExactQuantile(userName, 0.5))(quantile))
        })
      }
      case "rank" => {
        val approxRankPairs = calculated.collect()
          //to scala Pair
          .map(row => (row.getAs[String]("name"), row.getAs[Double]("rank")))

        approxRankPairs.foreach(userAgg => {
          val userName = userAgg._1
          val rank = userAgg._2

          assert(universeStatistics.contains(userName))
          assert(isWithinKllBounds(boundaryPercentage = 10, exactCount = universeStatistics.getExactRank(userName, 4321.0))(rank))
        })
      }
    }
  }

  def generateUsers(toGenerate: Int = 50000, idsBound: Int = 30000, ageBound: Int = 10): Seq[User] = {
    val names = Array("1", "2", "3", "4", "5", "6")
    val size = names.length

    for (i <- 0 to toGenerate) yield {
      User(names(i % size), Random.nextInt(ageBound), Random.nextInt(idsBound))
    }
  }

  def generateUniverses(toGenerate: Int = 4000, usersBound: Int = 5000, idsBound: Int = 30000): Seq[Universe] = {
    val names = Array("1", "2", "3", "4", "5", "6")
    val size = names.length

    for (i <- 0 to (toGenerate + size)) yield {
      if (i > toGenerate) {
        Universe(names(i % size), Random.nextInt(idsBound), 4321)
      }
      else {
        Universe(names(i % size), Random.nextInt(idsBound), Random.nextInt(usersBound))
      }
    }
  }

  case class UserStatistics() {
    private val nameToUniqueIds: mutable.Map[String, mutable.Set[Long]] = mutable.Map.empty
    private val nameAgeToUniqueIds: mutable.Map[(String, Int), mutable.Set[Long]] = mutable.Map.empty
    var totalRows: Int = 0

    def contains(name: String): Boolean = {
      nameToUniqueIds.contains(name)
    }

    def addUser(user: User): this.type = {
      nameToUniqueIds.getOrElseUpdate(user.name, new mutable.HashSet[Long]()).add(user.id)
      nameAgeToUniqueIds.getOrElseUpdate((user.name, user.age), new mutable.HashSet[Long]()).add(user.id)
      totalRows += 1
      this
    }

    def exactUniqueIds(name: String): Long = {
      nameToUniqueIds.get(name).map(values => values.size.toLong).getOrElse(Long.MinValue)
    }

    def exactUniqueIdsAgeIntersection(name: String): Long = {
      nameAgeToUniqueIds.toSeq.collect {
        case ((n, _), idsSet) if n == name => idsSet
      }.reduce((a1, a2) => a1 intersect a2).size
    }
  }

  case class UniverseStatistics() {
    val nameToUserCount: mutable.Map[String, ArrayBuffer[Double]] = mutable.Map.empty
    var totalRows: Int = 0

    def contains(name: String): Boolean = {
      nameToUserCount.contains(name)
    }

    def addUniverse(universe: Universe): this.type = {
      nameToUserCount.getOrElseUpdate(universe.name, new ArrayBuffer[Double]).add(universe.userCount)
      totalRows += 1
      this
    }

    def getExactQuantile(name: String, quantileValue: Double): Double = {
      val userCount = spark.createDataset(nameToUserCount(name)).as("userCount")

      userCount.stat.approxQuantile("userCount.value", Array(quantileValue), 0.0001)(0)
    }

    def getExactRank(name: String, rankValue: Double): Double = {
      val w = Window.orderBy($"userCount.value")
      val userCount = spark.createDataset(nameToUserCount(name)).as("userCount")

      val whereClause: String = "userCount.value = " + rankValue
      userCount.select($"userCount.value", rank.over(w).alias("rank")).where(whereClause).head().getInt(1).toDouble / nameToUserCount(name).size
    }
  }
}


case class User(name: String, age: Int, id: Int)

case class Universe(name: String, id: Int, userCount: Int)
