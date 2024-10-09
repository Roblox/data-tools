package org.apache.spark.sql

import com.roblox.spark.sketches.hll.HllSketchConfig
import com.roblox.spark.sketches.kll.KllSketchConfig
import com.roblox.spark.sketches.theta.ThetaSketchConfig
import org.apache.spark.sql.aggregates.hll.{HyperLogLogSketchBuildAggregate, HyperLogLogSketchMergeAggregate}
import org.apache.spark.sql.aggregates.kll.{KllSketchBuildAggregate, KllSketchMergeAggregate}
import org.apache.spark.sql.aggregates.theta.{ThetaSketchBuildAggregate, ThetaSketchMergeAggregate}
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.expressions.{
  CalculateKllSketchCompactQuantile,
  CalculateKllSketchCompactRank,
  CountDistinctHllSketchErrorBounds,
  CountDistinctHllSketchEstimate,
  CountDistinctHllSketchUnion,
  CountDistinctThetaSketchErrorBounds,
  CountDistinctThetaSketchEstimate,
  CountDistinctThetaSketchIntersection,
  CountDistinctThetaSketchUnion,
  KllSketchUnion
}

object functions_ex {

  /**
   * Aggregate function: returns the theta sketch as a byte array which represents
   * approximate number of distinct items in a group
   *
   * {{{
   *   ds.groupBy($"name").agg(functions_ex.theta_sketch_build($"id"))
   * }}}
   *
   * @group agg_funcs
   */
  def theta_sketch_build(column: Column): Column = withAggregateFunction {
    ThetaSketchBuildAggregate(column.expr)
  }

  def theta_sketch_build(column: Column, nominalEntitiesInput: Column): Column =
    theta_sketch_build(column, ThetaSketchConfig(nominalEntities = nominalEntitiesInput.expr.asInstanceOf[Literal].value.asInstanceOf[Int]))

  def theta_sketch_build(column: Column, config: ThetaSketchConfig): Column = withAggregateFunction {
    ThetaSketchBuildAggregate(column.expr, config)
  }

  /**
   * Aggregate function: merges theta sketches  as a byte array
   *
   * {{{
   *   ds.groupBy($"name").agg(theta_sketch_merge($"distinct_users_sketch"))
   * }}}
   *
   * @group agg_funcs
   */
  def theta_sketch_merge(column: Column, nominalEntitiesInput: Column): Column = withAggregateFunction{
    ThetaSketchMergeAggregate(
      column.expr,
      ThetaSketchConfig(nominalEntities = nominalEntitiesInput.expr.asInstanceOf[Literal].value.asInstanceOf[Int])
    )
  }

  def theta_sketch_merge(column: Column): Column = withAggregateFunction {
    ThetaSketchMergeAggregate(column.expr)
  }

  /**
   * Intersect two theta sketch intersections
   *
   * {{{
   *   ds.select(theta_sketch_intersection($"distinct_users_sketch", $"distinct_users_sketch2"))
   * }}}
   *
   * @group agg_funcs
   * @return The intersection of two sketches provided
   */

  def theta_sketch_intersection(left: Column, right: Column): Column = withExpr(
    {
      CountDistinctThetaSketchIntersection(left.expr, right.expr, ThetaSketchConfig())
    }
  )

  def theta_sketch_intersection(left: Column, right: Column, hllConfigColumn: Column): Column = withExpr(
    {
      CountDistinctThetaSketchIntersection(
        left.expr,
        right.expr,
        ThetaSketchConfig(nominalEntities = hllConfigColumn.expr.asInstanceOf[Literal].value.asInstanceOf[Int])
      )
    }
  )

  /**
   * Gets the unique count estimate.
   *
   * {{{
   *   ds.select(theta_sketch_get_estimate($"distinct_users_sketch"))
   * }}}
   *
   * @group binary_funcs
   * @return the sketch's best estimate of the cardinality of the input stream.
   */
  def theta_sketch_get_estimate(column: Column): Column = withExpr {
    CountDistinctThetaSketchEstimate(column.expr)
  }

  /**
   * Gets the error bounds of theta sketch estimate.
   *
   * {{{
   *   ds.select(theta_sketch_get_error_bounds($"distinct_users_sketch"))
   * }}}
   *
   * @group binary_funcs
   * @return the sketch's error bounds in a struct with lower_bound and upper_bound
   */
  def theta_sketch_get_error_bounds(column: Column): Column = withExpr {
    CountDistinctThetaSketchErrorBounds(column.expr, 2)
  }

  def theta_sketch_get_error_bounds(column: Column, stdDevColumn: Column): Column = withExpr {
    CountDistinctThetaSketchErrorBounds(
      column.expr,
      stdDevColumn.expr.asInstanceOf[Literal].value.asInstanceOf[Int]
    )
  }

  /**
   * Aggregate function: returns the hll sketch as a byte array which represents
   * approximate number of distinct items in a group
   *
   * {{{
   *   ds.groupBy($"name").agg(functions_ex.hll_sketch_build($"id"))
   * }}}
   *
   * @group agg_funcs
   */
  def hll_sketch_build(column: Column): Column = withAggregateFunction{
    HyperLogLogSketchBuildAggregate(column.expr)
  }

  def hll_sketch_build(column: Column, lgConfigKInput: Column): Column = {
    hll_sketch_build(column, HllSketchConfig(lgConfigK = lgConfigKInput.expr.asInstanceOf[Literal].value.asInstanceOf[Int]))
  }

  def hll_sketch_build(column: Column, config: HllSketchConfig): Column = withAggregateFunction {
    HyperLogLogSketchBuildAggregate(column.expr, config)
  }

  /**
   * Aggregate function: returns the hll sketch as a byte array which represents
   * approximate number of distinct items in a group
   *
   * {{{
   *   ds.groupBy($"name").agg(hll_sketch_merge($"distinct_users_sketch"))
   * }}}
   *
   * @group agg_funcs
   */
  def hll_sketch_merge(column: Column, lgConfigKInput: Column): Column = withAggregateFunction{
    HyperLogLogSketchMergeAggregate(column.expr, HllSketchConfig(lgConfigK = lgConfigKInput.expr.asInstanceOf[Literal].value.asInstanceOf[Int]))
  }

  def hll_sketch_merge(column: Column): Column = withAggregateFunction {
    HyperLogLogSketchMergeAggregate(column.expr)
  }

  /**
   * Gets the unique count estimate.
   *
   * {{{
   *   ds.select(hll_sketch_get_estimate($"distinct_users_sketch"))
   * }}}
   *
   * @group binary_funcs
   * @return the sketch's best estimate of the cardinality of the input stream.
   */
  def hll_sketch_get_estimate(column: Column): Column = withExpr {
    CountDistinctHllSketchEstimate(column.expr)
  }

  /**
   * Gets the error bounds of hll sketch estimate.
   *
   * {{{
   *   ds.select(hll_sketch_get_error_bounds($"distinct_users_sketch"))
   * }}}
   *
   * @group binary_funcs
   * @return the sketch's error bounds in a struct with lower_bound and upper_bound
   */
  def hll_sketch_get_error_bounds(column: Column): Column = withExpr {
    CountDistinctHllSketchErrorBounds(column.expr, 2)
  }

  def hll_sketch_get_error_bounds(column: Column, stdDevColumn: Column): Column = withExpr {
    CountDistinctHllSketchErrorBounds(
      column.expr,
      stdDevColumn.expr.asInstanceOf[Literal].value.asInstanceOf[Int]
    )
  }

  /**
   * Aggregate function: returns the kll sketch as a byte array which represents
   * approximate quantile information in a group
   *
   * {{{
   *   ds.groupBy($"universeId").agg(functions_ex.kll_sketch_build($"DAU"))
   * }}}
   *
   * @group agg_funcs
   */
  def kll_sketch_build(column: Column, kInput: Column): Column = {
    kll_sketch_build(column, KllSketchConfig(k = kInput.expr.asInstanceOf[Literal].value.asInstanceOf[Int]))
  }

  def kll_sketch_build(column: Column, config: KllSketchConfig): Column = withAggregateFunction {
    KllSketchBuildAggregate(column.expr, config)
  }

  /**
   * Aggregate function: merges kll sketches
   *
   * {{{
   *   ds.groupBy($"universeId").agg(kll_sketch_merge($"DAU"))
   * }}}
   *
   * @group agg_funcs
   */
  def kll_sketch_merge(column: Column): Column = withAggregateFunction {
    KllSketchMergeAggregate(column.expr, KllSketchConfig())
  }

  def kll_sketch_merge(column: Column, kInput: Column): Column = withAggregateFunction {
    KllSketchMergeAggregate(column.expr, KllSketchConfig(k = kInput.expr.asInstanceOf[Literal].value.asInstanceOf[Int]))
  }

  /**
   * Union two kll sketches.
   *
   * {{{
   *   ds.select(kll_sketch_union($"home_DAU", $"ingame_DAU"))
   * }}}
   *
   * @group binary_funcs
   * @return unions of two separate hll data sketches
   */
  def kll_sketch_union(left: Column, right: Column): Column = withExpr(
    {
      KllSketchUnion(left.expr, right.expr)
    }
  )

  /**
   * Gets the quantile estimate.
   *
   * {{{
   *   ds.select(kll_sketch_get_quantile($"quantile_sketch", lit(0.3)))
   * }}}
   *
   * @group binary_funcs
   * @return the sketch's best estimate of the quantile of the input stream.
   */
  def kll_sketch_get_quantile(column: Column, inputQuantile: Column): Column = withExpr( {
    CalculateKllSketchCompactQuantile(column.expr, inputQuantile.expr)
  })

  /**
   * Gets the quantile of a certain rank estimate.
   *
   * {{{
   *   ds.select(kll_sketch_get_rank($"quantile_sketch", lit(1629)))
   * }}}
   *
   * @group binary_funcs
   * @return the sketch's best estimate of the quantile of the input stream.
   */
  def kll_sketch_get_rank(column: Column, inputValue: Column): Column = withExpr({
    CalculateKllSketchCompactRank(column.expr, inputValue.expr)
  })

  def hll_sketch_union(left: Column, right: Column): Column = withExpr(
    {
      CountDistinctHllSketchUnion(left.expr, right.expr)
    }
  )

  def theta_sketch_union(left: Column, right: Column): Column = withExpr(
    {
      CountDistinctThetaSketchUnion(left.expr, right.expr, ThetaSketchConfig())
    }
  )

  def theta_sketch_union(left: Column, right: Column, hllConfigColumn: Column): Column = withExpr(
    {
      CountDistinctThetaSketchUnion(
        left.expr,
        right.expr,
        ThetaSketchConfig(nominalEntities = hllConfigColumn.expr.asInstanceOf[Literal].value.asInstanceOf[Int])
      )
    }
  )

  private def withAggregateFunction(func: AggregateFunction): Column = {
    Column(func.toAggregateExpression())
  }

  private def withExpr(expr: Expression): Column = Column(expr)

}
