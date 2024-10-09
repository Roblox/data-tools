package org.apache.spark.sql.registrar

import org.apache.spark.sql.aggregates.hll.{HyperLogLogSketchBuildAggregate, HyperLogLogSketchMergeAggregate}
import org.apache.spark.sql.aggregates.kll.{KllSketchBuildAggregate, KllSketchMergeAggregate}
import org.apache.spark.sql.aggregates.theta.{ThetaSketchBuildAggregate, ThetaSketchMergeAggregate}
import org.apache.spark.sql.catalyst.expressions.ExpressionInfo
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
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder

object SketchFunctionsRegistrar extends FunctionsRegistrar {
  override protected def expressions: Map[String, (ExpressionInfo, FunctionBuilder)] = Map(
    // Theta
    expression[ThetaSketchBuildAggregate]("theta_sketch_build"),
    expression[ThetaSketchMergeAggregate]("theta_sketch_merge"),
    expression[CountDistinctThetaSketchUnion]("theta_sketch_union"),
    expression[CountDistinctThetaSketchEstimate]("theta_sketch_get_estimate"),
    expression[CountDistinctThetaSketchIntersection]("theta_sketch_intersection"),
    expression[CountDistinctThetaSketchErrorBounds]("theta_sketch_get_error_bounds"),


    // HLL
    expression[HyperLogLogSketchBuildAggregate]("hll_sketch_build"),
    expression[HyperLogLogSketchMergeAggregate]("hll_sketch_merge"),
    expression[CountDistinctHllSketchUnion]("hll_sketch_union"),
    expression[CountDistinctHllSketchEstimate]("hll_sketch_get_estimate"),
    expression[CountDistinctHllSketchErrorBounds]("hll_sketch_get_error_bounds"),

    // KLL
    expression[KllSketchBuildAggregate]("kll_sketch_build"),
    expression[KllSketchMergeAggregate]("kll_sketch_merge"),
    expression[KllSketchUnion]("kll_sketch_union"),
    expression[CalculateKllSketchCompactQuantile]("kll_sketch_get_quantile"),
    expression[CalculateKllSketchCompactRank]("kll_sketch_get_rank")
  )
}
