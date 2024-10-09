package org.apache.spark.sql.expressions

import com.roblox.spark.sketches.SketchType
import com.roblox.spark.sketches.hll.{HllSketchConfig, HyperLogLogSketch}
import com.roblox.spark.sketches.theta.{ThetaSketch, ThetaSketchConfig}
import SketchType.{HLL, THETA}
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ImplicitCastInputTypes, Literal}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.{AbstractDataType, BinaryType, DataType}

abstract class CountDistinctSketchUnion() extends BinaryExpression
  with ImplicitCastInputTypes
  with Serializable {

  val left: Expression
  val right: Expression
  val sketch: SketchType.Value
  val configValue: Int

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType, BinaryType)

  override def dataType: DataType = BinaryType

  override protected def nullSafeEval(input1: Any, input2: Any): Any = {
    sketch match {
      case THETA => ThetaSketch(input1.asInstanceOf[Array[Byte]], configValue).merge(
        ThetaSketch(input2.asInstanceOf[Array[Byte]], configValue)
      ).toByteArray
      case HLL => HyperLogLogSketch(input1.asInstanceOf[Array[Byte]]).merge(
        HyperLogLogSketch(input2.asInstanceOf[Array[Byte]])
      ).toByteArray
      case _ => null
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val sketchClass = sketch match {
      case THETA => fullName(ThetaSketch.getClass)
      case HLL => fullName(HyperLogLogSketch.getClass)
    }

    // We don't use `nullSafeCodeGen` to support backward compatibility between Spark versions.
    // There were incompatible changes between Spark versions 2.3 and 2.4 that caused NoSuchMethodFound errors.
    // e.g ExprCode.value method returns String in Spark 2.3 and ExprValue return in 2.4
    defineCodeGen(ctx, ev, (left, right) => s"$sketchClass.apply($left).merge($sketchClass.apply($right));")
  }

  override def toString: String = s"count_distinct_sketch_union($left, $right)"

  override def prettyName: String = "count_distinct_sketch_union"

  private def fullName(clazz: Class[_]): String = {
    // class com.abc.Test -> com.abc.Text.MODULE$
    s"${clazz.toString.substring("class ".length)}.MODULE$$"
  }
}

case class CountDistinctHllSketchUnion(left: Expression, right: Expression, config: HllSketchConfig = HllSketchConfig()) extends CountDistinctSketchUnion {
  override val sketch: SketchType.Value = SketchType.HLL
  // we don't need this value in Hll sketch. This is a placeholder
  override val configValue: Int = 0

  def this(left: Expression, right: Expression) = this(left, right, HllSketchConfig())

  def this(left: Expression, right: Expression, configExpr: Expression) =
    this(left, right, HllSketchConfig())

  override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): CountDistinctSketchUnion =
    copy(left = newLeft, right = newRight)
}

case class CountDistinctThetaSketchUnion(left: Expression, right: Expression, config: ThetaSketchConfig)
  extends CountDistinctSketchUnion {
  override val sketch: SketchType.Value = SketchType.THETA
  override val configValue: Int = config.nominalEntities

  // Use 2^20 (max value allowed) as default k-value if users don't explicitly set up
  def this(left: Expression, right: Expression) = this(left, right, ThetaSketchConfig(nominalEntities = 1048576))

  def this(left: Expression, right: Expression, configExpr: Expression) =
    this(left, right, ThetaSketchConfig(nominalEntities = configExpr.asInstanceOf[Literal].value.asInstanceOf[Int]))

  override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): CountDistinctSketchUnion =
    copy(left = newLeft, right = newRight)
}
