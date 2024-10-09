package org.apache.spark.sql.expressions

import com.roblox.spark.sketches.SketchType
import com.roblox.spark.sketches.theta.{ThetaSketchIntersection, ThetaSketchConfig}
import SketchType.THETA
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ImplicitCastInputTypes, Literal}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.{AbstractDataType, BinaryType, DataType}

abstract class CountDistinctSketchIntersection() extends BinaryExpression
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
      case THETA => ThetaSketchIntersection(input1.asInstanceOf[Array[Byte]], configValue).intersect(
        ThetaSketchIntersection(input2.asInstanceOf[Array[Byte]], configValue)
      ).toByteArray
      case _ => null
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val sketchClass = sketch match {
      case THETA => fullName(ThetaSketchIntersection.getClass)
    }

    // We don't use `nullSafeCodeGen` to support backward compatibility between Spark versions.
    // There were incompatible changes between Spark versions 2.3 and 2.4 that caused NoSuchMethodFound errors.
    // e.g ExprCode.value method returns String in Spark 2.3 and ExprValue return in 2.4
    defineCodeGen(ctx, ev, (left, right) => s"$sketchClass.apply($left).merge($sketchClass.apply($right));")
  }

  override def toString: String = s"count_distinct_sketch_intersection($left, $right)"

  override def prettyName: String = "count_distinct_sketch_intersection"

  private def fullName(clazz: Class[_]): String = {
    // class com.abc.Test -> com.abc.Text.MODULE$
    s"${clazz.toString.substring("class ".length)}.MODULE$$"
  }
}

case class CountDistinctThetaSketchIntersection(left: Expression, right: Expression, config: ThetaSketchConfig)
  extends CountDistinctSketchIntersection {
  override val sketch: SketchType.Value = SketchType.THETA
  override val configValue: Int = config.nominalEntities

  // use largest nominal entities as default value to avoid lose of precision
  def this(left: Expression, right: Expression) = this(left, right, ThetaSketchConfig(nominalEntities = 1048576))

  def this(left: Expression, right: Expression, configExpr: Expression) =
    this(left, right, ThetaSketchConfig(nominalEntities = configExpr.asInstanceOf[Literal].value.asInstanceOf[Int]))

  override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): CountDistinctSketchIntersection =
    copy(left = newLeft, right = newRight)
}