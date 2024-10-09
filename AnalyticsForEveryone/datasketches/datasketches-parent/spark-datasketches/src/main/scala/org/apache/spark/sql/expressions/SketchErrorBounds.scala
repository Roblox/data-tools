package org.apache.spark.sql.expressions

import com.roblox.spark.sketches.hll.HyperLogLogSketch
import com.roblox.spark.sketches.SketchType
import com.roblox.spark.sketches.theta.ThetaSketch
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes, Literal, UnaryExpression}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.{AbstractDataType, BinaryType, DataType, StructType}
import SketchType.{HLL, THETA}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

case class SketchErrorBounds(lower_bound: Double, estimate: Long, upper_bound: Double)

abstract class CountDistinctSketchErrorBounds() extends UnaryExpression
  with ImplicitCastInputTypes
  with Serializable {

  val child: Expression
  val sketch: SketchType.Value
  val numStdDev: Int
  val schema = ScalaReflection.schemaFor[SketchErrorBounds].dataType.asInstanceOf[StructType]
  val enc: ExpressionEncoder[SketchErrorBounds] = ExpressionEncoder()

  private def catalystConverter: Any => Any = {
    val toRow = enc.createSerializer().asInstanceOf[Any => Any]
    value: Any => toRow(value).asInstanceOf[InternalRow]
  }


  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType)

  override def dataType: DataType = schema

  override protected def nullSafeEval(bytes: Any): Any = {
    sketch match {
      case THETA =>
        catalystConverter(
          SketchErrorBounds(
            ThetaSketch(bytes.asInstanceOf[Array[Byte]]).getLowerBound(numStdDev),
            ThetaSketch(bytes.asInstanceOf[Array[Byte]]).getEstimate,
            ThetaSketch(bytes.asInstanceOf[Array[Byte]]).getUpperBound(numStdDev)
          )
      )
      case HLL => catalystConverter(
        SketchErrorBounds(
          HyperLogLogSketch(bytes.asInstanceOf[Array[Byte]]).getLowerBound(numStdDev),
          HyperLogLogSketch(bytes.asInstanceOf[Array[Byte]]).getEstimate,
          HyperLogLogSketch(bytes.asInstanceOf[Array[Byte]]).getUpperBound(numStdDev)
        )
      )
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
    defineCodeGen(ctx, ev, child => s"NAMED_STRUCT('lower_bound', $sketchClass.apply($child).getLowerBound($numStdDev)," +
      s"'upper_bound', $sketchClass.apply($child).getUpperBound($numStdDev);")
  }

  override def toString: String = s"sketch_get_error_bounds($child)"

  override def prettyName: String = "sketch_get_error_bounds"

  private def fullName(clazz: Class[_]): String = {
    // class com.abc.Test -> com.abc.Text.MODULE$
    s"${clazz.toString.substring("class ".length)}.MODULE$$"
  }
}

case class CountDistinctHllSketchErrorBounds(child: Expression, numStdDev: Int) extends CountDistinctSketchErrorBounds {
  override val sketch: SketchType.Value = SketchType.HLL

  def this(child: Expression) = this(child, 2)

  def this(child: Expression, expr: Expression) =
    this(child, expr.asInstanceOf[Literal].value.asInstanceOf[Int])

  override protected def withNewChildInternal(newChild: Expression): CountDistinctHllSketchErrorBounds =
    copy(child = newChild)
}

case class CountDistinctThetaSketchErrorBounds(child: Expression, numStdDev: Int) extends CountDistinctSketchErrorBounds {
  override val sketch: SketchType.Value = SketchType.THETA

  def this(child: Expression) = this(child, 2)

  def this(child: Expression, expr: Expression) =
    this(child, expr.asInstanceOf[Literal].value.asInstanceOf[Int])

  override protected def withNewChildInternal(newChild: Expression): CountDistinctThetaSketchErrorBounds =
    copy(child = newChild)
}
