package org.apache.spark.sql.expressions

import com.roblox.spark.sketches.{KllCallType, SketchType}
import com.roblox.spark.sketches.kll.KarninLangLibertySketch
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes, Literal, UnaryExpression}
import org.apache.spark.sql.types.{AbstractDataType, BinaryType, DataType, Decimal, DoubleType}
abstract class CalculateCompactPercentile extends UnaryExpression
  with ImplicitCastInputTypes
  with Serializable {

  val child: Expression
  val sketch: SketchType.Value
  val kllCallType: KllCallType.Value
  val quantileOrRankValue: Double

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType)

  override def dataType: DataType = DoubleType

  override protected def nullSafeEval(bytes: Any): Any = {
    kllCallType match {
      case KllCallType.KllGetQuantile => KarninLangLibertySketch(bytes.asInstanceOf[Array[Byte]]).getQuantile(quantileOrRankValue)
      case KllCallType.KllGetRank => KarninLangLibertySketch(bytes.asInstanceOf[Array[Byte]]).getRank(quantileOrRankValue)
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val sketchClass = fullName(KarninLangLibertySketch.getClass)
    kllCallType match {
      case KllCallType.KllGetQuantile => defineCodeGen(ctx, ev, child => s"$sketchClass.apply($child).getQuantile($quantileOrRankValue);")
      case KllCallType.KllGetRank => defineCodeGen(ctx, ev, child => s"$sketchClass.apply($child).getRank($quantileOrRankValue);")
    }
  }

  override def toString: String = {
    kllCallType match {
      case KllCallType.KllGetQuantile => s"sketch_get_quantile($child)"
      case KllCallType.KllGetRank => s"sketch_get_rank($child)"
    }
  }

  override def prettyName: String = {
    kllCallType match {
      case KllCallType.KllGetQuantile => "sketch_get_quantile"
      case KllCallType.KllGetRank => "sketch_get_rank"
    }
  }

  private def fullName(clazz: Class[_]): String = {
    //class com.abc.Test -> com.abc.Text.MODULE$
    s"${clazz.toString.substring("class ".length)}.MODULE$$"
  }
}

case class CalculateKllSketchCompactQuantile(child: Expression, inputQuantile: Expression) extends CalculateCompactPercentile {
  private val inputType = inputQuantile.asInstanceOf[Literal].value
  override val kllCallType: KllCallType.Value = KllCallType.KllGetQuantile
  override val sketch: SketchType.Value = SketchType.KLL

  override val quantileOrRankValue: Double = inputType match {
    case _ : Decimal => inputType.asInstanceOf[Decimal].toDouble
    case _ : Double => inputType.asInstanceOf[Double]
  }

  override protected def withNewChildInternal(newChild: Expression): CalculateKllSketchCompactQuantile =
    copy(child = newChild)
}

case class CalculateKllSketchCompactRank(child: Expression, inputValue: Expression) extends CalculateCompactPercentile {
  private val inputType = inputValue.asInstanceOf[Literal].value
  override val kllCallType: KllCallType.Value = KllCallType.KllGetRank
  override val sketch: SketchType.Value = SketchType.KLL

  override val quantileOrRankValue: Double = inputType match {
    case _: Decimal => inputType.asInstanceOf[Decimal].toDouble
    case _: Double => inputType.asInstanceOf[Double]
  }

  override protected def withNewChildInternal(newChild: Expression): CalculateKllSketchCompactRank =
    copy(child = newChild)
}

