package org.apache.spark.sql.expressions

import com.roblox.spark.sketches.kll.{KarninLangLibertySketch, KllSketchConfig}
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.{AbstractDataType, BinaryType, DataType}

case class KllSketchUnion(left: Expression, right: Expression) extends BinaryExpression
  with ImplicitCastInputTypes
  with Serializable {

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType, BinaryType)

  override def dataType: DataType = BinaryType

  override protected def nullSafeEval(input1: Any, input2: Any): Any = {
    val kllSketch1 = KarninLangLibertySketch(input1.asInstanceOf[Array[Byte]])
    val kllSketch2 = KarninLangLibertySketch(input2.asInstanceOf[Array[Byte]])
    val maxK = math.min(kllSketch1.getK(), kllSketch2.getK())
    // Need start with empty or will throw "sketch memory is immutable" runtime exception
    KarninLangLibertySketch(KllSketchConfig(maxK)).merge(kllSketch1).merge(kllSketch2).toByteArray
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val sketchClass = fullName(KarninLangLibertySketch.getClass)

    // We don't use `nullSafeCodeGen` to support backward compatibility between Spark versions.
    // There were incompatible changes between Spark versions 2.3 and 2.4 that caused NoSuchMethodFound errors.
    // e.g ExprCode.value method returns String in Spark 2.3 and ExprValue return in 2.4
    defineCodeGen(ctx, ev, (left, right) => s"$sketchClass.apply($left).merge($sketchClass.apply($right));")
  }

  override def toString: String = s"kll_sketch_union($left, $right)"

  override def prettyName: String = "kll_sketch_union"

  private def fullName(clazz: Class[_]): String = {
    // class com.abc.Test -> com.abc.Text.MODULE$
    s"${clazz.toString.substring("class ".length)}.MODULE$$"
  }

  override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): KllSketchUnion =
    copy(left = newLeft, right = newRight)
}