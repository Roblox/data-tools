package org.apache.spark.sql.aggregates.kll

import com.roblox.spark.sketches.kll.{KarninLangLibertySketch, KllSketchConfig}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.types.{AbstractDataType, BinaryType, DataType, NumericType}

case class KllSketchBuildAggregate(child: Expression,
                                   config: KllSketchConfig,
                                   override val mutableAggBufferOffset: Int = 0,
                                   override val inputAggBufferOffset: Int = 0)
extends TypedImperativeAggregate[KarninLangLibertySketch] with ImplicitCastInputTypes with UnaryLike[Expression]{

  def this(child: Expression, kInput: Expression) = this(child, KllSketchConfig(kInput.asInstanceOf[Literal].value.asInstanceOf[Int]), 0, 0)

  override def createAggregationBuffer(): KarninLangLibertySketch = KarninLangLibertySketch(config)

  override def update(buffer: KarninLangLibertySketch, input: InternalRow): KarninLangLibertySketch = {
    val value = child.eval(input)

    // Ignore empty rows, for example: kll_sketch(null)
    if (value == null)
      return buffer

    // Update the buffer

    child.dataType match {
      case number: NumericType => buffer.update(number.numeric.toDouble(value.asInstanceOf[number.InternalType]))
      case other: DataType =>
        throw new UnsupportedOperationException(s"Unsupported data type ${other.catalogString}")
    }

    buffer
  }

  override def merge(buffer: KarninLangLibertySketch, input: KarninLangLibertySketch): KarninLangLibertySketch = buffer.merge(input)

  override def eval(buffer: KarninLangLibertySketch): Any = buffer.toByteArray

  override def serialize(buffer: KarninLangLibertySketch): Array[Byte] = buffer.toByteArray

  override def deserialize(bytes: Array[Byte]): KarninLangLibertySketch = KarninLangLibertySketch(bytes)

  override protected def withNewChildInternal(newChild: Expression): KllSketchBuildAggregate = copy(child = newChild)

  override def withNewMutableAggBufferOffset(newOffset: Int): ImperativeAggregate = copy(mutableAggBufferOffset = newOffset)

  override def withNewInputAggBufferOffset(newOffset: Int): ImperativeAggregate = copy(inputAggBufferOffset = newOffset)

  override def inputTypes: Seq[AbstractDataType] = Seq(NumericType)

  override def nullable: Boolean = false

  override def dataType: DataType = BinaryType

  override def prettyName: String = "kll_sketch_build"

}
