package org.apache.spark.sql.aggregates.kll

import com.roblox.spark.sketches.kll.{KarninLangLibertySketch, KllSketchConfig}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.types.{AbstractDataType, BinaryType, DataType}

case class KllSketchMergeAggregate(child: Expression,
                                   config: KllSketchConfig,
                                   override val mutableAggBufferOffset: Int = 0,
                                   override val inputAggBufferOffset: Int = 0)
extends TypedImperativeAggregate[KarninLangLibertySketch] with ImplicitCastInputTypes with UnaryLike[Expression]{

  def this(child: Expression, kInput: Expression) = this(
    child, KllSketchConfig(kInput.asInstanceOf[Literal].value.asInstanceOf[Int]), 0, 0
  )

  // single expression constructor is mandatory for function to be used in SQL queries
  // use the max allowed value as default to avoid lose precision
  def this(child: Expression) = this(child, KllSketchConfig(k=1600), 0, 0)

  override def createAggregationBuffer(): KarninLangLibertySketch = KarninLangLibertySketch()

  override def update(buffer: KarninLangLibertySketch, input: InternalRow): KarninLangLibertySketch = {
    val value = child.eval(input)

    if (value != null) {
      child.dataType match {
        case _ : BinaryType => buffer.merge(KarninLangLibertySketch(value.asInstanceOf[Array[Byte]]))
        case other: DataType =>
          throw new UnsupportedOperationException(s"Unsupported data type ${other.catalogString}")
      }
    }

    buffer
  }

  override def merge(buffer: KarninLangLibertySketch, input: KarninLangLibertySketch): KarninLangLibertySketch = buffer.merge(input)

  override def eval(buffer: KarninLangLibertySketch): Any = buffer.toByteArray

  override def serialize(buffer: KarninLangLibertySketch): Array[Byte] = buffer.toByteArray

  override def deserialize(bytes: Array[Byte]): KarninLangLibertySketch = KarninLangLibertySketch(bytes)

  override protected def withNewChildInternal(newChild: Expression): KllSketchMergeAggregate = copy(child = newChild)

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType)

  override def withNewMutableAggBufferOffset(newOffset: Int): ImperativeAggregate = copy(mutableAggBufferOffset = newOffset)

  override def withNewInputAggBufferOffset(newOffset: Int): ImperativeAggregate = copy(inputAggBufferOffset = newOffset)

  override def nullable: Boolean = false

  override def dataType: DataType = BinaryType

  override def prettyName: String = "kll_sketch_merge"
}
