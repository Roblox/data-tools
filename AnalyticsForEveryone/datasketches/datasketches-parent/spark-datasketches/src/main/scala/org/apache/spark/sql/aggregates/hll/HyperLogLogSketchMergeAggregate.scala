package org.apache.spark.sql.aggregates.hll

import com.roblox.spark.sketches.hll.{HllSketchConfig, HyperLogLogSketch}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.types.{AbstractDataType, BinaryType, DataType}

case class HyperLogLogSketchMergeAggregate(child: Expression,
                                           config: HllSketchConfig = HllSketchConfig(),
                                           override val mutableAggBufferOffset: Int = 0,
                                           override val inputAggBufferOffset: Int = 0)
  extends TypedImperativeAggregate[HyperLogLogSketch] with ImplicitCastInputTypes with UnaryLike[Expression] {

  // single expression constructor is mandatory for function to be used in SQL queries
  def this(child: Expression, lgConfigKInput: Expression) = this(child, HllSketchConfig(lgConfigKInput.asInstanceOf[Literal].value.asInstanceOf[Int]), 0, 0)

  // use max logK value for class initialization as it will be overwritten by smaller / same values from HLL sketches
  def this(child: Expression) = this(child, HllSketchConfig(lgConfigK = 21))

  def createAggregationBuffer(): HyperLogLogSketch = HyperLogLogSketch(config)

  def update(buffer: HyperLogLogSketch, input: InternalRow): HyperLogLogSketch = {
    val value = child.eval(input)

    if (value != null) {
      child.dataType match {
        case _ : BinaryType => buffer.merge(HyperLogLogSketch(value.asInstanceOf[Array[Byte]]))
        case other: DataType =>
          throw new UnsupportedOperationException(s"Unsupported data type ${other.catalogString}")
      }
    }

    buffer
  }

  def merge(buffer: HyperLogLogSketch, input: HyperLogLogSketch): HyperLogLogSketch = buffer.merge(input)

  def eval(buffer: HyperLogLogSketch): Any = buffer.toByteArray

  def serialize(buffer: HyperLogLogSketch): Array[Byte] = buffer.toByteArray

  def deserialize(bytes: Array[Byte]): HyperLogLogSketch = HyperLogLogSketch(bytes)

  def withNewMutableAggBufferOffset(newOffset: Int): ImperativeAggregate = {
    copy(mutableAggBufferOffset = newOffset)
  }

  def withNewInputAggBufferOffset(newOffset: Int): ImperativeAggregate = {
    copy(inputAggBufferOffset = newOffset)
  }

  def nullable: Boolean = false

  def dataType: DataType = BinaryType

  override def prettyName: String = "hll_sketch_merge"

  def inputTypes: Seq[AbstractDataType] = Seq(BinaryType)

  protected def withNewChildInternal(newChild: Expression): HyperLogLogSketchMergeAggregate = {
    copy(child = newChild)
  }
}

