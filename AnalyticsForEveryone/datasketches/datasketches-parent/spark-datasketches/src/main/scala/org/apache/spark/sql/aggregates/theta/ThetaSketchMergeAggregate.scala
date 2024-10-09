package org.apache.spark.sql.aggregates.theta

import com.roblox.spark.sketches.theta.{ThetaSketch, ThetaSketchConfig}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.types.{AbstractDataType, BinaryType, DataType}

case class ThetaSketchMergeAggregate(child: Expression,
                                     config: ThetaSketchConfig = ThetaSketchConfig(),
                                     override val mutableAggBufferOffset: Int = 0,
                                     override val inputAggBufferOffset: Int = 0)
  extends TypedImperativeAggregate[ThetaSketch] with ImplicitCastInputTypes with UnaryLike[Expression] {


  def this(child: Expression, nominalEntitiesInput: Expression) =
    this(child, ThetaSketchConfig(nominalEntities = nominalEntitiesInput.asInstanceOf[Literal].value.asInstanceOf[Int]), 0, 0)
  def this(child: Expression) = this(child, ThetaSketchConfig(), 0, 0)

  def createAggregationBuffer(): ThetaSketch = ThetaSketch(config)

  def update(buffer: ThetaSketch, inputRow: InternalRow): ThetaSketch = {
    val value = child.eval(inputRow)

    // Ignore empty rows, for example: theta_sketch(null)
    if (value != null) {
      child.dataType match {
        case _ : BinaryType => buffer.merge(ThetaSketch(value.asInstanceOf[Array[Byte]], config.nominalEntities))
        case other: DataType =>
          throw new UnsupportedOperationException(s"Unsupported data type ${other.catalogString}")
      }
    }

    buffer
  }

  def merge(buffer: ThetaSketch, input: ThetaSketch): ThetaSketch = {
    buffer.merge(input)
    buffer
  }

  def eval(buffer: ThetaSketch): Any = buffer.toByteArray

  def serialize(buffer: ThetaSketch): Array[Byte] = buffer.toByteArray

  def deserialize(bytes: Array[Byte]): ThetaSketch = ThetaSketch(bytes, config.nominalEntities)

  def withNewMutableAggBufferOffset(newOffset: Int): ImperativeAggregate = {
    copy(mutableAggBufferOffset = newOffset)
  }

  def withNewInputAggBufferOffset(newOffset: Int): ImperativeAggregate = {
    copy(inputAggBufferOffset = newOffset)
  }

  def nullable: Boolean = false

  def dataType: DataType = BinaryType

  override def prettyName: String = "theta_sketch_merge"

  def inputTypes: Seq[AbstractDataType] = Seq(BinaryType)

  protected def withNewChildInternal(newChild: Expression): ThetaSketchMergeAggregate = {
    copy(child = newChild)
  }
}
