package com.roblox.spark.sketches.theta

import com.roblox.spark.sketches.contract.TypedAggregationBuffer
import org.apache.datasketches.memory.{Memory, WritableMemory}
import org.apache.datasketches.common.{Family, ResizeFactor}
import org.apache.datasketches.theta.{SetOperation, Sketch, Sketches, Intersection, UpdateReturnState, UpdateSketch}
import org.apache.spark.TaskContext

class ThetaSketchIntersection(sketch: Sketch, var nomEntries: Int) {

  // no synchronization is needed as each task runs in a separate thread
  var intersection: Intersection =
    SetOperation.builder().setNominalEntries(nomEntries).setResizeFactor(ResizeFactor.X2).buildIntersection()

  def intersect(that: ThetaSketchIntersection): ThetaSketchIntersection = {
    this.intersection.intersect(this.getSketch)
    this.intersection.intersect(that.getSketch)
    val newBuffer = ThetaSketchIntersection(nomEntries)
    newBuffer.intersection = this.intersection
    return newBuffer
  }

  def toByteArray: Array[Byte] = {
    compact().getSketch.toByteArray
  }


  override def toString: String = sketch.toString

  //---- Internal state ----
  private def compact(ordered: Boolean = true): ThetaSketchIntersection = {
    if (sketch.isEmpty) {
      return new ThetaSketchIntersection(this.intersection.getResult(ordered, null), nomEntries)
    }

    if (!sketch.isCompact) {
      return new ThetaSketchIntersection(sketch.compact(ordered, null), nomEntries)
    }

    this
  }

  private def getSketch: Sketch = sketch

}

object ThetaSketchIntersection {
  private val familyByte: Int = 2

  def apply(bytes: Array[Byte]): ThetaSketchIntersection = {
    apply(bytes, 4096)
  }

  def apply(bytes: Array[Byte], nomEntities: Int): ThetaSketchIntersection = {
    val mem = WritableMemory.writableWrap(bytes)
    val familyId = extractFamilyByte(mem)

    Family.idToFamily(familyId) match {
      case Family.COMPACT => new ThetaSketchIntersection(Sketches.wrapSketch(mem), nomEntities)
      case Family.QUICKSELECT => new ThetaSketchIntersection(Sketches.wrapUpdateSketch(mem), nomEntities) //update family
      case _ => throw new UnsupportedOperationException(s"Unexpected familyId ${Family.idToFamily(familyId)}")
    }
  }

  //If creating a thetaSketchIntersection that will serve as the sink for many sketches getting merged into it create it with max size
  def apply(): ThetaSketchIntersection = {
    apply(Int.MaxValue)
  }

  def apply(nomEntries: Int): ThetaSketchIntersection = {
    new ThetaSketchIntersection(UpdateSketch.builder().setNominalEntries(nomEntries).setResizeFactor(ResizeFactor.X2).build(), nomEntries)
  }

  def apply(config: ThetaSketchConfig): ThetaSketchIntersection = new ThetaSketchIntersection(UpdateSketch
    .builder()
    .setNominalEntries(config.nominalEntities)
    .setResizeFactor(config.resizeFactor)
    .build(), config.nominalEntities
  )

  private def extractFamilyByte(mem: Memory): Int = {
    mem.getByte(familyByte) & 0XFF
  }
}
