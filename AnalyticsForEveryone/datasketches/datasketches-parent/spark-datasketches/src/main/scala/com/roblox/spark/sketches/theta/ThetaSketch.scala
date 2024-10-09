package com.roblox.spark.sketches.theta

import com.roblox.spark.sketches.contract.TypedAggregationBuffer
import org.apache.datasketches.common.{ResizeFactor, Family}
import org.apache.datasketches.memory.{Memory, WritableMemory}
import org.apache.datasketches.theta.{SetOperation, Sketch, Sketches, Union, UpdateReturnState, UpdateSketch}
import org.apache.spark.TaskContext

/**
 * A simple wrapper for Update & Compact sketches families fitting map/reduce pattern.
 *
 * By default QuickSelect family is used.
 *
 * Default nominal factor is 4096
 * Thus has a Relative Standard Error (RSE) of +/- 1.56% at a confidence of
 * 68%; or equivalently, a Relative Error of +/- 3.1% at a confidence of 95.4%.
 *
 * For more detailed info see https://datasketches.github.io/docs/Theta/ThetaErrorTable.html
 */
class ThetaSketch(sketch: Sketch, var nomEntries: Int) extends TypedAggregationBuffer[ThetaSketch] {
  override type ValueType = Any
  override type ReturnType = UpdateReturnState

  // no synchronization is needed as each task runs in a separate thread
  var union: Union = _ // postpone initialization till the either PartialMerge or Complete/Final phase

  // as a first step Spark will create an empty buffer which will be used as a mutable aggregate per grouping key
  // the buffer itself is created via CompanionObject's apply method thus sketch must be UpdateSketch family
  override def update(value: Any): UpdateReturnState = value match {
    case double: Double     => sketch.asInstanceOf[UpdateSketch].update(double)
    case long: Long         => sketch.asInstanceOf[UpdateSketch].update(long)
    case int: Int           => sketch.asInstanceOf[UpdateSketch].update(int)
    case str: String        => sketch.asInstanceOf[UpdateSketch].update(str)
    case bytes: Array[Byte] => sketch.asInstanceOf[UpdateSketch].update(bytes)
    case _ => throw new IllegalStateException(s"Value [$value] of type [${value.getClass}] is unsupported by ThetaSketch")
  }

  override def merge(that: ThetaSketch): ThetaSketch = {
    logState(that)

    nomEntries = Math.min(nomEntries, that.nomEntries)

    // This could happen in several cases when the following conditions are met:
    // 1. Distinct is used as one of the aggregations e.g. ds.agg(countDistinct($"id), approx_count_distinct_theta($"id"))
    //    It will force Spark to choose an inefficient plan which leads to the two aggregate-exchange phases.
    //    For more details take look at SparkStrategies:401 and AggUtils.planAggregateWithOneDistinct

    // 2. When ObjectHashAggregateExec is chosen it uses HashMap as an internal state.
    //    It stores up to the 128 (might be changed) buffers (per grouping key) and when threshold is reached
    //    it fails back to the SortBasedAggregator.
    //    Now it has to merge an in memory buffers with a new buffers created during sorting.
    //    (controlled by OBJECT_AGG_SORT_BASED_FALLBACK_THRESHOLD)
    if (!getSketch.isEmpty) { // slow path

      getUnion.union(this.compact().getSketch)
      getUnion.union(that.getSketch) // must always be compacted

      // return a new buffer with empty sketch and populated union
      val newBuffer = ThetaSketch(nomEntries)
      newBuffer.union = this.getUnion
      logDebug(s"[MERGE] New sketch ${newBuffer.hashCode()} is created. Partition Id ${TaskContext.getPartitionId()} - Thread Name  ${Thread.currentThread().getName}")
      return newBuffer
    }

    // fast path
    getUnion.union(that.getSketch)
    this
  }

  def toByteArray: Array[Byte] = {
    compact().getSketch.toByteArray
  }

  def getEstimate: Long = Math.round(compact().getSketch.getEstimate)

  def getLowerBound(numStdDev: Int): Double = sketch.getLowerBound(numStdDev)

  def getUpperBound(numStdDev: Int): Double = sketch.getUpperBound(numStdDev)

  override def toString: String = sketch.toString

  //---- Internal state ----
  private def compact(ordered: Boolean = true): ThetaSketch = {
    if (sketch.isEmpty) {
      return new ThetaSketch(getUnion.getResult(ordered, null), nomEntries)
    }

    if (!sketch.isCompact) {
      return new ThetaSketch(sketch.compact(ordered, null), nomEntries)
    }

    this
  }

  private def getSketch: Sketch = sketch

  //If logk shrinks because of a merge operation the theta nom entries will not actually reduce since the union has
  //already been initialized. This should still give an upper bound of the min size of the first two merged theta sketches
  private def getUnion: Union = {
    if (union == null) union = SetOperation.builder().setNominalEntries(nomEntries).setResizeFactor(ResizeFactor.X2).buildUnion()
    union
  }

  private def logState(that: ThetaSketch): Unit = {
    logDebug(
      s"""
         |[MERGE]
         |This buffer: ${this.hashCode()} with that buffer ${that.hashCode()}.
         |This Union state: ${if (union == null) "Empty, a new union will be created, current buffer will be compacted, slow path" else "Populated, no compaction is required, fast path"}
         |This Sketch state: ${if (this.sketch.isEmpty) "Empty " else "Full "} and ${if (this.sketch.isCompact) "Compacted" else "QuickSelect"}
         |That Sketch state: ${if (that.getSketch.isEmpty) "Empty " else "Full "} and ${if (that.getSketch.isCompact) "Compacted" else "ERROR! - QuickSelect"}
         |Partition Id ${TaskContext.getPartitionId()} - Thread Name  ${Thread.currentThread().getName}
       """.stripMargin)
  }
}

object ThetaSketch {
  private val familyByte: Int = 2

  def apply(bytes: Array[Byte]): ThetaSketch = {
    apply(bytes, 4096)
  }

  def apply(bytes: Array[Byte], nomEntities: Int): ThetaSketch = {
    val mem = WritableMemory.writableWrap(bytes)
    val familyId = extractFamilyByte(mem)

    Family.idToFamily(familyId) match {
      case Family.COMPACT => new ThetaSketch(Sketches.wrapSketch(mem), nomEntities)
      case Family.QUICKSELECT => new ThetaSketch(Sketches.wrapUpdateSketch(mem), nomEntities) //update family
      case _ => throw new UnsupportedOperationException(s"Unexpected familyId ${Family.idToFamily(familyId)}")
    }
  }

  //If creating a thetasketch that will serve as the sink for many sketches getting merged into it create it with max size
  def apply(): ThetaSketch = {
    apply(Int.MaxValue)
  }

  def apply(nomEntries: Int): ThetaSketch = {
    new ThetaSketch(UpdateSketch.builder().setNominalEntries(nomEntries).setResizeFactor(ResizeFactor.X2).build(), nomEntries)
  }

  def apply(config: ThetaSketchConfig): ThetaSketch = new ThetaSketch(UpdateSketch
    .builder()
    .setNominalEntries(config.nominalEntities)
    .setResizeFactor(config.resizeFactor)
    .build(), config.nominalEntities
  )

  private def extractFamilyByte(mem: Memory): Int = {
    mem.getByte(familyByte) & 0XFF
  }
}
