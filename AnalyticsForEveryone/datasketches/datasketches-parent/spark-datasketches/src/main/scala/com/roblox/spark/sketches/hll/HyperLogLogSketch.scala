package com.roblox.spark.sketches.hll

import com.roblox.spark.sketches.contract.TypedAggregationBuffer
import org.apache.datasketches.memory.WritableMemory
import org.apache.datasketches.hll.{HllSketch, Union}
import org.apache.spark.TaskContext

class HyperLogLogSketch(sketch: HllSketch) extends TypedAggregationBuffer[HyperLogLogSketch] {
  override type ValueType = Any
  override type ReturnType = Unit

  var union: Union = _ // postpone initialization till the reduce phase

  override def update(value: Any): Unit = value match {
    case double: Double     => sketch.update(double)
    case long: Long         => sketch.update(long)
    case int: Int           => sketch.update(int)
    case str: String        => sketch.update(str)
    case bytes: Array[Byte] => sketch.update(bytes)
    case _ => throw new IllegalStateException(s"Value [$value] of type [${value.getClass}] is unsupported by HllSketch")
  }

  override def merge(that: HyperLogLogSketch): HyperLogLogSketch = {
    logState(that)

    //The buffer will default to size 12 but be empty. Don't downgrade our hll if a more accurate estimate was built
    val lgMaxK = if(sketch.isEmpty) {
      that.getSketch.getLgConfigK
    } else if(that.getSketch.isEmpty) {
      sketch.getLgConfigK
    } else {
      Math.min(sketch.getLgConfigK, that.getSketch.getLgConfigK)
    }

    // see ThetaSketch merge for more detailed explanation
    if (!getSketch.isEmpty) {
      getUnion(lgMaxK).update(this.getSketch)
      getUnion(lgMaxK).update(that.getSketch)

      val newBuffer = HyperLogLogSketch()
      newBuffer.union = this.getUnion(lgMaxK)
      logDebug(s"[MERGE] New sketch ${newBuffer.hashCode()} is created. Partition Id ${TaskContext.getPartitionId()} - Thread Name  ${Thread.currentThread().getName}")
      return newBuffer
    }

    getUnion(lgMaxK).update(that.getSketch)
    this
  }

  def toByteArray: Array[Byte] = {
    if (getSketch.isEmpty) {
      return getUnion(lgK = getSketch.getLgConfigK).toCompactByteArray
    }
    getSketch.toCompactByteArray
  }

  def getEstimate: Long = Math.round(sketch.getEstimate)

  def getLowerBound(numStdDev: Int): Double = sketch.getLowerBound(numStdDev)

  def getUpperBound(numStdDev: Int): Double = sketch.getUpperBound(numStdDev)

  override def toString: String = sketch.toString

  private def getSketch: HllSketch = sketch

  private def getUnion(lgK: Int): Union = {
    if (union == null) union = new Union(lgK)
    union
  }

  private def logState(that: HyperLogLogSketch): Unit = {
    logDebug(
      s"""
         |[MERGE]
         |This buffer: ${this.hashCode()} with that buffer ${that.hashCode()}.
         |This Union state: ${if (union == null) "Empty, a new union will be created, slow path" else "Populated, fast path"}
         |This Sketch state: ${if (this.sketch.isEmpty) "Empty " else "Full "} and ${if (this.sketch.isCompact) "Compacted" else "Not Compacted"}
         |That Sketch state: ${if (that.getSketch.isEmpty) "Empty " else "Full "} and ${if (that.getSketch.isCompact) "Compacted" else "Not Compacted"}
         |Partition Id ${TaskContext.getPartitionId()} - Thread Name  ${Thread.currentThread().getName}
       """.stripMargin)
  }
}

object HyperLogLogSketch {

  def apply(): HyperLogLogSketch = new HyperLogLogSketch(new HllSketch())

  def apply(config: HllSketchConfig): HyperLogLogSketch = new HyperLogLogSketch(new HllSketch(config.lgConfigK, config.tgtHllType))

  def apply(bytes: Array[Byte]): HyperLogLogSketch = {
    val mem = WritableMemory.writableWrap(bytes)
    new HyperLogLogSketch(HllSketch.wrap(mem))
  }
}
