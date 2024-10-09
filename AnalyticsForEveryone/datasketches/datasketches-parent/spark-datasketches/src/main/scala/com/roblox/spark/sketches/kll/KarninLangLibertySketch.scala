package com.roblox.spark.sketches.kll

import com.roblox.spark.sketches.contract.TypedAggregationBuffer
import org.apache.datasketches.kll.KllDoublesSketch
import org.apache.datasketches.memory.WritableMemory
import org.apache.spark.TaskContext

class KarninLangLibertySketch(sketch: KllDoublesSketch) extends TypedAggregationBuffer[KarninLangLibertySketch] {
  override type ValueType = Double
  override type ReturnType = Unit

  override def update(value: Double): Unit =
    sketch.update(value)

  override def merge(that: KarninLangLibertySketch): KarninLangLibertySketch = {
    logState(that)
    sketch.merge(that.getSketch)
    this
  }

  def toByteArray: Array[Byte] = sketch.toByteArray

  def getQuantile(quantile: Double): Double = sketch.getQuantile(quantile)

  def getRank(value: Double): Double = sketch.getRank(value)


  override def toString: String = sketch.toString

  private def getSketch: KllDoublesSketch = sketch

  def getK(): Int = sketch.getK()

  private def logState(that: KarninLangLibertySketch): Unit = {
    logDebug(
      s"""
         |[MERGE]
         |This buffer: ${this.hashCode()} with that buffer ${that.hashCode()}.
         |This Sketch state: ${if (this.sketch.isEmpty) "Empty " else "Full "}
         |That Sketch state: ${if (that.getSketch.isEmpty) "Empty " else "Full "}
         |Partition Id ${TaskContext.getPartitionId()} - Thread Name  ${Thread.currentThread().getName}
       """.stripMargin)
  }
}

object KarninLangLibertySketch {

  def apply(): KarninLangLibertySketch = {
    new KarninLangLibertySketch(KllDoublesSketch.newHeapInstance())
  }

  def apply(config: KllSketchConfig): KarninLangLibertySketch = new KarninLangLibertySketch(KllDoublesSketch.newHeapInstance(config.k))

  def apply(bytes: Array[Byte]): KarninLangLibertySketch = {
    val mem = WritableMemory.writableWrap(bytes)
    new KarninLangLibertySketch(KllDoublesSketch.wrap(mem))
  }
}
