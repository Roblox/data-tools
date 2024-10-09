package com.roblox.spark.sketches.contract

trait Mergeable[T] {
  def merge(that: T): T
}

