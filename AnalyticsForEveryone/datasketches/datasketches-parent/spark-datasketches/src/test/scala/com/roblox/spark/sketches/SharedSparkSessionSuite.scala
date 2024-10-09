package com.roblox.spark.sketches

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

trait SharedSparkSessionSuite extends AnyFunSuite with BeforeAndAfterAll {

  lazy val spark: SparkSession = SparkSession
    .builder()
    .appName("data-sketches")
    .master("local[7]")
    .config("spark.network.timeout", "10000001")
    .config("spark.executor.heartbeatInterval", "10000000")
    .config("spark.storage.blockManagerSlaveTimeoutMs", "10000000")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.extraJavaOptions", "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED")
    .config("spark.executor.extraJavaOptions", "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED")
    .getOrCreate()

  override protected def beforeAll(): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
  }


  override protected def afterAll(): Unit = {
    spark.stop()
  }

}
