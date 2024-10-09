package com.roblox.spark.sketches

import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions_ex.{theta_sketch_build, _}

case class Player(userkey: Int, universe_id: Int, country: Int,
                  age_group: Int, gender: Int, platform: Int, os: Int,
                  locale: Int, is_new_user: Int, visits: Int, time_spent_secs: Int, ds: String)

object PlayerGenerator {
  def createPlayer(i: Int, ds: String): Player = {
    Player(
      userkey = i, universe_id = createDistribution(1000, 10000), country = createDistribution(0, 100),
      age_group = createDistribution(0, 3), gender = i%2, platform = createDistribution(0, 4, 1.3),
      os = createDistribution(0, 4, 1.5), locale = createDistribution(0, 20), is_new_user = createDistribution(0, 1, 6),
      visits = createDistribution(0, 10, 4), time_spent_secs = createDistribution(10, 10000, 1.5), ds = ds
    )
  }

  def generatePlayers(numPlayers: Int, idx: Int): List[Player] = {
    List.tabulate(numPlayers)(i => createPlayer(i, "2024-01-01"))
  }

  def createDistribution(min: Int, max: Int, power: Double = 2): Int = {
    val range = max - min
    math.floor(math.pow(math.random(), power) * range).toInt + min
  }
}


class DataGeneratorTest extends SharedSparkSessionSuite {

  import spark.implicits._

  import PlayerGenerator._

  test("Test Generate Data for example usage") {

    val ds = spark.createDataset(List.range(0, 100))

    val playersDS = ds.flatMap(i => generatePlayers(20000, i))

    playersDS.show()

    val groupingColumns = List("universe_id", "country", "locale", "age_group", "gender", "platform", "is_new_user")

    val groupedDS = playersDS.groupBy(groupingColumns.map(col): _*).agg(
      sum("visits") as "visits",
      sum("time_spent_secs") as "time_spent_secs",
      theta_sketch_build($"userkey", lit(4096)) as "userkey_sketch",
      hll_sketch_build($"userkey") as "userkey_hll"
    )

    groupedDS.show()
  }

}
