package inanme.spark.worksheet

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object _1 extends App {

  // Set off the vebose log messages
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val spark = SparkSession.builder
    .master("spark://menter:7077")
    .appName("scalaWorksheet")
    .config("spark.ui.showConsoleProgress", false)
    .getOrCreate()

  lazy val sc = spark.sparkContext

  println(sc.parallelize(1 to 104).sum())


  spark.stop()


}
