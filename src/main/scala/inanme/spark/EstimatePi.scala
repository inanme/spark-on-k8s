package inanme.spark

import org.apache.spark.sql.SparkSession

object EstimatePi {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("estimate-pi")
      .getOrCreate()

    val NUM_SAMPLES = 1000000

    val count = spark.sparkContext
      .parallelize(1 to NUM_SAMPLES)
      .filter { _ =>
        val x = math.random
        val y = math.random
        x * x + y * y < 1
      }
      .count()

    println(s"Pi is roughly ${4.0 * count / NUM_SAMPLES}")

    spark.stop()
  }
}
