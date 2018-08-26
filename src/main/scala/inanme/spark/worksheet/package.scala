package inanme.spark

import org.apache.log4j.{ Level, Logger }
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

package object worksheet {
  val master = "spark://menter:7077"
  val masterLocal = "local[2]"
  val logger: Logger = Logger.getLogger("scalaWorksheet")
  val spark: SparkSession = SparkSession.builder
    .master(masterLocal)
    .appName("scalaWorksheet")
    .config("spark.ui.showConsoleProgress", false)
    .config("spark.submit.deployMode", "cluster")
    .config("spark.executor.extraClassPath", "/Users/mert/temp/my-spark/target/scala-2.11/classes")
    //.config("spark.files", "target/scala-2.11/classes")
    //.config("spark.jars", "target/scala-2.11/my-spark_2.11-0.jar")
    //.config("spark.driver.extraJavaOptions", "-Dlog4j.configuration=my-spark_2.11-0.jar#log4j.properties")
    .getOrCreate()

  println(spark.version)
  //println(spark.conf.getAll.foreach(println))

  val sc: SparkContext = spark.sparkContext

  Runtime.getRuntime.addShutdownHook(new Thread() {
    override def run(): Unit = spark.close
  })
}
