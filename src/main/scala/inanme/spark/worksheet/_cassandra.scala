package inanme.spark.worksheet

import org.apache.spark.sql.SparkSession
import com.datastax.spark.connector._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.cassandra._

object _cassandra extends App {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)


  val spark = SparkSession.builder
    .master("spark://eleven:7077")
    .appName("scalaWorksheet")
    .config("spark.driver.bindAddress","eleven")
    .config("spark.jars", "/Users/mert/.ivy2/cache/com.datastax.spark/spark-cassandra-connector_2.11/jars/spark-cassandra-connector_2.11-2.3.1.jar,/Users/mert/DEV/mine/spark-on-k8s/target/scala-2.11/my-spark_2.11-0.jar")
    .config("spark.ui.showConsoleProgress", false)
    .config("spark.cassandra.connection.host", "127.0.0.1")
    .getOrCreate()

  lazy val sc = spark.sparkContext

  val rdd = sc.cassandraTable("test", "kv")
  println(rdd.count)
  println(rdd.first)
  println(rdd.map(_.getInt("value")).sum)

  spark.stop()

}
