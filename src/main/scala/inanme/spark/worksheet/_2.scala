package inanme.spark.worksheet

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import spark.implicits._

object _200 extends App {
  val ghLog = spark.read.json("data/2015-03-01-0.json")
  ghLog.createOrReplaceTempView("data101")
  val m = spark.sql("select distinct(type) from data101")
  m.show(100)
}

object _210 extends App {
  val ghLog = spark.read.json("data/2015-03-01-0.json")
  ghLog.createOrReplaceTempView("data101")
  val m = spark.sql("select e.* from data101 e")
    .write.partitionBy("type").parquet("data/pack")
  //m.show(100)
}

object _211 extends App {
  val ghLog = spark.read.json("data/2015-03-01-0.json")
  ghLog.createOrReplaceTempView("data101")
  val m = spark.sql("select e.* from data101 e")
    .write.bucketBy(2, "type").parquet("data/pack")
  //m.show(100)
}
