package inanme.spark.worksheet

import spark.implicits._

object _30 extends App {

  sc.range(10, 100)
    .map(i => (i, i + 1, i + 2))
    .toDF("i1", "i2", "i3")
    .createOrReplaceTempView("tonas")
  spark.sql("select max(i1 * i2 * i3) as max from tonas").show(1)
  spark.sql("select min(i1 * i2 * i3) as min from tonas").show(1)
  spark.sql("select avg(i1 * i2 * i3) as avg from tonas").show(1)

}

object _31 extends App {

  case class Data(key: String, value: BigInt)

  val m = sc.range(10, 100)
    .map(i => (s"key${i % 10}", i))
    .toDF("key", "value")
    .as[Data]
    .groupByKey(_.key)
    .count()
    .show(1000)
}

object _32 extends App {

  sc.range(10, 100)
    .map(i => (i, i + 1, i + 2))
    .toDF("i1", "i2", "i3")

}
