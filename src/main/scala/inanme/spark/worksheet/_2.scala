package inanme.spark.worksheet

import scala.io.Source
import spark.implicits._

object _2 extends App {

  val ghLog = spark.read.json("data/2015-03-01-0.json")
  ghLog.createOrReplaceTempView("data101")
  val m = spark.sql("select distinct(type) from data101")
  m.show(100)

//  val pushes = ghLog.filter("type = 'PushEvent'")
//  val grouped = pushes.groupBy("actor.login").count
//  val ordered = grouped.orderBy(grouped("count").desc)
//  val employees: Seq[String] =
//    Source.fromFile("spark-in-action/ch03/ghEmployees.txt").getLines.map(_.trim).toList
//
//  val bcEmployees = sc.broadcast(employees)
//
//  val isEmp: String => Boolean = user => user != null
//  val isEmp1: String => Boolean = user => bcEmployees.value.contains(user)
//  val isEmployee = spark.udf.register("isemp", isEmp)
//  val isEmployee1 = spark.udf.register("isemp1", isEmp1)
//  val filtered = ordered.filter(isEmployee1($"login"))
//  filtered.show()

}
