package inanme.spark.worksheet

import org.apache.spark.sql.{ Dataset, RelationalGroupedDataset, Row }
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import spark.implicits._

object _300 extends App {
  val m = sc.range(10, 100)
    .map(i => (i, i + 1, i + 2))
    .toDF("i1", "i2", "i3")
  m.printSchema()
  m.createOrReplaceTempView("tonas")
  spark.sql("select max(i1 * i2 * i3) as max from tonas").show(1)
  spark.sql("select min(i1 * i2 * i3) as min from tonas").show(1)
  spark.sql("select avg(i1 * i2 * i3) as avg from tonas").show(1)

}

object _301 extends App {

  val rdd = sc.range(10, 100).map(i => Row(i, i + 1, i + 2))
  val schema: StructType = StructType(Array(
    StructField("i1", LongType, nullable = false),
    StructField("i2", LongType, nullable = true),
    StructField("i3", LongType, nullable = true)
  ))

  val m = spark.createDataFrame(rdd, schema)

  m.createOrReplaceTempView("tonas")
  spark.sql("select max(i1 * i2 * i3) as max from tonas").show(1)
  spark.sql("select min(i1 * i2 * i3) as min from tonas").show(1)
  spark.sql("select avg(i1 * i2 * i3) as avg from tonas").show(1)

}

object _302 extends App {
  (10 to 100).map(i => (i, i + 1, i + 2)).toDF("i1", "i2", "i3").createOrReplaceTempView("tonas")
  spark.sql("select max(i1 * i2 * i3) as max from tonas").show(1)
  spark.sql("select min(i1 * i2 * i3) as min from tonas").show(1)
  spark.sql("select avg(i1 * i2 * i3) as avg from tonas").show(1)
}

object _303 extends App {
  val list = 1 to 104
  val m: Dataset[Int] = list.toDS()
  m.agg(
    min(columnName = "value") as "min",
    max(columnName = "value") as "max",
    avg(columnName = "value") as "avg",
    sum(columnName = "value") as "sum").show()
}

object _310 extends App {

  case class Data(key: String, value: BigInt)

  val m = sc.range(10, 100)
    .map(i => (s"key${i % 10}", i))
    .toDF("key", "value")
    .as[Data]
    .groupByKey(_.key)
    .count()
    .show(1000)
}

object _320 extends App {

  val df = sc.range(10, 100)
    .map(i => (i, i + 1, i + 2))
    .toDF("i1", "i2", "i3")

  df.select('i1, 'i2, ('i1 * 'i2) as "mul", 'i3).where('i1 * 'i2 > 'i3).show()

}

object _330 extends App {

  case class Student(name: String, gender: String, weight: Int, graduation_year: Int)

  val studentsDF = Seq(
    Student("John", "M", 180, 2015),
    Student("Mary", "F", 110, 2015),
    Student("Derek", "M", 200, 2015),
    Student("Julie", "F", 109, 2015),
    Student("Allison", "F", 105, 2015),
    Student("kirby", "F", 115, 2016),
    Student("Jeff", "M", 195, 2016)).toDF
  // calculating the average weight for each gender per graduation year
  val graduationByYear: RelationalGroupedDataset = studentsDF.groupBy("graduation_year")

  graduationByYear.pivot("gender").avg("weight").show()

  graduationByYear.pivot("gender")
    .agg(
      min("weight").as("min"),
      max("weight").as("max"),
      avg("weight").as("avg")
    ).show()

  graduationByYear.pivot("gender", Seq("M"))
    .agg(
      min("weight").as("min"),
      max("weight").as("max"),
      avg("weight").as("avg")
    ).show()

}

object _340 extends App {

  case class Employee(first_name: String, dept_no: Long)

  val employeeDF = Seq(
    Employee("John", 31),
    Employee("Jeff", 33),
    Employee("Mary", 33),
    Employee("Mandy", 34),
    Employee("Julie", 34),
    Employee("Kurt", null.
      asInstanceOf[Int])
  ).toDF

  case class Dept(id: Long, name: String)

  val deptDF = Seq(
    Dept(31, "Sales"),
    Dept(33, "Engineering"),
    Dept(34, "Finance"),
    Dept(35, "Marketing")
  ).toDF
  // register them as views so we can use SQL for perform joins
  employeeDF.createOrReplaceTempView("employees")
  deptDF.createOrReplaceTempView("departments")

  spark.sql("select nvl(first_name, 'N/A') first_name, d.name from employees e right JOIN departments d on dept_no == id").show
  spark.sql("select e.* from employees e left anti JOIN departments d on dept_no == id").show
  spark.sql("select d.* from departments d left anti JOIN employees e on dept_no == id").show
  spark.sql("select e.* from employees e left semi JOIN departments d on dept_no == id").show
  spark.sql("select d.* from departments d left semi JOIN employees e on dept_no == id").show
}