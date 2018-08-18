package inanme.spark.worksheet

object _1 extends App {
  println(sc.parallelize(1 to 104).sum())
}
