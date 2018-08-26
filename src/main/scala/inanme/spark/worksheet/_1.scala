package inanme.spark.worksheet

object _100 extends App {
  val list = 1 to 104
  println(sc.parallelize(list).sum())
}

object _110 extends App {
  val list = List(("candy1", 5.2), ("candy2", 3.5), ("candy1", 2.0), ("candy3", 6.0))
  println(sc.parallelize(list).collectAsMap())
}

object _120 extends App {
  val list = 1 to 104
  val m = sc.parallelize(list, 2)
    .mapPartitionsWithIndex {
      case (index, iter) =>
        println(s"index is $index")
        iter
    }
  println(m.sum())
}
