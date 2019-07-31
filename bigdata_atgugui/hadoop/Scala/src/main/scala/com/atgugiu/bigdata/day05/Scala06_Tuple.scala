package com.atgugiu.bigdata.day05

/**
  * Create by chenqinping on 2019/5/5 22:33
  */
object Scala06_Tuple {

  def main(args: Array[String]): Unit = {

    //元组
    val tuple: (String, Int, String, Double) = ("zs", 111, "lisi", 3.1)

    println(tuple._4)


    for (elem <- tuple.productIterator) {
      println(elem)
    }

    val tuple1: (String, Int) = ("zs", 111)
    val tupleMap: Map[String, Int] = Map(("zs", 111))

    tupleMap.foreach(t=>{println(t._1,t._2)})
  }
}
