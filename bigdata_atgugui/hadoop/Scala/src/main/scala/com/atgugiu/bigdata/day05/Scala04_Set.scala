package com.atgugiu.bigdata.day05

/**
  * Create by chenqinping on 2019/5/5 22:20
  */
object Scala04_Set {

  def main(args: Array[String]): Unit = {

    val set: Set[Int] = Set(1,2,3,4,1)

    /*println(set.mkString(","))

    println(set -2)

    set.foreach(println)*/

    import scala.collection.mutable
    val ints = mutable.Set(1,2,3,4,1)

    val ints1 = ints.+(5)

    println(ints==ints1)
  }

}
