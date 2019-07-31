package com.atgugiu.bigdata.day05

import scala.collection.immutable.StringOps


/**
  * Create by chenqinping on 2019/5/6 8:49
  */
object Scala_Test1 {

  def main(args: Array[String]): Unit = {
    val list: List[(String, Int)] = List(("Hello Scala World", 4), ("Hello World", 3), ("Hello Hadoop", 2), ("Hello Hbase", 1))


    val tuples = list.flatMap(x => {

      val value: String = x._1

      val words = value.split(" ")
      words.map(t => {
        (t, x._2)
      })
    })
    println(tuples)
  }
}