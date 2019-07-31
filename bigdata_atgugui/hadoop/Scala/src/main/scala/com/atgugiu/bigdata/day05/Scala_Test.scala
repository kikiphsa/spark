package com.atgugiu.bigdata.day05

import scala.collection.immutable.StringOps


/**
  * Create by chenqinping on 2019/5/6 8:49
  */
object Scala_Test {

  def main(args: Array[String]): Unit = {
    val list: List[(String, Int)] = List(("Hello Scala World", 4), ("Hello World", 3), ("Hello Hadoop", 2), ("Hello Hbase", 1))

    //(Hello,4)
    val wordInt = list.flatMap(t => {
      val line: String = t._1
      val words = line.split(" ")
      words.map(m => (m, t._2))
    })

//    wordInt.foreach(print)
//    (Hadoop,List((Hadoop,2)))(World,List((World,4), (World,3)))
    val stringToTuplesMap = wordInt.groupBy(x=>x._1)
//    stringToTuplesMap.foreach(print)
    val count = stringToTuplesMap.map(t => {
      val i: List[(String, Int)] = t._2

      val ints: List[Int] = i.map(x => x._2)
      (t._1,ints.sum)
    })

    val stringToInt = count.toList.sortWith((l,r)=>(l._2>r._2)).take(3)

    println(stringToInt)
  }
}