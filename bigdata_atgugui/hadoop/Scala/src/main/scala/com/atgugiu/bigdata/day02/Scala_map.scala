package com.atgugiu.bigdata.day02

/**
  * Create by chenqinping on 2019/5/5 16:44
  */
object Scala_map {


  def main(args: Array[String]): Unit = {
    val strings = List("Hello","Hive","hadoop","hive","Scala","Hello", "Hive", "hadoop")

    val tuples = strings.groupBy(x=>x).map(t=>{(t._1,t._2.size)}).toList.sortWith((r,l)=>{r._2>l._2}).take(3)

    println(tuples)
  }

}
