package com.atgugiu.bigdata.day02

/**
  * Create by chenqinping on 2019/5/5 21:22
  */
object Scala07_Method {

  def main(args: Array[String]): Unit = {
    val wordList = List("Hello", "Scala", "Hello", "World", "Hbase", "Hadoop", "Kafka", "Scala", "World")

   /* //分到一组

    val groubByKey = wordList.groupBy(x=>x)
    println(groubByKey)

    /// 2) 将数据结构进行转换
    val map = groubByKey.map(t=>{(t._1,t._2.size)})
    println(map)

    //倒叙
    val tuples = map.toList.sortWith((r,l)=>{r._2>l._2})

    println(tuples)

    val take3 = tuples.take(3)
    println(take3)*/

    val tuples = wordList.groupBy(x=>x).map(t=>(t._1,t._2.size)).toList.sortWith((r,l)=>{r._2>l._2}).take(3)

    println(tuples)

  }
}
