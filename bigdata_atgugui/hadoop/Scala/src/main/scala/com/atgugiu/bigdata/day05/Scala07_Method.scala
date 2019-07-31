package com.atgugiu.bigdata.day05

/**
  * Create by chenqinping on 2019/5/5 22:43
  */
object Scala07_Method {

  def main(args: Array[String]): Unit = {
    val list = List(1, 2, 4, 3, 1, 3, 2, 3)
    /*
        println(list.sum)

        println(list.tails)

        println(list.max)

        println(list.min)

        println(list.last)*/

    /*
        println(list.product)

        println(list.reverse)

        val intToInts: Map[Int, List[Int]] = list.groupBy(x=>x)

        intToInts.foreach(t=>{t._1+"="+t._2})
    */


    val stringList = List("11", "35", "14", "26", "15", "28", "14", "26")

    /*    val stringToStrings: Map[String, List[String]] = stringList.groupBy(s=>s.substring(0,1))

        stringToStrings.foreach(t=>{print(t._1,t._2.size)})*/

  /*  val strings = stringList.sortWith((x, y) => (x == y))

    print(strings.mkString(","))*/

/*    val strings1 = stringList.sortWith((left,right)=>{ left.substring(1).toInt > right.substring(1).toInt})

    println(strings1.mkString(","))*/

   /* val s="111"
    println(s.substring(1))

    for (elem <- stringList){
      print(elem)
    }*/

 /*   val wordList = List("Hello", "Scala", "Hello", "World", "Hbase", "Hadoop", "Kafka", "Scala", "World")


    val tuples = wordList.groupBy(x=>x).map(t=>(t._1,t._2.size)).toList.sortWith((r,l)=>(r._2>l._2)).take(3)

    println(tuples)*/


    // TODO 扁平化操作
    // 将一个整体中的内容拆成一个一个的个体
    val lineList = List("Hello World", "Hello Scala", "Hello Hadoop")

    val tuples = lineList.flatMap(x=>x.split(" ")).groupBy(x=>x).map(t=>(t._1,t._2.size)).toList.sortWith((r,l)=>{r._2>l._2})

    println(tuples)
  }
}
