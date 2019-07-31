package com.atgugiu.bigdata.day05

/**
  * Create by chenqinping on 2019/5/5 22:26
  */
object Scala05_Map {

  def main(args: Array[String]): Unit = {

    val map = Map("a" -> 1, "b" -> 2, "c" -> 3)

    println(map.mkString(","))
    val map1: Map[String, Int] = map.+("d"-> 4)

    println(map1.mkString(","))

    val map2: Map[String, Int] = map.+("a"-> 5)
    println(map2.mkString(","))

    //删除

    val deleteMap = map-("a")
    println(deleteMap.mkString(","))

    val updatedMap = map.updated("b",19)
    println(updatedMap.mkString(","))


    println(map.get("a").getOrElse(0))


  }

}
