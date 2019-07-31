package com.atgugiu.bigdata.day05

/**
  * Create by chenqinping on 2019/5/5 23:10
  */
object Scala08_Method1 {

  def main(args: Array[String]): Unit = {
    // TODO 拉链 : ZIP
    val list1 = List(1,2,3,7)
    val list2 = List(3,4,5,6)

    println(list1.zip(list2))

    println(list1.union(list2))

    println(list1.intersect(list2))

    println(list1.diff(list2))

    println(list2.diff(list1))
  }

}
