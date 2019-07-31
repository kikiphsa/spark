package com.atgugiu.bigdata.day05

import scala.collection.mutable.ListBuffer

/**
  * Create by chenqinping on 2019/5/5 20:59
  */
object Scala03_Seq {

  def main(args: Array[String]): Unit = {

    // 序列 Seq
    // 默认scala提供的集合都是不可变的（immutable）

    val ints: List[Int] = List(1, 2, 3, 4)

    val ints1: List[Int] = List(5, 6, 7, 8)

    val list1 = ints :+ 1

    println(list1.mkString(","))

    val list2 = 1 +: ints
    println(list2.mkString(","))

    val list3 = ints.drop(2)
    println(list3)

    ints.head
    println(ints.head)

    println(ints.tails)
    println(ints.last)

    val list4 = ints ++ ints1

    println(list4.mkString("<"))

    val list5: List[Int] = list1.::(9)

    println(list5.mkString(","))

    val list6 = 5 :: 9 :: list1

    println("11111111111" + list1.mkString(","))
    println("6666666" + list6.mkString(","))

    val list7: List[Int] = list1.updated(1, 3)

    println(list7.mkString(","))

    val list8: List[Int] = ints.union(ints1)
    println(list8.mkString(","))


    //可变集合
    val b1: ListBuffer[Int] = ListBuffer(1, 2, 3, 4)

    /*   b1.insert(1,2)

       println(b1.mkString(","))*/

    /*
        b1.remove(2,1)
        println(b1.mkString(","))*/

    println(b1.tail)

    import scala.collection.mutable
    val q = mutable.Queue(1, 2, 3, 4)

    println(q)

    println(q.dequeue())
    println(q.dequeue())
    println(q.dequeue())


  }

}
