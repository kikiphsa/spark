package com.atgugiu.bigdata.day05

import scala.collection.mutable.ArrayBuffer

/**
  * Create by chenqinping on 2019/5/5 19:35
  */
object Scala01_List {

  def main(args: Array[String]): Unit = {

    val ints: Array[Int] = Array(1,2,3,4)

//    ints.foreach((i:Int)=>{print(i)})
//    ints.foreach(println)

   /* println(ints.length)
    println(ints(2))
    println(ints.mkString(","))

    val array = ints.+("sss")
    println(array)

    ints.foreach({println(_)})
    ints.foreach(println)*/

    val ints1: Array[Int] = ints :+ 5

    println(ints1.mkString(","))

    val ints2: Array[Int] = 1+:ints

    println(ints2.mkString("|"))

     ints.update(1,5)
    println(ints.mkString(","))

    println(ints==ints2)


    //可变数组ArrayBUuffer

    val arrayBuffer: ArrayBuffer[Int] = ArrayBuffer(5,6,7,8)

    println(arrayBuffer(1))
    arrayBuffer.insert(1,3)
//    println(arrayBuffer.mkString(","))

    val buffer: ArrayBuffer[Int] = arrayBuffer.+=(9)

    println(buffer.mkString(","))

//    val value = arrayBuffer.++(9)
//
//    println(value)


  }
}
