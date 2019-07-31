package com.atgugiu.bigdata.day01

import scala.io.StdIn
import scala.util.control.Breaks

/**
  * Create by chenqinping on 2019/4/27 14:04
  */
object Scala_01 {

  def main(args: Array[String]): Unit = {
    /*var i = 0

    println("name")
    val name = StdIn.readLine()

    println("age")
    val age = StdIn.readInt()*/

    val ints = for (i <- 1 to 10) yield i * 2
    println(ints)

    Breaks.breakable {


      for (i <- 1 to 8; j <- 1 to 3) {
        if (i == 5) {
          Breaks.break()
        }
        println("i " + i, "j " + j)
      }
    }
    println("开出来吧")
  }
}
