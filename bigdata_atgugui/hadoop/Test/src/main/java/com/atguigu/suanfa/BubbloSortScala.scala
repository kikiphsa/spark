package com.atguigu.suanfa

import scala.util.control.Breaks

/**
  * Create by chenqinping on 2019/6/19 16:02
  */
object BubbloSortScala {

  def main(args: Array[String]): Unit = {

    var data = Array(9, -16, 21, 23, -30, -49, 21, 30, 30)

    val length: Int = data.length

    for (i <- length - 1 to 0) {
      var flag = false
      for (j <- length - 1 - i to 0) {
        if (data(j) > data(j + 1)) {
          var temp = data(j + 1)
          data(j + 1) = data(j)
          data(j) = temp
          flag = true
        }
      }
    }

    println(java.util.Arrays.toString(data))

  }

}
