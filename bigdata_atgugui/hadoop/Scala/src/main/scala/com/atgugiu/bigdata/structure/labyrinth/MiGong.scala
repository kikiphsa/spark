package com.atgugiu.bigdata.structure.labyrinth

/**
  * Create by chenqinping on 2019/5/30 10:04
  */
object MiGong {
  def main(args: Array[String]): Unit = {

    val map = Array.ofDim[Int](8, 7)

    for (i <- 0 until 7) {
      map(0)(i) = 1
      map(7)(i) = 1
    }

    for (i <- 0 until 8) {
      map(i)(0) = 1
      map(i)(6) = 1
    }

    map(3)(1) = 1
    map(3)(2) = 1
    map(2)(2) = 1
    for (i <- map) {
      for (j <- i) {
        printf("%d \t", j)
      }
      println()
    }


    println("找到了")

    xiaoqiu(map, 1, 1)
    for (i <- map) {
      for (j <- i) {
        printf("%d \t", j)
      }
      println()
    }


    def xiaoqiu(map: Array[Array[Int]], i: Int, j: Int): Boolean = {

      if (map(6)(5) == 2) {
        println("找到了")
        return true
      } else {
        if (map(i)(j) == 0) {

          map(i)(j) = 2
          //(下->右->上->左)
          if (xiaoqiu(map, i + 1, j)) {
            return true
          } else if (xiaoqiu(map, i, j + 1)) {
            return true
          } else if (xiaoqiu(map, i - 1, j)) {
            return true
          } else if (xiaoqiu(map, i, j - 1)) {
            return true
          } else {
            map(i)(j) = 3
            return false
          }
        } else {
          return false
        }
      }
    }

  }
}
