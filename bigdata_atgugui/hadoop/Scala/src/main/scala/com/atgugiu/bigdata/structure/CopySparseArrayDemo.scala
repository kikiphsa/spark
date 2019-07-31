package com.atgugiu.bigdata.structure

import scala.collection.mutable.ArrayBuffer

/**
  * Create by chenqinping on 2019/5/28 10:07
  */
object CopySparseArrayDemo {

  def main(args: Array[String]): Unit = {

    val rows = 11
    val cols = 11

    val chessMap1: Array[Array[Int]] = Array.ofDim[Int](rows, cols)

    chessMap1(1)(2) = 1
    chessMap1(2)(3) = 2

    println("原始的棋盘")

    for (row <- chessMap1) {
      for (i <- row) {
        printf("%d", i)
      }
      println()
    }

    println("压缩后的棋盘")

    val sparseArr: ArrayBuffer[Node1] = ArrayBuffer[Node1]()

    val Node1 = new Node1(rows, cols, 0)
    sparseArr.append(Node1)
    for (i <- 0 until chessMap1.length) {
      for (j <- 0 until chessMap1(i).length) {
        if (chessMap1(i)(j) != 0) {
          val Node1 = new Node1(i, j, chessMap1(i)(j))
          sparseArr.append(Node1)
        }
      }
    }

    println("稀疏数组")

    for (elem <- sparseArr) {
      printf("%d\t%d\t%d\n",elem.row,elem.col,elem.value)
    }

  }

}

case class Node1(row: Int, col: Int, value: Int)
