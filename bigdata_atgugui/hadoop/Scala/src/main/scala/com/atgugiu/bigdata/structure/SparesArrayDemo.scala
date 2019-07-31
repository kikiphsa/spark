package com.atgugiu.bigdata.structure

import scala.collection.mutable.ArrayBuffer

/**
  * Create by chenqinping on 2019/5/28 9:24
  */
object SparesArrayDemo {

  def main(args: Array[String]): Unit = {
    //先试用二维数组,映射棋盘
    val rows = 11
    val cols = 11
    var values = 0

    val chessMap1: Array[Array[Int]] = Array.ofDim[Int](rows, cols)
    //    val chessMap1: Array[Array[Array[Int]]] = Array.ofDim[Int](rows, cols, values)

    //初始化
    chessMap1(1)(2) = 1 //黑子
    chessMap1(2)(3) = 2 //篮子


    println("原始的棋盘")

    for (row <- chessMap1) {
      for (item <- row) {
        printf("%d", item)
      }
      println()
    }

    // 对原始的二维数组进行压缩
    //思路
    //1.创建一个ArrayBuffer，可以动态的添加数据
    //2.使用Node对象，表示一个数据
    val sparse = ArrayBuffer[Node]()
    //先放入第一行数据
    sparse.append(new Node(rows, cols, 0))
    //遍历chessMap1,非0的加入sparse
    for (i <- 0 until chessMap1.length) {
      for (j <- 0 until chessMap1(i).length) {
        if (chessMap1(i)(j) != 0) {
          values += 1
          sparse.append(new Node(i, j, chessMap1(i)(j)))
        }
      }
    }

    println("压缩后的数组")
    for (i <- 0 until sparse.length) {
      val node = sparse(i)
      printf("%d %d %d\n", node.row, node.col, node.value)
    }


    ///恢复成原始的磁盘
    //将稀疏数组恢复成原始的模盘
    //思路
    //1.读取稀疏数组的第一行，创建一个二维模盘chessMap2
    //2.遍历（从稀疏数组的第二行），每读取到一个Node，就将对应的值，恢复到chessMap2
    val node: Node = sparse(0)
    val chessMap2: Array[Array[Int]] = Array.ofDim[Int](node.row, node.col)

    for (i <- 1 until sparse.length) {
      val node1 = sparse(i)
      chessMap2(node1.row)(node1.col) = node1.value
    }

    println("恢复后的数组")
    for (row <- chessMap2) {
      for (item <- row) {

        printf("%d", item)
      }
      println()
    }

  }

}

case class Node(row: Int, col: Int, value: Int)