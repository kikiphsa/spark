package com.atgugiu.bigdata.structure.LinkSing

/**
  * Create by chenqinping on 2019/5/29 11:18
  */
object test {
  def main(args: Array[String]): Unit = {

    test(4)
  }

  def test(n: Int): Unit = {
    if (n > 3) {
      test(n - 1)
    }
      println("n=" + n)
    }
  }
