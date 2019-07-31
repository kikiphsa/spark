package com.atguigu.suanfa

/**
  * Create by chenqinping on 2019/6/24 9:57
  */


class merger {

  /**
    * 归并
    * 时间复杂度:O(nlogn)
    * 空间复杂度:O(n)
    */
  def merge(left: List[Int], right: List[Int]): List[Int] = (left, right) match {
    case (Nil, _) => right
    case (_, Nil) => left
    case (x :: xTail, y :: yTail) =>
      if (x <= y) x :: merge(xTail, right)
      else y :: merge(left, yTail)
  }


  def m(left: List[Int], right: List[Int]): List[Int] = (left, right) match {
    case (Nil, _) => right
    case (_, Nil) => left

    case (x :: xTail, y :: yTail) => {
      if (x <= y) x :: m(xTail, yTail)
      else y :: m(left, right)
    }
  }
}
