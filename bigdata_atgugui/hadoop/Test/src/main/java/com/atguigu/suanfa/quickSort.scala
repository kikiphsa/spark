package com.atguigu.suanfa

import java.util

/**
  * Create by chenqinping on 2019/6/24 9:16
  */
class quickSort {

  /**
    * 快排
    * 时间复杂度:平均时间复杂度为O(nlogn)
    * 空间复杂度:O(logn)，因为递归栈空间的使用问题
    */
  def quickSort(list: List[Int]): List[Int] = list match {
    //Nil是一个空的List，定义为List[Nothing]，根据List的定义List[+A]，所有Nil是所有List[T]的子类。
    case Nil => Nil
    case List() => List()
    case head :: tail =>
      val (left, right) = tail.partition(_ < head)
      quickSort(left) ::: head :: quickSort(right)
  }

  def quick(list: List[Int]): List[Int] = list match {
    case Nil => Nil
    case List() => List()
    case head :: tail => {
      val (left, right): (List[Int], List[Int]) = tail.partition(_ < head)

      quick(left) ::: head :: quick(right)
    }
  }





}
