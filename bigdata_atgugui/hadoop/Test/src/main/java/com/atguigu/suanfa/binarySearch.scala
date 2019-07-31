package com.atguigu.suanfa

/**
  * Create by chenqinping on 2019/6/24 18:55
  */
object binarySearch {


  def binarySearch(arr: Array[Int], left: Int, right: Int, findVal: Int): Int = {
    if (left > right) {
      //递归退出条件，找不到，返回-1
      -1
    }

    val midIndex = (left + right) / 2

    if (findVal < arr(midIndex)) {
      //向左递归查找
      binarySearch(arr, left, midIndex, findVal)
    } else if (findVal > arr(midIndex)) {
      //向右递归查找
      binarySearch(arr, midIndex, right, findVal)
    } else {
      //查找到，返回下标
      midIndex
    }
  }


  //二分查找
  def binary(arr: Array[Int], left: Int, right: Int, findValue: Int): Int = {
    if(left>right){
      -1
    }

    val midIndex=(left+right)/2

    if(findValue < arr(midIndex)){
      binary(arr,left,midIndex,findValue)
    }else if(findValue>arr(midIndex)){
      binary(arr,midIndex,right,findValue)
    }else{
      midIndex
    }
  }


}
