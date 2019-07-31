package com.atgugiu.bigdata.structure.LinkSing

import scala.util.control.Breaks

/**
  * Create by chenqinping on 2019/5/29 8:48
  */
object JosephuDemo {

  def main(args: Array[String]): Unit = {

    val josephu = new Josephu()
    josephu.addBoy(5)
    josephu.list()
    josephu.countBoy2(2, 2, 5)
  }

}

class Josephu {
  //第一个
  var first: Boy = null

  def countBoy(startNO: Int, countNum: Int, boyNums: Int): Unit = {
    if (first == null || startNO > boyNums || startNO <= 0) {
      printf("输入有错")
      return
    }
    var helper = first

    Breaks.breakable {
      while (true) {
        if (helper.next == first) {
          Breaks.break()
        }
        helper = helper.next
      }
    }

    for (i <- 0 until startNO - 1) {
      first = first.next
      helper = helper.next
    }

    Breaks.breakable {
      while (true) {

        for (i <- 0 until countNum - 1) {
          first = first.next
          helper = helper.next
        }

        //删除
        printf("小孩小孩no=%d出圈了 \n", first.no)
        first = first.next
        helper.next = first

        //是否最后一个小孩
        if (first == helper) {
          printf("最后一个小孩小孩no=%d出圈了 \n", first.no)
          Breaks.break()
        }
      }
    }

    printf("最后的小孩是哈哈哈哈小孩no=%d出圈了 \n", first.no)
  }



  def countBoy2(startNO: Int, countNum: Int, boyNums: Int): Unit = {
    if (first == null || startNO > boyNums || startNO <= 0) {
      printf("输入有错")
      return
    }
    var helper = first

    Breaks.breakable {
      while (true) {
        if (helper.next == first) {
          Breaks.break()
        }
        helper = helper.next
      }
    }

    for (i <- 0 until startNO - 1) {
      helper = helper.next
    }

    Breaks.breakable {
      while (true) {

        for (i <- 0 until countNum - 1) {
          helper = helper.next
        }

        //删除
        printf("小孩no=%d出圈了 \n", helper.next.no)
        helper.next=helper.next.next
        //是否最后一个小孩
        if (helper == helper.next) {
          Breaks.break()
        }
      }
    }

    println("最后的小孩是哈哈哈哈", helper.no)
  }


  def addBoy(boyNum: Int): Unit = {

    var curBoy: Boy = null
    for (i <- 1 to boyNum) {

      val boy = new Boy(i)
      //处理第一个小孩,形成环状
      if (i == 1) {
        first = boy
        curBoy = boy
        first.next = first
      } else {
        curBoy.next = boy
        boy.next = first
        curBoy = boy
      }
    }
  }

  //遍历环形列表
  def list(): Unit = {

    if (first == null) {
      println("没有小孩")
      return
    }
    var curBoy = first
    Breaks.breakable {
      while (true) {
        printf("no=%d \n", curBoy.no)
        if (curBoy.next == first) {
          //表示是最后的了
          Breaks.break()
        } else {
          //后移
          curBoy = curBoy.next
        }
      }

    }

  }
}

class Boy(bNo: Int) {
  val no = bNo
  var next: Boy = null
}