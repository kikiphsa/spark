package com.atgugiu.bigdata.structure.LinkSing

import scala.util.control.Breaks

/**
  * Create by chenqinping on 2019/5/28 16:08
  */
object DoubleLikedListDemo {
  def main(args: Array[String]): Unit = {

    val doubleLikedList = new DoubleLikedList()
    val node1 = new HeroNode2(1, "宋江", "及时雨")
    val node2 = new HeroNode2(2, "卢俊义", "玉麒麟")
    val node3 = new HeroNode2(3, "吴用", "智多星")
    val node4 = new HeroNode2(4, "张飞", "翼德")

    doubleLikedList.add(node4)
    doubleLikedList.add(node1)
    doubleLikedList.add(node3)
    doubleLikedList.add(node2)

    doubleLikedList.list()

    /*   val node5 = new HeroNode(4, "公孙胜", "入云龙")
       doubleLikedList.update(node5)
       println()
       doubleLikedList.list()*/


    doubleLikedList.del(1)
    doubleLikedList.del(2)
    doubleLikedList.del(3)
    doubleLikedList.del(4)
    println()
    doubleLikedList.list()
  }

}

class DoubleLikedList() {
  val head = new HeroNode2(-1, "", "")


  def del(no: Int): Unit = {
    if (isEmpty()) {
      println("双向链表为空")
      return
    }
    var temp: HeroNode2 = head.next

    var flag = false

    Breaks.breakable {
      while (true) {
        if (temp.no == no) {
          flag = true
          Breaks.break()
        }

        if (temp.next == null) {
          Breaks.break()
        }

        temp = temp.next
      }
    }


    if (flag) {
      temp.pre.next = temp.next
      if (temp.next != null) {

        temp.next.pre = temp.pre
      }
    } else {
      println("没有找到" + no)
    }

  }


  def add(heroNode2: HeroNode2): Unit = {

    var temp: HeroNode2 = head


    Breaks.breakable {
      while (true) {
        if (temp.next == null) {
          Breaks.break()
        }
        temp = temp.next
      }
    }

    temp.next = heroNode2
    heroNode2.pre = temp //必须有
  }

  def update(heroNode: HeroNode): Unit = {

    if (isEmpty()) {
      println("双向链表为空")
      return
    }

    var temp: HeroNode2 = head.next

    var flag = false
    Breaks.breakable {
      while (true) {

        if (temp.no == heroNode.no) {
          flag = true
          Breaks.break()
        }

        if (temp.next == null) {
          Breaks.break()
        }

        temp = temp.next
      }
    }

    if (flag) {
      temp.nickNmae = heroNode.nickNmae
      temp.name = heroNode.name
    } else {
      printf("你要修改的%d英雄不存在.", heroNode.no)
    }
  }


  def list(): Unit = {

    if (isEmpty()) {
      println("双向链表为空")
      return
    }

    var temp: HeroNode2 = head.next
    Breaks.breakable {
      while (true) {
        printf("no=%d name=%s nickNmae=%s -->", temp.no, temp.name, temp.nickNmae)

        if (temp.next == null) {
          Breaks.break()
        }

        temp = temp.next
      }
    }

  }

  def isEmpty(): Boolean = {
    head.next == null
  }
}

class HeroNode2(hNo: Int, hName: String, hNickname: String) {

  val no = hNo
  var name = hName
  var nickNmae = hNickname
  var next: HeroNode2 = null
  var pre: HeroNode2 = null
}