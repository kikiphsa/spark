package com.atgugiu.bigdata.structure.LinkSing

import scala.util.control.Breaks

/**
  * Create by chenqinping on 2019/5/28 14:13
  */
object SingleLinkedListDemo {

  def main(args: Array[String]): Unit = {
    val singleLinkedList = new SingleLinkedList()

    val node1 = new HeroNode(1, "宋江", "及时雨")
    val node2 = new HeroNode(2, "卢俊义", "玉麒麟")
    val node3 = new HeroNode(3, "吴用", "智多星")
    val node4 = new HeroNode(4, "张飞", "翼德")

    //        singleLinkedList.add(node4)
    //        singleLinkedList.add(node1)
    //        singleLinkedList.add(node3)
    //        singleLinkedList.add(node2)

    println("链表的情况")
    singleLinkedList.list()


    /*  val node5 = new HeroNode(4, "你妹的", "哈哈")
      singleLinkedList.update(node5)
      printf("修改后的\n\n")
      singleLinkedList.list()*/


    /* singleLinkedList.del(1)
     singleLinkedList.del(2)
     singleLinkedList.del(3)
     singleLinkedList.del(4)
     println("删除")
     singleLinkedList.list()*/

    singleLinkedList.addByOrder(node4)
    singleLinkedList.addByOrder(node1)
    singleLinkedList.addByOrder(node3)
    singleLinkedList.addByOrder(node2)

    singleLinkedList.list()

    println("倒叙")

    singleLinkedList.reversePrint()

  }

}

class SingleLinkedList {
  var head = new HeroNode(-1, "", "")

  //使用栈,打印单链表,不破坏链表本身的结构
  //1.遍历单向链表，将结点，push到stack中
  //2.遍历Stack，取出每个结点，新出信息
  //注意：在操作中，没有创建新的对象，只是使用引用
  def reversePrint(): Unit = {
    val stack = new java.util.Stack[HeroNode]()
    var temp: HeroNode = head.next

    Breaks.breakable {
      while (true) {
        stack.push(temp)
        if (temp.next == null) {
          Breaks.break()
        }
        temp = temp.next
      }
    }

    println()
    while (!stack.empty()) {
      val heroNode: HeroNode = stack.pop()
      printf("no=%d name=%s -->", heroNode.no, heroNode.name)
    }
  }




  def del(no: Int): Unit = {
    if (isEmpty()) {
      println("数据为空,删除不了")
      return
    }

    var temp: HeroNode = head

    var flag = false

    Breaks.breakable {
      while (true) {
        if (temp.next.no == no) {
          flag = true
          Breaks.break()
        }

        if (temp.next.next == null) {
          Breaks.break()
        }
        //让temp后移，实现遍历
        temp = temp.next
      }
    }

    if (flag) {
      temp.next = temp.next.next
    } else {
      printf("你要删除的结点%d 不存在", no)
    }
  }

  def update(heroNode: HeroNode): Unit = {
    if (isEmpty()) {
      printf("数据不存在,修改不了")
      return
    }

    var temp: HeroNode = head.next


    var flag = false
    Breaks.breakable {
      while (true) {

        if (temp.no == heroNode.no) {
          flag = true
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

  def add(heroNode: HeroNode): Unit = {

    var temp = head

    Breaks.breakable {
      while (true) {
        if (temp.next == null) {
          Breaks.break()
        }
        temp = temp.next
      }
    }

    temp.next = heroNode
  }


  def addByOrder(heroNode: HeroNode): Unit = {

    var temp: HeroNode = head

    var flag = false
    Breaks.breakable {
      while (true) {
        if (temp.next == null) {
          Breaks.break()
        }

        if (temp.next.no == heroNode.no) {
          flag = true
          Breaks.break()
        } else if (temp.next.no > heroNode.no) {
          Breaks.break()
        }

        temp = temp.next
      }

    }

    if (flag) {
      printf("已经存在", heroNode.no)
    } else {
      heroNode.next = temp.next
      temp.next = heroNode
    }
  }


  def list(): Unit = {
    if (isEmpty) {
      printf("数据为空")
      return
    }

    var temp: HeroNode = head.next

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

class HeroNode(hNo: Int, hName: String, hNickname: String) {

  val no = hNo
  var name = hName
  var nickNmae = hNickname
  var next: HeroNode = null
}