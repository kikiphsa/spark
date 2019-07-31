package com.atgugiu.bigdata.structure

import scala.io.StdIn

/**
  * Create by chenqinping on 2019/5/28 10:28
  */
object ArrayQueueDemo {

  def main(args: Array[String]): Unit = {

    val queue = new ArrayQueue(3)

    var key = ""

    while (true) {
      println("show: 表示显示队列")
      println("exit: 表示退出程序")
      println("add: 表示添加队列数据")
      println("get: 表示取出队列数据")
      println("peek: 查看队列头的数据(不改变队列)")
      key = StdIn.readLine()
      key match {
        case "show" => queue.show()
        case "add" => {
          println("请输入一个数")

          val str: Int = StdIn.readInt()
          queue.addQueue(str)
        }

        case "get" => {
          val res: Any = queue.getQueue()
          //异常
          if (res.isInstanceOf[Exception]) {
            println(res.asInstanceOf[Exception].getMessage)
          } else {
            println(s"取出数据是 $res")
          }
        }

        case "peek" => {
          val res: Any = queue.peek()

          if (res.isInstanceOf[Exception]) {
            println(res.asInstanceOf[Exception].getMessage)
          } else {
            println(s"查看的数据是 $res")
          }
        }
      }
    }
  }
}

class ArrayQueue(arrMaxSize: Int) {

  var maxSize = arrMaxSize
  var arr = new Array[Int](maxSize)

  var front = -1 //头前一个元素
  var rear = -1 //尾部

  //判读队列满
  def isFull(): Boolean = {
    rear == maxSize - 1
  }

  //空
  def isEmpty(): Boolean = {
    rear == front
  }

  //查看头元素 不取出
  def peek(): Any = {

    if (isEmpty()) {
      return new Exception("队列空,无数据")
    }
    //这里注意，不要去改变fornt 值

    arr(front + 1)

  }

  //增
  def addQueue(num: Int): Unit = {
    if (isFull()) {
      return
    }

    //将rear后移
    rear += 1
    arr(rear) = num

  }

  //查
  def show(): Unit = {
    if (isEmpty()) {
      printf("队列空")
    }
    //遍历
    printf("队列数据情况是: ")
    for (i <- front + 1 to rear) {
      printf("arr(%d)=%d \t", i, arr(i))
    }
  }

  //取数据
  def getQueue(): Any = {
    if (isEmpty()) {
      return new Exception("队列空")
    }
    //front 后移
    front += 1
    return arr(front)
  }


  //改

  //删


}