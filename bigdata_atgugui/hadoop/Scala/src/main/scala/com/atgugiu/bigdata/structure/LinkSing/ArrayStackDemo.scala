package com.atgugiu.bigdata.structure.LinkSing

/**
  * Create by chenqinping on 2019/5/29 10:13
  */
object ArrayStackDemo {

  def main(args: Array[String]): Unit = {

    val stack = new ArrayStack(4)

    var key = ""


  }

}

class ArrayStack(arrMaxSize: Int) {

  var arr = new Array[Int](arrMaxSize)

  var top = -1

  def isFull(): Boolean = {
    top == arrMaxSize - 1
  }

  def isEmpty(): Boolean = {
    top == -1
  }

  def push(num: Int): Unit = {
    if (isFull()) {
      println("栈满,不能加入")
      return
    }
    top += 1
    arr(top) = num
  }

  def pop(): Any = {
    if (isEmpty()) {
      return new Exception("栈空")
    }

    val res = arr(top)
    top -= 1
    return res
  }


  def list(): Unit = {
    if (isEmpty) {
      println("栈空,不能遍历")
      return
    }


  }

}