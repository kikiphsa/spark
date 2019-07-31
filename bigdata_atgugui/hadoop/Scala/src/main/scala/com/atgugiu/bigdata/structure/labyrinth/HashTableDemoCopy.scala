package com.atgugiu.bigdata.structure.labyrinth

import scala.util.control.Breaks

/**
  * Create by chenqinping on 2019/5/29 13:59
  */
object HashTableDemoCopy {
  def main(args: Array[String]): Unit = {
    val hashTable = new HashTable1(5)
    //创建Emp1
    val Emp11 = new Emp1(5, "aa")
    val Emp12 = new Emp1(10, "bb")
    val Emp13 = new Emp1(15, "cc")
    val Emp14 = new Emp1(20, "dd")
    val Emp15 = new Emp1(6, "ee")
    val Emp16 = new Emp1(11, "ff")
    val Emp17 = new Emp1(16, "gg")
    val Emp18 = new Emp1(21, "hh")
    val Emp19 = new Emp1(24, "jj")
    hashTable.addEmp1(Emp11)
    hashTable.addEmp1(Emp12)
    hashTable.addEmp1(Emp13)
    hashTable.addEmp1(Emp14)
    hashTable.addEmp1(Emp15)
    hashTable.addEmp1(Emp16)
    hashTable.addEmp1(Emp17)
    hashTable.addEmp1(Emp18)
    hashTable.addEmp1(Emp19)
    hashTable.list()


    hashTable.findEmpByNo(11)
  }
}

//编写HashTable 类
class HashTable1(size: Int) {

  val Emp1LinkedLists = new Array[Emp1LinkedList1](size)

  for (i <- 0 until size) {
    Emp1LinkedLists(i) = new Emp1LinkedList1()
  }

  def findEmpByNo(no:Int): Unit ={

    val empno: Int = hash(no)
    val emp: Emp1 = Emp1LinkedLists(empno).findEmpByNo(no)

    if (emp != null){
      println()
      printf("找到了  no=%d name=%s ->",emp.no,emp.next)
    }else{
      println("没有找到该雇员=" + no)
    }

  }

  def addEmp1(Emp1: Emp1): Unit = {
    val Emp1LinkedNo: Int = hash(Emp1.no)
    Emp1LinkedLists(Emp1LinkedNo).addEmp1(Emp1)
  }

  //遍历hashtable
  def list(): Unit = {
    for (i <- 0 until size) {
      Emp1LinkedLists(i).list(i)
    }
  }

  def hash(no: Int): Int = {

    no % size
  }
}

class Emp1(Emp1NO: Int, eName: String) {

  val no = Emp1NO
  var name = eName
  var next: Emp1 = null
}

class Emp1LinkedList1 {

  var head: Emp1 = null


  def findEmpByNo(no: Int): Emp1 = {
    if (head == null) {
      println("没有找到")
      return null
    }

    var curEmp: Emp1 = head
    Breaks.breakable {
      while (true) {
        if (head.no == no) {
          Breaks.break()
        }

        if (curEmp.next == null) {
          curEmp == null
          Breaks.break()
        }

        curEmp = curEmp.next
      }
    }
    curEmp

  }

  //添加
  def addEmp1(Emp1: Emp1): Unit = {
    if (head == null) {
      head = Emp1
    } else {
      //使用辅助指针完成
      var curEmp1 = head

      Breaks.breakable {
        while (true) {
          if (curEmp1.next == null) {
            Breaks.break()
          }
          curEmp1 = curEmp1.next
        }
      }
      curEmp1.next = Emp1
    }
  }

  //  /遍历该链表
  def list(no: Int): Unit = {
    if (head == null) {
      println()
      println("di" + no + "为空")
      return
    }

    var curEmp1 = head
    println()
    print("第" + no + "链表情况->")

    Breaks.breakable {
      while (true) {
        printf("no=%d name=%s ->", curEmp1.no, curEmp1.name)
        if (curEmp1.next == null) {
          Breaks.break()
        }

        curEmp1 = curEmp1.next
      }
    }

  }
}