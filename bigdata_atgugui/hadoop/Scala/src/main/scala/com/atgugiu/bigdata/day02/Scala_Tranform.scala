package com.atgugiu.bigdata.day02

/**
  * Create by chenqinping on 2019/5/5 19:00
  */
object Scala_Tranform {


  def main(args: Array[String]): Unit = {

    implicit def tran(u2:U2): P1 ={

      new P1
    }

    val u = new U2

    u.delete()
    u.insert()
  }

}


class P1{
  def insert(): Unit ={

  }
}

class U2(){
  def delete(): Unit ={

  }
}