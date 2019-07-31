package com.atgugiu.bigdata.day02

/**
  * Create by chenqinping on 2019/5/5 8:45
  */
object Scala_Trait {

  def main(args: Array[String]): Unit = {

    val  u=new U1()
    println(u.test())
  }

}

class Person{
  println("Person")

}

trait T1{

  def test()
}

trait T2{
  println("T2")
}


class U1 extends Person with T1 with T2 {

  def test(): Unit ={
    println("U1")
  }

}
