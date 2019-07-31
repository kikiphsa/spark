package com.atgugiu.bigdata.day02

/**
  * Create by chenqinping on 2019/5/5 18:51
  */
object Scala_TransForm extends App {




  implicit def transform(d:Double): Int ={
    d.toInt
  }

  /*  implicit  def transform1(d:Double):Int={
      d.toInt
    }*/

  val i:Int=5.0

  println(i)


//   val user: User = new User()



}

class Person{
  def insert(): Unit ={

  }
}

class User{
  def delete: Unit ={

  }
}