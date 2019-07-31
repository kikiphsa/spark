package com.atgugiu.bigdata.day02

/**
  * Create by chenqinping on 2019/4/30 16:33
  */
object Scala_method extends App {


  val user =new User01()

  user.login(1)


}


class User01{

  def login(s:Int): Unit ={
    println(s)
  }

  def logout(): Unit ={
    println("退出")
  }
}


object Test extends App{

  val s= Student("lisi")

  println(s)
/*
  for ( i <- Range(1,4)){

    println(i)
  }*/
}