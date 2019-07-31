package com.atgugiu.bigdata.day02

/**
  * Create by chenqinping on 2019/5/5 8:41
  */
object Scala_Class extends App {

  private val user = new  User("1")
  println(user.name)
}


class User(var name:String){

}
