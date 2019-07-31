package com.atgugiu.bigdata.day02

/**
  * Create by chenqinping on 2019/4/30 10:59
  */
object Scala_value extends App{


    val bb = new BB()

    bb.f("s")

    println(bb.name)


}

class AA {

  val email: String = "xxxx"

  val name: String = ""

  def f(): Unit = {
    println("pppp")
  }
}

class BB extends AA {

  override val name: String = "ss"

  def f(s: String): Unit = {
    println(s)
  }

}