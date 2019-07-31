package com.atgugiu.bigdata.day02

import com.atgugiu.bigdata.day02.Scala04_Transform3.U4

/**
  * Create by chenqinping on 2019/5/5 19:27
  */
object Scala04_Transform3 {

  def main(args: Array[String]): Unit = {

    val user4 = new U4()

    user4.insert()
    user4.delete()
  }

  class U4 extends Test{
    def insert(){

    }
  }
}

trait Test {

}
object Test {
  implicit class Person4(u:U4) {
    def delete() {

    }
  }
}