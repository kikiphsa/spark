package com.atgugiu.bigdata.day02

/**
  * Create by chenqinping on 2019/5/5 19:05
  */
object Scala03_Transform2 {

  def main(args: Array[String]): Unit = {

    implicit   val name:String="wangwu"
    implicit  val age:Int=10

    def test(implicit name:String="zhangsan"): Unit ={
      println(name)
    }

    test

    def test1( implicit name : String ): Unit = {
//      def test(implicit name : String = "zhangsan" ): Unit = {
      println("Hello " + name)
//    }
    }

    test1
  }

}
