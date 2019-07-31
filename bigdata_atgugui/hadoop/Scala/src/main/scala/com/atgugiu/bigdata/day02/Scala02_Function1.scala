package com.atgugiu.bigdata.day02

object Scala02_Function1 {


    def main(args: Array[String]): Unit = {

        // 函数：入参（方法参数），出参(返回值)

        // 无参， 无返回值
        def test(): Unit = {
            println("Test")
        }

        //test()

        // 有参，无返回值
        // 函数没有重载的概念
        // 如果在同一个作用域中，函数不能同名
        def test1( s : String ): Unit = {
            println(s)
        }

        //test1("zhangsan")

        // 有参，有返回值
        def test2(s : String): String = {
            return s + "....."
        }

        //val rtnVal: String = test2("zhangsan")
        //println(rtnVal)

        // 无参，有返回值
        def test3(): String = {
            return "Hello World"
        }

        println(test3())

        // scala中没有throws关键字，所以函数中如果有异常发送，也不需要在函数声明时抛出异常


    }
}
