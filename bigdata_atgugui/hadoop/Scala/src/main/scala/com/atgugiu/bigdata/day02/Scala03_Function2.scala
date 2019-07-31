package com.atgugiu.bigdata.day02

object Scala03_Function2 {

  def main(args: Array[String]): Unit = {

    // TODO scala可以采用自动推断功能来简化函数的声明

    // 1) 如果函数声明时，明确无返回值Unit，那么即使函数体中有return也不起作用
    /*
      def test(): Unit = {
          //println("Hello Scala")
          return "zhangsan"
      }
      */

    // 如果将函数体的最后一行代码进行返回，那么return关键字可以省略
    /*
      def test() : String = {
          //return "zhangsan"
          "zhangsan"
          //println("xxxx")
      }
      */

    // 如果可以根据函数的最后一行代码推断类型，那么函数返回值类型可以省略
    /*
      def test() = {
          "zhangsan"
      }
      */

    // 如果函数体中只有一行代码，大括号可以省略
    //def test() = "zhangsan"

    // 如果函数声明中没有参数列表，小括号可以省略
    // 如果函数小括号省略，那么访问函数时不能增加小括号
    // TODO 声明函数必须要增加def
    def test = "lisi"

    val test1 = "lisi"

    /*
      def test3()={
          "lsls"
      }

      println(test3())


      ()->{println("lll")}*/

    //        println(test)

    def test3(name2: String = "lisi", nam1: String): Unit = {

    }

    /*   def f4(f:String): Int ={
     f()+10
    }*/

    /*def f6(f:()=>Unit): Unit ={
      f()
    }

    f6(()=>{println("0000000000")})*/

    def f1(): Unit = {
      println("function")
    }

    def f2() = {
      println("nimiede ")
      f1 _
    }

    f2()()

    // 函数在scala可以做任何的事情
    // 函数可以赋值给变量
    // 函数可以作为函数的参数
    // 函数可以作为函数的返回值

    def f5(i: Int) = {

      def f55(j: Int): Int = {
        i * j
      }

      f55 _
    }

    println("*555555555  " + f5(5)(2))


    // TODO 闭包
    // 一个函数在实现逻辑时，将外部的变量引入到函数的内容，改变了这个变量的生命周期，称之为闭包

    def f7(): Unit = {
      println("你没偶的")
    }

    def f8(): Unit = {

      f7()
    }

    f8()
    // 将函数作为参数传递给另外一个函数，需要采用特殊的声明方式
    // ()=>Unit
    // 参数列表=>返回值类型

    def f9(f: () => Int): Int = {
      f() + 10
    }

    def f10(): Int = {
      3
    }

    println(f9(f10))


    def f11(f: () => Unit): Unit = {
      f()
    }

    f11(() => println("1111"))
    // 如果希望函数中的某个一个参数使用默认值，那么可以在声明时直接赋初始值

    def d1(name2: String = "lisi", name1: String): Unit = {
      println(s"${name1} - ${name2}")
    }

    //      d1("zhangsan","")

    d1(name1 = "ss")


    def s1(s: (Int) => Unit): Unit = {
      s(10)
    }


/*
    def s2(i: Int): Unit = {
      println(i)
    }

    s1(s2)
*/
    s1((i:Int)=>{println(i)})
    s1(println(_))
    s1(println)


  /*  def s3(s:(Int,Int)=>Int): Int ={
      s(12,12)
    }

     s3((x:Int,y:Int)=>{x+y})

    println(s3((x,y)=>{x+y}))

    println(s3((x,y)=>x+y))

    println(s3(_+_))*/


    def s3(s:(Int,Int)=>Int): Int = {
      s(1,12)
    }

    println(s3((x,y)=>{x+y}))

    println(s3((x,y)=>x+y))

    println(s3(_+_))
  }
}
