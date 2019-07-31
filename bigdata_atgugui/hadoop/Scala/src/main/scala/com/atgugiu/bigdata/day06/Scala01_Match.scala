package com.atgugiu.bigdata.day06

/**
  * Create by chenqinping on 2019/5/6 19:55
  */
object Scala01_Match {

  def main(args: Array[String]): Unit = {
    /*   val oper = '#'
    val n1 = 20
    val n2 = 10
    var res = 0*/

    /*   oper match {

      case '+' => res = n1 + n2
      case '-' => n1 - n2
      case '*' => n1 * n2
//      case '/' => n1 / n2
      case _ => println("hahah")
    }

    println(res)*/

    /*   val oper = '+'
    val n1 = 20
    val n2 = 10
    var res = 0
    oper match {
      case '+' => res = n1 + n2
      case '-' => res = n1 - n2
      case '*' => res = n1 * n2
      case '/' => {
        res = n1 / n2
      }
      //case _ => println("oper error")
    }
    println("res=" + res)*/
    /*

        for (ch <- "+-3!") {
          var sign = 0
          var digit = 0
          ch match {
            case '+' => sign = 1
            case '-' => sign = -1
            case _ if ch.toString.equals("3") => digit = 3

            case _ => sign = 6
          }
          println(ch + " " + sign + " " + digit)

        }
    */


    val a = 10
    val obj = if (a == 1) 5
    else if (a == 2) "2"
    else if (a == 3) BigInt(3)
    else if (a == 4) Map("aa" -> 1)
    else if (a == 5) Map(1 -> "aa")
    else if (a == 6) Array(1, 2, 3) // Array[Int]
    else if (a == 7) Array("aa", 1) // Array[Any]
    else if (a == 8) Array("aa") // Array[String]
    else if (a == 9) List("aa") // Array[String]
    else if (a == 10) List(1, 2) // Array[String]


    val result = obj match {
      case a: Int => a
      case b: Map[String, Int] => "对象是一个字符串-数字的Map集合"
      case c: Map[Int, String] => "对象是一个数字-字符串的Map集合"
      case d: Array[String] => "对象是一个字符串数组"
      case e: Array[Int] => "对象是一个数字数组"
      case f: BigInt => Int.MaxValue
      case g: List[String] => "xxxxxx"
      case h: List[Int] => "yyyyy"
      case _ => "啥也不是"
    }
    println(result)

    for (arr <- Array(Array(0), Array(1, 0), Array(0, 1, 0), Array(1, 1, 0), Array(1, 1, 0, 1))) {

      val result = arr match {
        case Array(0) => "0"
        case Array(x, y) => x + "=" + y
        case Array(0, _*) => "以0开头和数组"
        case _ => "什么集合都不是"
      }
      println("result = " + result)
    }
  }
}