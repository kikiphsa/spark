package com.atgugiu.bigdata.day06

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
  * Create by chenqinping on 2019/5/6 17:43
  */
object Scala10_WordCountFile {

  def main(args: Array[String]): Unit = {

    /*val list: List[String] = Source.fromFile("C:\\D\\JAVA\\spark\\workbase\\spark\\hadoop\\Scala\\in\\word.txt").getLines().toList

    list.foreach(println)*/

//    val list = List(1, 2, 3, 4)

    /*  val i: Int = list.reduceRight(_-_)
      println(i)*/

    /*val i: Int = list.fold(100)(_-_)
    val i1: Int = list.foldLeft(100)(_-_)
    val i2: Int = list.foldLeft(100)(_+_)

    val ints: List[Int] = list.scanLeft(10)(_-_)

    println(ints)
    println(i)
    println(i1)
    println(i2)


    val ints1: List[Int] = list.scanRight(10)(_-_)
    val ints2: List[Int] = list.scanRight(10)(_+_)
    println(ints1)
    println(ints2)*/
    /*
        val map1 = mutable.Map( "a"->1, "b"->2, "c"->3 )
        val map2 = mutable.Map( "a"->3, "c"->2, "d"->1 )

        val stringToInt: mutable.Map[String, Int] = map1.foldLeft(map2)((map, t) => {
          map(t._1) = map.getOrElse(t._1, 0) + t._2
          map
        })
        println(stringToInt)


        val arr = ArrayBuffer("1", "2", "3")

        import scala.collection.JavaConversions.bufferAsJavaList
        val javaArr = new ProcessBuilder(arr)
        val arrList = javaArr.command()

        println(arrList)

        import scala.collection.JavaConversions.asScalaBuffer
        import scala.collection.mutable

        val scalaArr: mutable.Buffer[String] = arrList

        scalaArr.append("jack")
        println(scalaArr)

        val tuple: (String, String, String) = ("1", "2", "3")

        for (emle <- tuple.productIterator){
          println(emle)
        }


        val ints: ArrayBuffer[Int] = ArrayBuffer[Int](1,2,3)*/

    val list1 = List(1, List(2, 3), List(4, 5), 6, List(7, 8))

    val l: List[Any] = list1.flatMap(x => {

      if (x.isInstanceOf[List[Int]]) {
        x.asInstanceOf[List[Int]]
      } else {
        List(x)
      }
    })
    println(l)


    // 并行
    val result1 = (0 to 100).map { case _ => Thread.currentThread.getName }
    val result2 = (0 to 100).par.map { case _ => Thread.currentThread.getName }
    println(result1)
    println(result2)




    val list2 = List(1, 2, 3, "abc")
    val list5 = 4 :: 5 :: 6 :: list2 :: Nil // 从右向左的逻辑
     println(list5)
    val list7 = 4 :: 5 :: 6 :: list2 ::: Nil
    println(list7)


    /*    import scala.collection.immutable

        val ints = new mutable.Queue[Int]()

        println(ints)
        ints += 1
        ints ++= List(1, 2, 3, 4)
        println(ints)


        val map4 = mutable.Map(("A", 1), ("B", 2), ("C", 3), ("D", 30.9))

        if (map4.contains("A")) {
          println("存在")
        } else {
          println("快出来")
        }

        println(map4.get("g"))
        println(map4.get("g"))

        println(map4.getOrElse("CC", "0"))

        map4("A") = 12

        map4 += ("D" -> 3,"F"->1)
        println(map4)

        map4 -=("A","FF")
        println(map4)*/


    /*    val map1 = mutable.Map( ("A", 1), ("B", "北京"), ("C", 3) )
        for ((k, v) <- map1) println(k + " is mapped to " + v)
        for (v <- map1.keys) println(v)
        for (v <- map1.values) println(v)
        for(v <- map1) println(v)*/


 /*   import scala.collection.mutable.Set
    val mutableSet = Set(1, 2, 3) //可变

    mutableSet +=(1)
    mutableSet.add(54)
    println(mutableSet)

    mutableSet -=1
    mutableSet.remove(3)
    println(mutableSet)*/

    /*val list1 = List(1, 20, 30, 4, 5)
    def sum(n1: Int, n2: Int): Int = {
      n1 + n2
    }

    val res = list1.reduceLeft(sum)
    println("res=" + res)


    def sum1(x:Int,y:Int): Int ={
      x+y
    }
    println(list1.reduceLeft(_ + _))


    val list = List(1, 2, 3, 4 ,5)
    def minus( num1 : Int, num2 : Int ): Int = {
      num1 - num2
    }
    println(list.reduceLeft(minus)) //
    println(list.reduceRight(minus))
    println(list.reduce(minus))*/

/*    val list = List(1, 2, 3, 4)

    val i: Int = list.foldLeft(10)(_+_)
    println(i)

    val i1: Int = list.foldRight(10)(_-_)

    println(i1)
    val i2: Int =(10/:list)(_-_)
    val i3: Int =(list:\10)(_-_)
    println(i2)
    println(i3)

    val ints: List[Int] = list.scanLeft(10)(_-_)
    println(ints)

    val ints1: List[Int] = list.scanRight(10)(_-_)
    println(ints1)*/


   /* val iterator1 = List(1, 2, 3, 4, 5).iterator // 得到迭代器

    while (iterator1.hasNext){
      println(iterator1.next())
    }*/

  }

}
