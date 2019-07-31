package com.atgugiu.bigdata.day06

/**
  * Create by chenqinping on 2019/5/6 11:11
  */
object Scala_flod {

  def main(args: Array[String]): Unit = {

    val list = List(1, 2, 3, 4)

    /*val i = list.reduce(_+_)
    println(i)

    val i1 = list.reduceLeft(_+_)

    println(i1)

    val i3 = list.reduceLeft(_-_)

    println(i3)

    val i4 = list.reduceRight(_-_)

    println(i4)*/

    //    list.fold()()

    import scala.collection.mutable

    val map1: mutable.Map[String, Int] = mutable.Map("a" -> 1, "b" -> 2, "c" -> 3)
    val map2: mutable.Map[String, Int] = mutable.Map("a" -> 2, "c" -> 2, "d" -> 4)

    val stringToInt = map1.foldLeft(map2)((map, t) => {

      map(t._1) = map.getOrElse(t._1, 0) + t._2
//      map(t._1) = map.getOrElseUpdate(t._1,0)+t._2
      map
    })
    println(stringToInt)



    val l1 = List(1,List(2,3),List(4,5))


    val list2: List[Any] = l1.flatMap(x => {
      if (x.isInstanceOf[List[Int]]) {
        x.asInstanceOf[List[Int]]
      } else {
        List(x)
      }
    })
    list
    println(list)

    println(l1)
    val list1 = 1::l1:::list
    println(list1)
  }

}
