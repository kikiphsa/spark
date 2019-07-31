package com.youfan.bigdata.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Create by chenqinping on 2019/5/10 9:54
  */
object Scala_03 {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("Scala_03").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    val list: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 4)

    //    list.coalesce()

    /* val repartitions: RDD[Int] = list.repartition(2)

     repartitions.glom.collect().foreach(println)

     val rdd = sc.parallelize(Array((1,"aaa"),(2,"bbb"),(3,"ccc"),(4,"ddd")),4)

     rdd.partitions*/

    val words = Array("one", "two", "two", "three", "three", "three")
    /*val wordMap: RDD[(String, Int)] = sc.parallelize(words).map(x=>(x,1)).reduceByKey(_+_)
//    wordMap.map(x=>(x._1,x._2.))
    wordMap.groupByKey.collect().foreach(println)*/

    /*  val rdd = sc.parallelize(List(("a",3),("a",2),("c",4),("b",3),("c",6),("c",8)),2)

  //    rdd.glom().foreach(println)

      val aggreRDD: RDD[(String, Int)] = rdd.aggregateByKey(0)(math.max(_,_),_+_)

      aggreRDD.collect().foreach(println)*/

    /*val rdd = sc.parallelize(List(("a",3),("a",2),("c",4),("b",3),("c",6),("c",8)),2)


    val aggRDD: RDD[(String, Int)] = rdd.aggregateByKey(0)(_+_,_+_)
*/

    /* val rdd = sc.parallelize(List(("a",3),("a",2),("c",4),("b",3),("c",6),("c",8)),2)
     val aggRDD: RDD[(String, Int)] = rdd.foldByKey(0)(_+_)
     aggRDD.collect().foreach(println)*/
    /*
        val input = sc.parallelize(Array(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)), 2)

        val valueRDD: RDD[(String, (Int, Int))] = input.combineByKey(
          (_, 1),
          (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
          (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
        )
        val value: RDD[(String, Double)] = valueRDD.map {

          case (key, value) => (key, value._1 / value._2.toDouble)
        }

        value.collect().foreach(println)*/


  /*  val rdd = sc.parallelize(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)

    val aggRDD: RDD[(String, Int)] = rdd.aggregateByKey(0)(math.max(_, _), _ + _)

    aggRDD.collect.foreach(println)

    val foldRdd = sc.parallelize(List((1, 3), (1, 2), (1, 4), (2, 3), (3, 6), (3, 8)), 3)

    val fold: RDD[(Int, Int)] = foldRdd.foldByKey(0)(_ + _)
    fold.collect.foreach(println)


    val input = sc.parallelize(Array(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)), 2)

    val comRDD: RDD[(String, (Int, Int))] = input.combineByKey(
      (_, 1),
      (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
      (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    )
    val com: RDD[(String, Double)] = comRDD.map {
      case (key, value) => (key, value._1 / value._2.toDouble)
    }
    com.collect().foreach(println)*/


    val rdd = sc.parallelize(Array((3,"aa"),(6,"cc"),(2,"bb"),(1,"dd")))

    rdd.sortByKey(false).collect().foreach(println)


    val rdd3 = sc.parallelize(Array((1,"a"),(1,"d"),(2,"b"),(3,"c")))

    rdd3.mapValues(_+"a").collect().foreach(println)
  }


}
