package com.youfan.bigdata.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Create by chenqinping on 2019/5/10 22:38
  */
object Scala_copy {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("Scala_copy").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    /*   val words = Array("one", "two", "two", "three", "three", "three")

       val wordRDD: RDD[(String, Int)] = sc.parallelize(words).map(word=>(word,1))

       val groupbyRDD: RDD[(String, Iterable[Int])] = wordRDD.groupByKey()

       groupbyRDD.collect.foreach(println)

       val sumRDD: RDD[(String, Int)] = groupbyRDD.map(t=>(t._1,t._2.sum))
       sumRDD.collect.foreach(println)*/

    /* val rdd = sc.parallelize(List(("female",1),("male",5),("female",5),("male",2)))

     val reduceRDD: RDD[(String, Int)] = rdd.reduceByKey((x,y)=>x+y)

     reduceRDD.collect.foreach(println)*/

    /* val rdd = sc.parallelize(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)

     //    val aggRDD: RDD[(String, Int)] = rdd.aggregateByKey(0)(math.max(_,_),_+_)
     val foldRDD: RDD[(String, Int)] = rdd.foldByKey(0)(_ + _)
     foldRDD.collect.foreach(println)*/

    val input = sc.parallelize(Array(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)), 2)

    val comRDD: RDD[(String, (Int, Int))] = input.combineByKey(
      (_, 1),
      (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
      (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    )
    comRDD.collect().foreach(println)

    val toRDD: RDD[(String, Double)] = comRDD.map {

      case (x, y) => {
        (x, y._1 / y._2.toDouble)
      }
    }
    toRDD.collect().foreach(println)

  }
}
