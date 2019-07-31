package com.youfan.bigdata.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Create by chenqinping on 2019/5/7 15:31
  */
object WordCount {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    val sc = new SparkContext(conf)

    /* val rdd: RDD[String] = sc.textFile("C:\\D\\JAVA\\spark\\workbase\\spark\\hadoop\\Spark\\in/1.txt").flatMap(_.split(" "))

     val words: RDD[(String, Int)] = rdd.map(x=>{(x,1)})

     val reduceByWord: RDD[(String, Int)] = words.reduceByKey(_+_)

     val tuples: Array[(String, Int)] = reduceByWord.collect()

     tuples.foreach(println)*/
    /*
        val unit: RDD[Int] = sc.parallelize(1 to 10)

        unit.foreach(println)

        val map1: RDD[Int] = unit.map(_*3)
        map1.foreach(println)*/

   /* val rdd: RDD[Int] = sc.parallelize(Array(1, 2, 3, 4))

    val map: RDD[Int] = rdd.mapPartitions(x=>{x.map(_*2)})
    map.foreach(print)*/

//    rdd.mapPartitions(x=>{x.})

    /*val map2: RDD[(Int, Int)] = rdd.mapPartitionsWithIndex((index,items)=>(items.map((index,_))))

    map2.foreach(print)*/

    val rdd: RDD[String] = sc.textFile("C:\\D\\JAVA\\spark\\workbase\\spark\\hadoop\\Spark\\in/1.txt").flatMap(_.split(" "))

    rdd.map(x=>{(x,1)}).reduceByKey(_+_).collect().foreach(print)
  }

}
