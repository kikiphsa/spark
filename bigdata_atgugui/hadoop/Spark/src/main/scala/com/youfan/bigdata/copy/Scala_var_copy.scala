package com.youfan.bigdata.copy

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Create by chenqinping on 2019/5/11 14:22
  */

object Scala_var {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("Scala_var").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    val rdd1: RDD[(Int, String)] = sc.makeRDD(List((1, "a"), (2, "b"), (3, "c")))
    //    val rdd2: RDD[(Int, Int)] = sc.makeRDD(List((1, 1), (2, 2), (3, 3)))

    val list = List((1, 1), (2, 2), (3, 3))

    val broadcast: Broadcast[List[(Int, Int)]] = sc.broadcast(list)

    val listRDD: RDD[(Int, (String, Any))] = rdd1.map {
      case (key, value) => {
        var v2: Any = null
        for (elem <- broadcast.value) {
          if (key == elem._1) {
            v2 = elem._2
          }
        }
        (key, (value, v2))
      }
    }
    listRDD.foreach(println)

    sc.stop()
  }
}
