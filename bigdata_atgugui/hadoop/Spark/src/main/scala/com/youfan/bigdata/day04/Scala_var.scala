package com.youfan.bigdata.day04

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Create by chenqinping on 2019/5/11 14:22
  */

import scala.util.parsing.json.JSON

object Scala_var {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("Scala_var").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    val rdd1: RDD[(Int, String)] = sc.makeRDD(List((1, "a"), (2, "b"), (3, "c")))
    //    val rdd2: RDD[(Int, Int)] = sc.makeRDD(List((1,1),(2,2),(3,3)))

    /*   val joinRDD: RDD[(Int, (String, Int))] = rdd1.join(rdd2)

       joinRDD.foreach(println)

       (2,(b,2))
   (1,(a,1))
   (3,(c,3))
       */

    val list = List((1, 1), (2, 2), (3, 3))


    //可以使用广播变量减少数据的传输 (只读变量)
    //构建广播变量
    val broadcast: Broadcast[List[(Int, Int)]] = sc.broadcast(list)

    val rddRDD: RDD[(Int, (String, Any))] = rdd1.map {
      case (key, value) => {
        var t2: Any = null

        //使用广播变量
        for (elem <- broadcast.value) {
          if (key == elem._1) {
            t2 = elem._2
          }
        }

        (key, (value, t2))
      }
    }
    rddRDD.foreach(println)

    sc.stop()

  }
}
