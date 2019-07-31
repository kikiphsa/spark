package com.imooc.spark

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Create by chenqinping on 2019/5/27 19:59
  */
object TransformApp {
  def main(args: Array[String]): Unit = {


    val sparkConf: SparkConf = new SparkConf().setAppName("TransformApp").setMaster("local[*]")

    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val strings = List("zs", "ls")

    val blacksRDD: RDD[(String, Boolean)] = ssc.sparkContext.parallelize(strings).map(x => (x, true))

    val liens: ReceiverInputDStream[String] = ssc.socketTextStream("linux", 9999)

    val values: DStream[Array[String]] = liens.map(_.split(","))

    val listRDD: DStream[(String, String)] = liens.map(x => (x.split(",")(1), x))

    val clicklog: DStream[String] = listRDD.transform(rdd => {
      rdd.leftOuterJoin(blacksRDD)
        .filter(x => x._2._2.getOrElse(false) != true)
        .map(x => x._2._1)
    })

    clicklog.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
