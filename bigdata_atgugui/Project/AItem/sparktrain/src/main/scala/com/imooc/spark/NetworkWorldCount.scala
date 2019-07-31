package com.imooc.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Create by chenqinping on 2019/5/19 22:57
  */
object NetworkWorldCount {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("NetworkWorldCount").setMaster("local[*]")

    val ssc = new StreamingContext(sparkConf, Seconds(3))


    val ds: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 6379)

    val result: DStream[(String, Int)] = ds.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    result.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
