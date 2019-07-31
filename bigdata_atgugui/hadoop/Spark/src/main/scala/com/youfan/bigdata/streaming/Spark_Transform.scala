package com.youfan.bigdata.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Create by chenqinping on 2019/5/14 14:06
  */
object Spark_Transform {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("Spark_Transform").setMaster("local[*]")

    val streamingContext = new StreamingContext(sparkConf, Seconds(3))




    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
