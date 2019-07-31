package com.youfan.bigdata.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Create by chenqinping on 2019/5/14 14:06
  */
object Spark_Window {

  def main(args: Array[String]): Unit = {

    /* val list = List(1,2,3,4,5)
     val iterators: Iterator[List[Int]] = list.sliding(3,2)

     for (elem <- iterators) {
       println(elem)
     }*/

    val sparkConf: SparkConf = new SparkConf().setAppName("Spark_Window").setMaster("local[*]")

    val streamingContext = new StreamingContext(sparkConf, Seconds(3))

    val kafkaDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(
      streamingContext,
      "hadoop102:2181",
      "atguigu",
      Map("atguigu" -> 3)
    )
    val windowDStram: DStream[(String, String)] = kafkaDStream.window(Seconds(9),Seconds(3))

    val reduceDStream: DStream[(String, Int)] = windowDStram.flatMap(t=>t._2.split(" ")).map((_,1)).reduceByKey(_+_)


    reduceDStream.print()
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
