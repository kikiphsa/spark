package com.youfan.bigdata.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Create by chenqinping on 2019/5/14 10:54
  */
object Spark_Streaming {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new  SparkConf().setAppName("Spark_Streaming").setMaster("local[*]")

    val streamingContext = new StreamingContext(sparkConf,Seconds(4))


    val socketTextStream: ReceiverInputDStream[String] = streamingContext.socketTextStream("hadoop102",9999)

    val wordDStream: DStream[String] = socketTextStream.flatMap(words=>words.split(" "))

    val mapDStream: DStream[(String, Int)] = wordDStream.map((_,1))

    val reduceBykey: DStream[(String, Int)] = mapDStream.reduceByKey(_+_)

    reduceBykey.print()

    streamingContext.start()
    streamingContext.awaitTermination()

  }

}
