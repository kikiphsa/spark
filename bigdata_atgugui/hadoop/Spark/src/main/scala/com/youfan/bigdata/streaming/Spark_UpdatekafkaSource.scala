package com.youfan.bigdata.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Create by chenqinping on 2019/5/14 14:06
  */
object Spark_UpdatekafkaSource {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("Spark_UpdatekafkaSource").setMaster("local[*]")

    val streamingContext = new StreamingContext(sparkConf, Seconds(4))

    //保存检查点的路径
    streamingContext.sparkContext.setCheckpointDir("cp")

    val kafkaList: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(
      streamingContext,
      "hadoop102:2181",
      "atguigu",
      Map("atguigu" -> 3)
    )
    val kafkaDStream: DStream[String] = kafkaList.flatMap(t => t._2.split(" "))

    val mapDStream: DStream[(String, Int)] = kafkaDStream.map((_, 1))
    //    val reduceDSteam: DStream[(String, Int)] = mapDStream.reduceByKey(_ + _)

    val reduceDSteam: DStream[(String, Int)] = mapDStream.updateStateByKey {
      case (seq, buffer) => {
        val sum: Int = buffer.getOrElse(0) + seq.sum
        Option(sum)
      }
    }

    reduceDSteam.print()
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
