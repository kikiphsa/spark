package com.youfan.bigdata.copy

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Create by chenqinping on 2019/5/14 15:18
  */
object Spark_kafkaSourceCopy {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("Spark_kafkaSourceCopy").setMaster("local[*]")

    val streamingContext = new StreamingContext(sparkConf, Seconds(3))

    streamingContext.sparkContext.setCheckpointDir("cp")

    val kafkaDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(
      streamingContext,
      "hadoop102:2181",
      "atguigu",
      Map("atguigu" -> 2)

    )


    val list: DStream[String] = kafkaDStream.flatMap(t => t._2.split(" "))


    val mapDStream: DStream[(String, Int)] = list.map((_, 1))

    val reduceDStream: DStream[(String, Int)] = mapDStream.updateStateByKey {
      case (seq, buffer) => {
        val sum: Int = buffer.getOrElse(0) + seq.sum
        Option(sum)
      }
    }
    //    val reduceDStream: DStream[(String, Int)] = mapDStream.reduceByKey(_ + _)


    reduceDStream.print()


    streamingContext.start()
    streamingContext.awaitTermination()

  }

}
