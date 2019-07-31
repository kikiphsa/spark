package com.atguigu.sparkmall.realtime

import com.atguigu.sparkmall.common.util.{DateUtil, MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
  * Create by chenqinping on 2019/5/21 9:20
  */
object Req5DateAreaCityAdcClickApplication {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("Req5DateAreaCityAdcClickApplication").setMaster("local[*]")

    val streamingContext = new StreamingContext(sparkConf, Seconds(5))

    val topic = "ads_log"
    val kafkaMessage: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, streamingContext)

    val messageDStream: DStream[KafkaMessage] = kafkaMessage.map {
      case record => {
        val values: Array[String] = record.value().split(" ")
        KafkaMessage(values(0), values(1), values(2), values(3), values(4))
      }
    }
    messageDStream.print()

    val dateStrngDStream: DStream[(String, Int)] = messageDStream.map(message => {
      val dateString: String = DateUtil.parseTimeToString(message.timestamp.toLong, "yyyy-MM-dd")
      (dateString + ":" + message.province + ":" + message.city + ":" + message.adid, 1)
    })

    streamingContext.sparkContext.setCheckpointDir("cp")
    val totalDStream: DStream[(String, Int)] = dateStrngDStream.updateStateByKey {
      case (seq, buffer) => {
        val total: Int = buffer.getOrElse(0)
        Option(total)
      }
    }

    //    totalDStream.foreachRDD(rdd=>rdd.collect.foreach(println))
    //    println(totalDStream.toString)
    totalDStream.foreachRDD(rdd => {
      rdd.foreachPartition { datas => {
        val jedis: Jedis = RedisUtil.getJedisClient
        val key = "date:area:city:ads"
        for ((field, sum) <- datas) {
          jedis.hset(key, field, sum.toString)
        }

        jedis.close()
      }
      }
    })

    streamingContext.start()
    streamingContext.awaitTermination()
  }

}
