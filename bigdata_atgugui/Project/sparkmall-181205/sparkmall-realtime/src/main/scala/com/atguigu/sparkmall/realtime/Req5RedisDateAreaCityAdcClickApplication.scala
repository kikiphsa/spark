package com.atguigu.sparkmall.realtime

import java.util

import com.atguigu.sparkmall.common.util.{DateUtil, MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
  * Create by chenqinping on 2019/5/21 9:20
  */
object Req5RedisDateAreaCityAdcClickApplication {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("Req5RedisDateAreaCityAdcClickApplication").setMaster("local[*]")

    val streamingContext = new StreamingContext(sparkConf, Seconds(5))

    val topic = "ads_log"
    val kafkaMessage: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, streamingContext)

    val messageDStream: DStream[KafkaMessage] = kafkaMessage.map {
      case record => {
        val values: Array[String] = record.value().split(" ")
        KafkaMessage(values(0), values(1), values(2), values(3), values(4))
      }
    }

    messageDStream.foreachRDD(rdd => {
      rdd.foreachPartition(datas => {


        val jedis: Jedis = RedisUtil.getJedisClient

        val key = "date:area:city:ads"
        datas.foreach(data => {

          val dateString: String = DateUtil.parseTimeToString(data.timestamp.toLong, "yyyy-MM-dd")
          val field = dateString + ":" + data.province + ":" + data.city + ":" + data.adid
          jedis.hincrBy(key, field, 1)
        })
        jedis.close()
      })
    })

    streamingContext.start()
    streamingContext.awaitTermination()
  }

}
