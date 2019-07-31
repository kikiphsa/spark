package com.atguigu.sparkmall.realtime

import com.atguigu.sparkmall.common.util.{DateUtil, MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

/**
  * Create by chenqinping on 2019/5/21 9:20
  */
object Req6DateAreaCityAdcClickApplication {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("Req6DateAreaCityAdcClickApplication").setMaster("local[*]")

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

    //   ==================需求6====================================
    // TODO 4.1 获取需求五的数据
    // TODO 4.2 将数据进行结构的转换（date-area-city-adv, sum）(date-area-adv, sum)
    val dataAreaAdvSumDStream: DStream[(String, Int)] = totalDStream.map {
      case (keys, sum) => {
        val ks: Array[String] = keys.split(":")
        (ks(0) + "_" + ks(1) + "_" + ks(3), sum)
      }
    }

    //TODO 4.3 将转换结构后的数据进行统计（date-area-adv，totalsum）
    val dataAreaAdvSumReduce: DStream[(String, Int)] = dataAreaAdvSumDStream.reduceByKey(_ + _)

    //TODO 4.4将统计后的结果转换结构（date-area-adv，totalsum）→（date-area，（adv，totalsum））
    val dateProAndAdidSumDstream: DStream[(String, (String, Int))] = dataAreaAdvSumReduce.map {
      case (keys, sum) => {
        val ks: Array[String] = keys.split("_")
        (ks(0) + "_" + ks(1), (ks(2), sum))
      }
    }


    //TODO 4.5将转换结构后的数据进行分组（date-area，Iterator[（adv，totalsum）J）

    val dateProAndAdidSumGroup: DStream[(String, Iterable[(String, Int)])] = dateProAndAdidSumDstream.groupByKey()

    //TODO 4.6将分组后的数据进行排序（降序）
    val mapTop3: DStream[(String, Map[String, Int])] = dateProAndAdidSumGroup.mapValues(datas => {
      datas.toList.sortWith {
        case (r, l) => {
          r._2 > l._2
        }
      }.take(3).toMap
    })


    //TODO 4.8将结果保存到redis中
    mapTop3.foreachRDD(rdd => {
      rdd.foreachPartition(datas => {
        val jedis: Jedis = RedisUtil.getJedisClient
        for ((keys, map) <- datas) {
          val ks = keys.split("_")
          val key = "ddd:" + ks(0)
          val field = ks(1)

          import org.json4s.JsonDSL._
          val listSting: String = JsonMethods.compact(JsonMethods.render(map))
          jedis.hset(key, field, listSting)
        }
        jedis.close()
      })
    })


    streamingContext.start()
    streamingContext.awaitTermination()
  }

}
