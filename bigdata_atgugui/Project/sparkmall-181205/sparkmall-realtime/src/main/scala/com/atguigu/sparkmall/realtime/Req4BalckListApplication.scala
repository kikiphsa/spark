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
object Req4BalckListApplication {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("Req4BalckListApplication").setMaster("local[*]")

    val streamingContext = new StreamingContext(sparkConf, Seconds(5))

    val topic = "ads_log"
    val kafkaMessage: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, streamingContext)

    val messageDStream: DStream[KafkaMessage] = kafkaMessage.map {
      case record => {
        val values: Array[String] = record.value().split(" ")
        KafkaMessage(values(0), values(1), values(2), values(3), values(4))
      }
    }

    println("1")
    messageDStream.print()

    //TODO 获取kafka数据
    val blacklist = "blacklist"

    //TODO 将消费的数据加入到黑名单
    /*   val jedisClient: Jedis = RedisUtil.getJedisClient
      //smembers取出改值得所有值
       val blacklSet: util.Set[String] = jedisClient.smembers(blacklist)
      println("============"+blacklSet)
       jedisClient.close()

       val broadcastSet: Broadcast[util.Set[String]] = streamingContext.sparkContext.broadcast(blacklSet)


        val filterDStream: DStream[KafkaMessage] = messageDStream.filter(message => {
          !broadcastSet.value.contains(message.userid)
        })*/
    //TODO Diver(1) 周期性的采取
    val filterDStream: DStream[KafkaMessage] = messageDStream.transform(rdd => {
      val jedisClient: Jedis = RedisUtil.getJedisClient
      val blacklSet: util.Set[String] = jedisClient.smembers(blacklist)
      jedisClient.close()
      val broadcastSet: Broadcast[util.Set[String]] = streamingContext.sparkContext.broadcast(blacklSet)
      //TODO Diver(N)
      rdd.filter(message => {
        //TODO Exector(N)
        !broadcastSet.value.contains(message.userid)
      })
    })

    //TODO 2 想redis加入
    filterDStream.foreachRDD(rdd => {
      rdd.foreachPartition(datas => {
        val jedis: Jedis = RedisUtil.getJedisClient
        datas.foreach(messaage => {
          val key = "data:adv:user:click"
          val dataString: String = DateUtil.parseTimeToString(messaage.timestamp.toLong, "yyyy-MM-dd")
          val field = dataString + ":" + messaage.adid + ":" + messaage.userid
          jedis.hincrBy(key, field, 1)

          //TODO 3.0 获取当前redis中的数据统计结果
          val sumClick: Long = jedis.hget(key, field).toLong

          //TODO 4.0 判断统计结果是否超过阀值
          if (sumClick >= 100) {
            jedis.sadd(blacklist, messaage.userid)
          }
        })
        jedis.close()
      })

    })


    streamingContext.start()
    streamingContext.awaitTermination()
  }

}

case class KafkaMessage(timestamp: String, province: String, city: String, userid: String, adid: String)