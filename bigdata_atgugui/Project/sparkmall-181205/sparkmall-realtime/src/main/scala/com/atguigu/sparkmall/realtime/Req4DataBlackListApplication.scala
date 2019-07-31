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
object Req4DataBlackListApplication {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("Req4DataBlackListApplication").setMaster("local[*]")

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

    // TODO 4.1 获取kafka的消费数据
    val blacklist = "blacklist"

    // TODO 4.X 将消费数据进行黑名单校验，如果用户不在黑名单中，继续访问，如果在黑名单中，直接过滤掉
    /*
    val jedisClient: Jedis = RedisUtil.getJedisClient
    val blacklistSet: util.Set[String] = jedisClient.smembers(blacklist)
    //println(blacklistSet.size())
    //blacklistSet.contains("xxxx")
    jedisClient.close()

    // 广播变量采用的序列化规则不是java的
    val setBroadcast: Broadcast[util.Set[String]] = streamingContext.sparkContext.broadcast(blacklistSet)


    val filterDStream: DStream[KafkaMessage] = messageDStream.filter(message => {
        !setBroadcast.value.contains(message.userid)
    })
    */
    // TODO Driver (1)
    val filterDStream: DStream[KafkaMessage] = messageDStream.transform(rdd => {
      // TODO Driver(N)
      val jedisClient: Jedis = RedisUtil.getJedisClient
      val blacklistSet: util.Set[String] = jedisClient.smembers(blacklist)
      jedisClient.close()

      val setBroadcast: Broadcast[util.Set[String]] = streamingContext.sparkContext.broadcast(blacklistSet)

      rdd.filter(message => {
        // TODO Executor(M)
        !setBroadcast.value.contains(message.userid)
      })
    })

    // 将过滤后的数据进行结构的转换，为了方便统计
    val dateAndAdvAndUserToClickDStream: DStream[(String, Int)] = filterDStream.map {
      message => {

        val dateString: String = DateUtil.parseTimeToString(message.timestamp.toLong, "yyyy-MM-dd")

        (dateString + "_" + message.adid + "_" + message.userid, 1)
      }
    }

    // 使用有状态的聚合操作
    streamingContext.sparkContext.setCheckpointDir("cp")
    val dateAndAdvAndUserToSumDStream: DStream[(String, Int)] = dateAndAdvAndUserToClickDStream.updateStateByKey {
      case (seq, buffer) => {
        val total = seq.sum + buffer.getOrElse(0)
        Option(total)
      }
    }

    // 判断有状态数据聚合结果是否超过阈值，如果超过，将用户拉入黑马单
    dateAndAdvAndUserToSumDStream.foreachRDD(rdd => {
      rdd.foreach {
        case (key, sum) => {
          if (sum >= 100) {
            val jedisClient: Jedis = RedisUtil.getJedisClient
            jedisClient.sadd(blacklist, key.split("_")(2))
            jedisClient.close()
          }
        }
      }
    })

    streamingContext.start()
    streamingContext.awaitTermination()
  }

}
