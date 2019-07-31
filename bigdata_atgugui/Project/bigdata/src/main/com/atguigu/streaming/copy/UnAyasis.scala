package com.atguigu.streaming.copy

import java.util.Properties

import com.atguigu.streaming.copy.VipAnalysis.{jdbcPassword, jdbcUrl, jdbcUser, prop}
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scalikejdbc.ConnectionPool
import scalikejdbc.{ConnectionPool, DB, _}
import sun.net.www.MessageHeader

/**
  * Create by chenqinping on 2019/6/18 19:51
  */
object UnAyasis {

  // 从properties文件里获取各种参数
  private val prop = new Properties()
  prop.load(this.getClass.getClassLoader.getResourceAsStream("UnPaymentAnalysis.properties"))
  // 获取jdbc相关参数
  val jdbcDriver: String = prop.getProperty("jdbcDriver")
  val jdbcUrl: String = prop.getProperty("jdbcUrl")
  val jdbcUser = prop.getProperty("jdbcUser")
  val jdbcPassword: String = prop.getProperty("jdbcPassword")
  // 设置批处理间隔
  val processingInterval: Long = prop.getProperty("processingInterval").toLong
  // 获取kafka相关参数
  val brokers: String = prop.getProperty("brokers")
  // 设置jdbc
  Class.forName(jdbcDriver)

  // 设置连接池
  ConnectionPool.singleton(jdbcUrl, jdbcUser, jdbcPassword)


  def main(args: Array[String]): Unit = {

    //sparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName)

    val ssc = new StreamingContext(sparkConf, Seconds(processingInterval))

    //kafkaParams
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "enable.auto.commit" -> "false")

    // 获取offset
    val fromOffSet: Map[TopicAndPartition, Long] = DB.readOnly { implicit session =>
      sql"""select topic,part_id,offset from unpayment_topic_offset""".map {
        r =>
          TopicAndPartition(r.string(1), r.int(2)) -> r.long(3)
      }.list().apply().toMap
    }
    //设置messageHandler
    val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
    // 获取kafka Dstream
    val message: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffSet, messageHandler)

    var offsetRanges = Array.empty[OffsetRange]

    // 业务计算
    message.transform { rdd =>
      //每个rdd的偏移量
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }.filter { msg =>
      // 过滤非完成订单的数据，且验证数据合法性
      filterlenthg(msg)
    }.map { msg =>
      //// 数据格式转换为(uid,1)
      mapUidOne(msg)
    }.reduceByKeyAndWindow((a: Int, b: Int) => a + b, Seconds(processingInterval * 4), Seconds(processingInterval * 2))
      .filter { msg =>
        // 过滤定单行为异常的用户
        filterUser(msg)
      }.foreachRDD { rdd =>
      // 返回结果到客户端

      val tuples: Array[(String, Int)] = rdd.collect()

      // 开启事务
      DB.localTx { implicit session =>
        tuples.foreach { msg =>
          val uid: String = msg._1
          val count: Int = msg._2

          sql"""replace into unpayment_record(uid) values (${uid})""".executeUpdate().apply()
          println(msg)
        }
        // 统计结果持久化到Mysql中
        // 保存offset
        for (o <- offsetRanges) {
          println(o.topic, o.partition, o.untilOffset, o.fromOffset)
          sql"""update unpayment_topic_offset set offset=${o.untilOffset} where topic=${o.topic} and part_id=${o.partition}""".update().apply()
        }

      }

    }


    ssc.start()
    ssc.awaitTermination()
  }

  // 当进入订单页大于等于3时，去业务表查询用户当前是否为vip状态
  def filterUser(msg: (String, Int)): Boolean = {
    val uid: String = msg._1
    val count: Int = msg._2

    if (count >= 3) {
      val ints = DB.readOnly { implicit session =>
        sql"""select uid from vip_user where uid=${uid}""".map { r =>
          r.get[Int](1)
        }.list().apply()
      }
      if (ints.isEmpty) {
        true
      } else {
        false
      }
    } else {
      false
    }
  }


  def mapUidOne(msg: (String, String)) = {
    val strings: Array[String] = msg._2.split("\t")
    val uid: String = strings(0)
    (uid, 1)
  }


  def filterlenthg(msg: (String, String)): Boolean = {
    val strings: Array[String] = msg._2.split("\t")
    if (strings.length == 17) {

      val enterOrderPage: String = msg._2.split("\t")(15)
      "enterOrderPage".equals(enterOrderPage)
    } else {
      false
    }
  }

}
