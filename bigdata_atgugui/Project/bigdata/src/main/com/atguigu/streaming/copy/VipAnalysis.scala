package com.atguigu.streaming.copy

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.atguigu.streaming.VipIncrementAnalysis._
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import net.ipip.ipdb.{City, CityInfo}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scalikejdbc.{ConnectionPool, DB, _}

/**
  * Create by chenqinping on 2019/6/18 11:00
  */
object VipAnalysis {
  // 提取出公共变量，转换算子共用

  val sdf = new SimpleDateFormat("yyyy-MM-dd")
  // 从properties文件里获取各种参数
  private val prop = new Properties()
  prop.load(this.getClass.getClassLoader().getResourceAsStream("VipIncrementAnalysis.properties"))

  // 使用静态ip资源库
  val ipdb = new City(this.getClass.getClassLoader.getResource("ipipfree.ipdb").getPath)
  // 获取jdbc相关参数
  val jdbcDriver: String = prop.getProperty("jdbcDriver")
  val jdbcUrl: String = prop.getProperty("jdbcUrl")
  val jdbcUser = prop.getProperty("jdbcUser")
  val jdbcPassword: String = prop.getProperty("jdbcPassword")
  // 设置jdbc
  Class.forName(jdbcDriver)

  // 设置连接池
  ConnectionPool.singleton(jdbcUrl, jdbcUser, jdbcPassword)

  def main(args: Array[String]): Unit = {

    // 参数检测
    if (args.length != 1) {
      println("传输不对")
      System.exit(1)
    }

    // 通过传入参数设置检查点
    val checkPoint = args(0)

    // 通过getOrCreate方式可以实现从Driver端失败恢复
    val ssc: StreamingContext = StreamingContext.getOrCreate(checkPoint, () => {
      getCheckPoint(checkPoint)
    })

    // 启动流计算
    ssc.start()
    ssc.awaitTermination()

  }


  def getCheckPoint(checkPoint: String): StreamingContext = {


    // 定义update函数
    val function = (values: Seq[Int], state: Option[Int]) => {
      val currentCount: Int = values.sum

      val newCount: Int = state.getOrElse(0)
      Some(currentCount + newCount)
    }

    // 设置批处理间隔
    val processingInterval: Long = prop.getProperty("processingInterval").toLong

    // 获取kafka相关参数
    val brokers: String = prop.getProperty("brokers")

    //设置sparkconf
    val sparkConf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val ssc = new StreamingContext(sparkConf, Seconds(processingInterval))

    // 获取offset
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "enable.auto.commit" -> "false")

    //offset
    val fromOffsets: Map[TopicAndPartition, Long] = DB.readOnly { implicit session =>
      sql"select topic,part_id,offset from topic_offset"
        .map { r =>
          TopicAndPartition(r.string(1), r.int(2)) -> r.long(3)
        }.list().apply().toMap
    }
    //message
    val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic + "-" + mmd.partition, mmd.message())

    val message: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)
    // 获取Dstream

    var offsetRanges = Array.empty[OffsetRange]
    //设置检查点
    ssc.checkpoint(checkPoint)
    message.checkpoint(Seconds(processingInterval * 10))

    // 业务计算     //连接redis transform 周期性执行的

    message.transform { rdd =>
      //每个rdd的偏移量
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }.filter { msg =>
      // 过滤非完成订单的数据，且验证数据合法性
      filter17Length(msg)
    }.map { msg => {
      // 数据转换，返回((2019-04-03,北京),1)格式的数据
      dateCount(msg)
    }
    }.updateStateByKey {
      // 更新状态变量
      function
    } /*.filter { msg =>
      // 只保留最近2天的数据，这样返回给Driver的数据会非常小
      // 将最近两天的数据返回给Driver端
      filterTowDay(msg)
    }*/ .foreachRDD { rdd =>
      val resultTuple: Array[((String, String), Int)] = rdd.collect()
      // 开启事务
      DB.localTx { implicit session =>
        resultTuple.foreach(msg => {
          val dt: String = msg._1._1
          val province: String = msg._1._2
          val cnt: Int = msg._2
          sql"""replace into vip_increment_analysis(province,cnt,dt) values(${province},${cnt},${dt})""".executeUpdate().apply()
          println(msg)
        })

        for (o <- offsetRanges) {
          println(o.topic, o.partition, o.fromOffset, o.untilOffset)
          sql"""update topic_offset set offset=${o.untilOffset} where topic=${o.topic} and part_id=${o.partition}""".update().apply()
        }
      }
    }

    //    value.foreachRDD(rdd => println(rdd.collect()))


    // 统计结果持久化到Mysql中
    ssc
  }

  def filterTowDay(msg: ((String, String), Int)): Boolean = {
    val time: String = msg._1._1
    val eventTime: Long = sdf.parse(time).getTime

    // 获取当前系统时间缀
    val currentTime = System.currentTimeMillis()
    // 两者比较，保留两天内的
    if (currentTime - eventTime >= 172800000) {
      false
    } else {
      true
    }

  }


  def dateCount(msg: (String, String)): ((String, String), Int) = {
    val value: String = msg._1
    val strings: Array[String] = msg._2.split("\t")
    //    ((2019-04-03,北京),1)

    val ip: String = strings(8)

    val time: String = strings(16)

    val dateTime = new Date(time.toLong * 1000)

    val eventTime: String = sdf.format(dateTime)

    var ipInfo = "未知"
    val cityInfo: CityInfo = ipdb.findInfo(ip, "CN")
    if (cityInfo != null) {
      ipInfo = cityInfo.getRegionName
    }

    ((eventTime, ipInfo), 1)
  }

  def filter17Length(msg: (String, String)): Boolean = {
    val strings: Array[String] = msg._2.split("\t")
    if (strings.length == 17) {
      val ecompleteOrder: String = msg._2.split("\t")(15)
      "completeOrder".equals(ecompleteOrder)
    } else {
      false
    }
  }

}
