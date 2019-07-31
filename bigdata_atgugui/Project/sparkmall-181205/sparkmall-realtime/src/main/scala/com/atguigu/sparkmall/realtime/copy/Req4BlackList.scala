package com.atguigu.sparkmall.realtime.copy

import com.atguigu.sparkmall.common.util.{ConfigurationUtil, MyKafkaUtil}
import com.atguigu.sparkmall.realtime.KafkaMessage
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Create by chenqinping on 2019/7/21 14:08
  */
object Req4BlackList {

  def main(args: Array[String]): Unit = {


    // 构建流式数据处理环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Req4BlackList")

    val streamingContext = new StreamingContext(sparkConf, Seconds(5))

    // 消费kafka的数据
    val topic = "ads_log"
    val kafkaMessage: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, streamingContext)

    val messageDStream: DStream[KafkaMessage] = kafkaMessage.map {
      case record => {
        val value = record.value()
        val values = value.split(" ")
        KafkaMessage(values(0), values(1), values(2), values(3), values(4))
      }
    }
    messageDStream.print()

    //      4.X 将消费数据进行黑名单校验，如果用户不在黑名单中，继续访问，如果在黑名单中，直接过滤掉
    //    4.2 向redis中进行数据的更新
    //      4.3 获取当前redis中的数据统计结果
    //      4.4 判断统计结果是否超过阈值
    //      4.5 如果超过阈值，需要将用户拉入黑名单，禁止用户下一回访问

    streamingContext.start()
    streamingContext.awaitTermination()

    //获助配置文件commerce.properties中的Kafk配置参数val broker=Configurationutil.getValueFromConfig（"kafka.broker.1ist"）

   /* import org.apache.spark.sql.Dataset
    @throws[Exception]
    def writeParquet(spark: Nothing, date: String, basePath: String): Unit = {
      val df: Dataset[Nothing] = getVEHICLE_EVENT(spark, date)
      df.show()
      val ParquetUrl: String = basePath + "/" + table + "/date=" + date
      log.error("读取条数目:" + df.count)
      df.write.parquet(ParquetUrl)
      log.error(table + " Parquet存储完毕,时间:" + date)
      log.error("存储地址:" + ParquetUrl)
    }*/
  }

}
