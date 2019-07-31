package com.youfan.gmall1018.dw.realtime.app

import com.alibaba.fastjson.JSON
import com.youfan.gmall1018.dw.realtime.util.MyKafkaUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.youfan.gmall1018.dw.common.constant.GmallConstant
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import com.youfan.gmall1018.dw.realtime.bean.OrderInfo
import com.youfan.gmall1018.dw.common.util.MyEsUtil

/**
  * Create by chenqinping on 2019/5/8 9:34
  */
object OrderInfoInfo {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("OrderInfoInfo").setMaster("local[*]")

    val ssc = new StreamingContext(new SparkContext(sparkConf), Seconds(5))


    val recordDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.TOPIC_ORDER, ssc)


    val orderInfoDStream: DStream[OrderInfo] = recordDstream.map(_.value()).map { jsonString =>

      val orderInfo: OrderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])

      val createTimeArr: Array[String] = orderInfo.createTime.split(" ")

      orderInfo.createDate = createTimeArr(0)
      orderInfo.createHour = createTimeArr(1).split(":")(0)
      orderInfo.createHourMinute = createTimeArr(1).split(":")(0) + ":" + createTimeArr(1).split(":")(1)

      //脱敏
      orderInfo.consignee = orderInfo.consignee.splitAt(1)._1 + "**"
      orderInfo.consigneeTel = orderInfo.consigneeTel.splitAt(3)._1 + "********"
      val value: String = orderInfo.consignee.splitAt(1)._1
      println(value)
      val value1: String = orderInfo.consignee.splitAt(1)._2
      println(value1)
      orderInfo

    }
    //保存到es
    orderInfoDStream.foreachRDD { rdd =>
        rdd.foreachPartition { orderInfoItr =>
            MyEsUtil.executeIndexBulk("gmall1018_new_order", orderInfoItr.toList)

        }
    }

    ssc.start()
    ssc.awaitTermination()

  }
}
