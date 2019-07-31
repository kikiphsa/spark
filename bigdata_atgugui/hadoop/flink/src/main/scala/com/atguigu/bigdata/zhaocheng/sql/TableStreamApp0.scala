package com.atguigu.bigdata.zhaocheng.sql

import com.alibaba.fastjson.JSON
import com.atguigu.bigdata.zhaocheng.bean.StartUpLog
import com.atguigu.bigdata.zhaocheng.util.MyKafkaUtil
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.table.api.scala.{StreamTableEnvironment, _}
import org.apache.flink.table.api.{Table, TableEnvironment}

/**
  * Create by chenqinping on 2019/6/10 14:20
  */
object TableStreamApp0 {

  def main(args: Array[String]): Unit = {


    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val tableEnv: StreamTableEnvironment = TableEnvironment.getTableEnvironment(env)

    val myKafkaConsumer: FlinkKafkaConsumer011[String] = MyKafkaUtil.getConsumer("GMALL_STARTUP")

    val dstream: DataStream[String] = env.addSource(myKafkaConsumer)


    val startupLogDstream: DataStream[StartUpLog] = dstream.map { jsonString => JSON.parseObject(jsonString, classOf[StartUpLog]) }

    val startupLogTable: Table = tableEnv.fromDataStream(startupLogDstream)

    val table: Table = startupLogTable.select('mid, 'ch).filter("ch ='appstore'")

    val midchDataStream: DataStream[(String, String)] = table.toAppendStream[(String, String)]

    midchDataStream.print("oioo")


    env.execute()


  }
}
