package com.atguigu.bigdata.zhaocheng

import com.alibaba.fastjson.JSON
import com.atguigu.bigdata.zhaocheng.bean.StartUpLog
import com.atguigu.bigdata.zhaocheng.util.{MyEsUtil, MyKafkaUtil, MyRedisUtil}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
  * Create by chenqinping on 2019/6/6 9:47
  */
object DataSetMap {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置同时并发线程数
    env.setParallelism(8)
    val kafkaString: FlinkKafkaConsumer011[String] = MyKafkaUtil.getConsumer("GMALL_STARTUP")

    val kafkaStream: DataStream[String] = env.addSource(kafkaString)

    val statupLogDStream: DataStream[StartUpLog] = kafkaStream.map(jsonString => JSON.parseObject(jsonString, classOf[StartUpLog]))

    /*val streamMap: KeyedStream[(String, Int), Tuple] = statupLogDStream.map(item => (item.ch, 1)).keyBy(0)

    val reducestream: DataStream[(String, Int)] = streamMap.reduce(
      (item1, item2) => (item1._1, item1._2 + item2._2)
    )
    val stringStream: DataStream[(String, String)] = reducestream.map(item => (item._1, item._2 + ""))

    stringStream.addSink(MyRedisUtil.getRedisSink())


    val filterstream: DataStream[StartUpLog] = statupLogDStream.filter(item => item.ch == "xiaomi")*/
    /* statupLogDStream.split { startupLog =>
       var flag: List[String] = null

       if ("".equals(startupLog.ch)) {

       }
     }*/

    // 明细发送到es 中
    //    val esSink: ElasticsearchSink[String] = MyEsUtil.getEsSink(
    //      "gmall1018_dau")
    //    kafkaStream.addSink(esSink)


    //每10秒钟统计一次
    val streamMap: KeyedStream[(String, Int), Tuple] = statupLogDStream.map(item => (item.ch, 1)).keyBy(0)

//    val windowStram: WindowedStream[(String, Int), Tuple, TimeWindow] = streamMap.timeWindow(Time.seconds(10), Time.seconds(5))
    val windowStram: WindowedStream[(String, Int), Tuple, GlobalWindow] = streamMap.countWindow(100L,10L)

    val reducestream: DataStream[(String, Int)] = windowStram.reduce(
      (item1, item2) => (item1._1, item1._2 + item2._2)
    )

    reducestream.print("window::")

    env.execute("job")

  }

}
