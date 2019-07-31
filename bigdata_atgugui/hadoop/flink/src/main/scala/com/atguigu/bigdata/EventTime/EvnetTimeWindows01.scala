package com.atguigu.bigdata.EventTime

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Create by chenqinping on 2019/6/5 19:35
  */
object EvnetTimeWindows01 {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    val stream: DataStream[String] = env.socketTextStream("hadoop102", 2222)
    stream.print()
    // 对stream进行处理并按key聚合
    val streamKeyBy: KeyedStream[(String, Int), Tuple] = stream.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[String](Time.milliseconds(0)) {
        override def extractTimestamp(element: String): Long = {
          val sysTime = element.split(" ")(0).toLong
          println(sysTime)
          sysTime
        }
      }).map(item => (item.split(" ")(1), 1)).keyBy(0)

    // 引入滚动窗口
    val streamWindow = streamKeyBy.window(TumblingEventTimeWindows.of(Time.seconds(5)))

    // 引入滑动窗口
    //    val streamWindow = streamKeyBy.window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5)))

    // 引入会话窗口
    //    val streamWindow = streamKeyBy.window(EventTimeSessionWindows.withGap(Time.seconds(5)))


    // 执行聚合操作
    val streamReduce = streamWindow.reduce(
      (item1, item2) => (item1._1, item1._2 + item2._2)
    )

    // 将聚合数据写入文件
    streamReduce.print

    // 执行程序
    env.execute("TumblingWindow")
  }

}
