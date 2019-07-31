package flink1111.app.test

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object TestEventTimeApp {


  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 创建SocketSource
    val stream = env.socketTextStream("hadoop1", 7777)

    // 对stream进行处理并按key聚合
    val streamKeyBy = stream.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[String](Time.milliseconds(0)) {
        override def extractTimestamp(element: String): Long = {
          val sysTime = element.split(" ")(0).toLong
          println(sysTime)
          sysTime
        }}).map(item => (item.split(" ")(1), 1)).keyBy(0)

    // 引入滚动窗口
    val streamWindow = streamKeyBy.window(TumblingEventTimeWindows.of(Time.seconds(10)))

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
