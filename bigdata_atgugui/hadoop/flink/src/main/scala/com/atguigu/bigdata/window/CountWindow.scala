package com.atguigu.bigdata.window

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow}

/**
  * Create by chenqinping on 2019/6/5 15:13
  */
object CountWindow {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    /*val stream: DataStream[String] = env.readTextFile(
      "C:\\\\D\\\\JAVA\\\\spark\\\\workbase\\\\spark\\\\hadoop\\\\flink\\\\in\\\\test02.txt"

    )*/

    val stream: DataStream[String] = env.socketTextStream("hadoop102", 2222)

    val streamKeyBy: KeyedStream[(String, Long), Tuple] =
      stream.map(item => (item, 1l)).keyBy(0)


    val countWinon: WindowedStream[(String, Long), Tuple, TimeWindow] = streamKeyBy.timeWindow(Time.seconds(10), Time.seconds(2))

    /*  reduce
    val streamReduce: DataStream[(String, Long)] = countWinon.reduce(
       (item1, item2) => (item1._1, item1._2 + item2._2)
     )
 */


    /* fold
    val streamReduce= countWinon.fold(100)(
       (begin, time) => begin + time._2.toInt
     )*/
    val streamReduce = countWinon.max(1)


    streamReduce.print()

    env.execute("job")

  }

}
