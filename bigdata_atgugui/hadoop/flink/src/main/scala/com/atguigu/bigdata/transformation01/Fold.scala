package com.atguigu.bigdata.transformation01

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala._

/**
  * Create by chenqinping on 2019/6/5 14:37
  */
object Fold {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val streamKeyBy: KeyedStream[(String, Int), Tuple] = env.readTextFile(
      "C:\\\\D\\\\JAVA\\\\spark\\\\workbase\\\\spark\\\\hadoop\\\\flink\\\\in\\\\test00(1).txt"

    ).flatMap(_.split(" ")).map((_, 1)).keyBy(0)


    val streamFold: DataStream[Int] = streamKeyBy.fold(100)(
      (begin, item) => (begin + item._2)
    )

    streamFold.print()

    env.execute("job")

  }

}
