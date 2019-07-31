package com.atguigu.bigdata.transformation01

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala._

/**
  * Create by chenqinping on 2019/6/5 14:24
  */
object Transformations02 {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment


    val stream: DataStream[(String, Long)] = env.readTextFile(
      "C:\\\\D\\\\JAVA\\\\spark\\\\workbase\\\\spark\\\\hadoop\\\\flink\\\\in\\\\test00(1).txt"
    ).flatMap(_.split(" ")).map((_, 1L))

    val streamkeyBy: KeyedStream[(String, Long), Tuple] = stream.keyBy(0)

    val streamReduce: DataStream[(String, Long)] = streamkeyBy.reduce(
      (item1, item2) => (item1._1, item1._2 + item2._2)
    )


    streamReduce.print()

    env.execute("job")

  }

}
