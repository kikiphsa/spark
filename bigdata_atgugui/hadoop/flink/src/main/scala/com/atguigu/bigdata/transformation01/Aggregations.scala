package com.atguigu.bigdata.transformation01

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala._

/**
  * Create by chenqinping on 2019/6/5 14:50
  */
object Aggregations {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment


    val streamKeyBy: KeyedStream[(String, Long), Tuple] = env.readTextFile(
      "C:\\\\D\\\\JAVA\\\\spark\\\\workbase\\\\spark\\\\hadoop\\\\flink\\\\in\\\\test02.txt"

    ).map(item => (item.split(" ")(0), item.split(" ")(1).toLong)).keyBy(0)

    val streamSum: DataStream[(String, Long)] = streamKeyBy.sum(1)

    streamSum.print()

    env.execute("job")

  }

}
