package com.atguigu.bigdata

import org.apache.flink.streaming.api.scala._

/**
  * Create by chenqinping on 2019/5/12 21:16
  */
object FlinkHello {

  def main(args: Array[String]): Unit = {

    //创建环境


    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment(1)
    //    val stream: DataStream[String] = env.readTextFile("C:\\D\\JAVA\\spark\\workbase\\spark\\hadoop\\flink\\in\\test00(1).txt")

       val list = List((1, 2, 3, 4),(1, 2, 3, 4))
       //    val stream = env.socketTextStream("hadoop102", 11111)
       val stream = env.fromCollection(list)
       stream.print()

//    val stream: DataStream[Long] = env.generateSequence(1, 10)

    stream.writeAsCsv(
      "C:\\\\D\\\\JAVA\\\\spark\\\\workbase\\\\spark\\\\hadoop\\\\flink\\\\in\\\\spl.txt"

    )

    env.execute("FlinkHello")

  }

}
