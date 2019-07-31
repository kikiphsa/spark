package com.atguigu.bigdata.transformation01

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}


/**
  * Create by chenqinping on 2019/6/5 10:14
  */
object Transformation01 {

  import org.apache.flink.streaming.api.scala._

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    /*
        Map操作
        val stream: DataStream[Long] = env.generateSequence(1, 10)
        val sum: DataStream[Long] = stream.map(item => item * 2)*/

    /* flatMap
   val text: DataStream[String] = env.readTextFile("C:\\\\D\\\\JAVA\\\\spark\\\\workbase\\\\spark\\\\hadoop\\\\flink\\\\in\\\\test00(1).txt")
    val streamFlat = text.flatMap(item => item.split(" "))*/


    /* filter
      val data: DataStream[Long] = env.generateSequence(1, 10)
      val streamFlat: DataStream[Long] = data.filter(item => item != 1)*/

    /*   Connect CastMap 操作
        val data01: DataStream[Long] = env.generateSequence(1, 10)

        val text: DataStream[String] = env.readTextFile(
          "C:\\\\D\\\\JAVA\\\\spark\\\\workbase\\\\spark\\\\hadoop\\\\flink\\\\in\\\\test00(1).txt")
          .flatMap(_.split(" "))

        val streamConnect: ConnectedStreams[Long, String] = data01.connect(text)

        val streamFlat: DataStream[Any] = streamConnect.map(item => item * 2, item => item.map((_, 1)))*/


    val stream: DataStream[String] = env.readTextFile(
      "C:\\\\D\\\\JAVA\\\\spark\\\\workbase\\\\spark\\\\hadoop\\\\flink\\\\in\\\\test00(1).txt")
      .flatMap(item => item.split(" "))

    val streamSplit: SplitStream[String] = stream.split(
      word =>
        ("hadoop".equals(word)) match {
          case true => List("hadoop")
          case false => List("othen")
        }
    )
    val streamSelect01: DataStream[String] = streamSplit.select("hadoop")
    val streamSelect02: DataStream[String] = streamSplit.select("othen")

    val streamUnion: DataStream[String] = streamSelect01.union(streamSelect02)

    streamUnion.print()

    env.execute("Transformation01")

  }

}
