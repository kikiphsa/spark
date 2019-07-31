package com.imooc.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

/**
  * Create by chenqinping on 2019/5/19 23:15
  */
object UpdateStateByKey {


  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("UpdateStateByKey").setMaster("local[*]")

    val ssc = new StreamingContext(sparkConf, Seconds(3))

    ssc.checkpoint(".")

    val ds: ReceiverInputDStream[String] = ssc.socketTextStream("linux", 9999)
    val result: DStream[(String, Int)] = ds.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    val state: DStream[(String, Int)] = result.updateStateByKey[Int](updateFunction _)

    state.print()

    ssc.start()
    ssc.awaitTermination()
  }

  def updateFunction(curr: Seq[Int], perValues: Option[Int]): Option[Int] = {
    val sum: Int = curr.sum
    val per: Int = perValues.getOrElse(0)

    Some(sum + per)
  }
}
