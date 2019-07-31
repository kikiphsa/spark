package com.youfan.bigdata.day04

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Create by chenqinping on 2019/5/11 9:47
  */
object Scala_04 {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("Scala_04").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)



    sc.stop()

  }
}
