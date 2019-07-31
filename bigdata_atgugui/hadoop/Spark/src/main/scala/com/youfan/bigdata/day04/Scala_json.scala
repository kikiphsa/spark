package com.youfan.bigdata.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Create by chenqinping on 2019/5/11 14:22
  */
import scala.util.parsing.json.JSON

object Scala_json {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("Scala_json").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    val jsonList: RDD[String] = sc.textFile("\"C:\\\\D\\\\JAVA\\\\spark\\\\workbase\\\\spark\\\\hadoop\\\\Spark\\\\in\\user.json")

    val jsonRDD: RDD[Option[Any]] = jsonList.map(JSON.parseFull)

    jsonRDD.collect.foreach(println)

    sc.stop()

  }
}
