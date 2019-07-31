package com.youfan.bigdata.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Create by chenqinping on 2019/5/8 11:54
  */
object Spark_HelloWorld {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("Spark_HelloWorld").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    val unit: RDD[Int] = sc.makeRDD(List(1,2,3,4))

    val textRDD: RDD[String] = sc.textFile("C:\\D\\JAVA\\spark\\workbase\\spark\\hadoop\\Spark\\in/1.txt",2)

    textRDD.saveAsObjectFile("C:\\D\\JAVA\\spark\\workbase\\spark\\hadoop\\Spark\\output")
  }

}
