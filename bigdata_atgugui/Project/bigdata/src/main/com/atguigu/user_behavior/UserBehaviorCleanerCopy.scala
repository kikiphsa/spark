package com.atguigu.user_behavior

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Create by chenqinping on 2019/6/17 8:58
  */
object UserBehaviorCleanerCopy {


  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("输出输入不对")
      System.exit(1)
    }

    //获取路径
    val inputPath = args(0)
    val outputPath = args(1)

    val conf: SparkConf = new SparkConf().setAppName(this.getClass().getSimpleName).setMaster("local[*]")

    val sc = new SparkContext(conf)

    val textRDD: RDD[String] = sc.textFile(inputPath)


    textRDD.filter(event => LengthRDD(event))
      .map(event => konggang(event))
      .map(phone)
      .saveAsTextFile(outputPath)


    sc.stop()
  }

  def phone(event: String): String = {
    var buffer = new StringBuffer()
    val strings: Array[String] = event.split("\t")
    val str: String = strings(9)
    if (str != null || "".equals(str)) {
      buffer = buffer.append(str.substring(0, 3)).append("xxx").append(str.substring(7, 11))
      strings(9) = buffer.toString
    }
    strings.mkString("\t")
  }


  def konggang(event: String): String = {

    val strings: Array[String] = event.split("\t")
    val str: String = strings(1)
    if (str != null || "".equals(str)) {
      strings(1) = str.replace("\n", "")
    }
    strings.mkString("\t")
  }


  //判断长度是否17行
  def LengthRDD(event: String): Boolean = {
    val strings: Array[String] = event.split("\t")
    strings.length == 17
  }
}
