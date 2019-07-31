package com.youfan.bigdata.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Create by chenqinping on 2019/5/10 15:46
  */
object Practice {
  def main(args: Array[String]): Unit = {
    //    时间戳，省份，城市，用户，广告，中间字段使用空格分割
    //    1516609143867 6 7 64 16
    ///需求：统计出每一个省份广告被点击次数的TOP3
    val sparkConf: SparkConf = new SparkConf().setAppName("Practice").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    val list: RDD[String] = sc.textFile("C:\\D\\JAVA\\spark\\workbase\\spark\\hadoop\\Spark\\in\\agent.log")


    list.map{
      case(x)=>{
        val words: Array[String] = x.split(" ")
        (words(0)+"-"+words(4),1)
      }
    }.reduceByKey(_+_).map{
      case (x,y)=>{
        val pvAndadv: Array[String] = x.split("-")
        (pvAndadv(0),(pvAndadv(1),y))
      }
    }.groupByKey().mapValues{
      case (y)=>{
        y.toList.sortWith{
          case(l,r)=>{
            l._2>r._2
          }
        }
      }.take(3)
    }.collect().foreach(println)

    //9.关闭与spark的连接
    sc.stop()

  }

}
