package com.youfan.bigdata.day04

import java.sql.Connection
import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Create by chenqinping on 2019/5/11 14:22
  */

object Scala_SharaData {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("Scala_SharaData").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    val listRDD: RDD[String] = sc.makeRDD(List("hive","hadoop","scala","spark"), 2)


    val accumulator: LongAccumulator = sc.longAccumulator

    var sum: Int = 0

    /* val rddRDD: Unit = listRDD.foreach {
       case (i) => {
         accumulator.add(i)
       }
     }

     println(accumulator.value)*/

    val wordAccumulator = new WordAccumulator()

    sc.register(wordAccumulator)


    listRDD.foreach{
      case (word)=>{
        wordAccumulator.add(word)
      }
    }

    println(wordAccumulator.value)

    sc.stop()

  }
}

class WordAccumulator extends AccumulatorV2[String, util.ArrayList[String]] {
  val list = new util.ArrayList[String]()


  //当前的累加器是否为初始化状态
  override def isZero: Boolean = list.isEmpty

  //复制累加器对象
  override def copy(): AccumulatorV2[String, util.ArrayList[String]] = {

    new WordAccumulator()
  }

  //重置累加器
  override def reset(): Unit = {
    list.clear()
  }

  //向累加器增加数据
  override def add(v: String): Unit = {
    if (v.contains("h")) {
      list.add(v)
    }
  }

  //合并累加器
  override def merge(other: AccumulatorV2[String, util.ArrayList[String]]): Unit = {
    list.addAll(other.value)
  }

  //获取数据
  override def value: util.ArrayList[String] = list
}
