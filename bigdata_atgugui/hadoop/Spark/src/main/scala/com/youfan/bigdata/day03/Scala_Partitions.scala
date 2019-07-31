package com.youfan.bigdata.day03

import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * Create by chenqinping on 2019/5/10 10:47
  */
object Scala_Partitions {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("Scala_Partitions").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

//    val list: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 34, 6, 6, 7, 8, 9))
    val listRDD: RDD[(String, Int)] = sc.makeRDD(List(("aaa",1),("bbb",2),("ccc",3)))

    val partitionBys: RDD[(String, Int)] = listRDD.partitionBy(new Mypartition(3))

    partitionBys.saveAsObjectFile("C:\\D\\JAVA\\spark\\workbase\\spark\\hadoop\\Spark\\output")

  }


}


class Mypartition(partitions: Int) extends Partitioner {
  def numPartitions: Int = {
    partitions
  }

  def getPartition(key: Any): Int = {
    1
  }
}