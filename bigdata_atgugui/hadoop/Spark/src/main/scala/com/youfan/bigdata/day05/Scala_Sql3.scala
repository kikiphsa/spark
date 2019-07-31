package com.youfan.bigdata.day05

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * Create by chenqinping on 2019/5/13 14:39
  */
object Scala_Sql3 {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("Scala_Sql3").setMaster("local[*]")

    //配置对象

    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()


    val listRDD: RDD[(Int, String)] = spark.sparkContext.makeRDD(List((1, "zahngs"), (10, "wu"), (40, "lisi")))

    import  spark.implicits._

    val userDF: DataFrame = listRDD.toDF()
    val userDS: Dataset[User] = userDF.as[User]

    userDS.show()

    val frameDF: DataFrame = userDS.toDF()

    frameDF.show()

    val rdd: RDD[Row] = frameDF.rdd

    rdd.collect()
  }
}

case class User(age:Int,name:String)