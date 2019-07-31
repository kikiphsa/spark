package com.youfan.bigdata.day05

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Create by chenqinping on 2019/5/13 14:39  and a.year= date(date(),'-1 a.year')
  */
object Scala_Sql10 {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("Scala_Sql10").setMaster("local[*]")

    //配置对象

    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()


    val jsonRDD: DataFrame = spark.read.json("C:\\D\\JAVA\\spark\\workbase\\spark\\hadoop\\Spark\\in\\user1.json")
    val frame: DataFrame = jsonRDD.toDF("age", "userid", "time")
    //    //    jsonRDD.show()


    frame.createOrReplaceTempView("User")

    //    spark.sql("select count(user_id) from Visit ").show()
    spark.sql("select  count(distinct(a.userid) )  from User a join User b on a.userid =b.userid  and a.time=date_sub(b.time,1)  ").show()
//        spark.sql("select * from User a").show()

    spark.stop()
  }

}
