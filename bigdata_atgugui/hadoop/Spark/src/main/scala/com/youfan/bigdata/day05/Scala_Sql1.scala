/*
package com.youfan.bigdata.day05

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Create by chenqinping on 2019/5/13 14:39
  */
object Scala_Sql1 {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("Scala_Sql1").setMaster("local[*]")

    //配置对象

    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()


    val listRDD: RDD[(Int, String)] = spark.sparkContext.makeRDD(List((1, "zahngs"), (10, "wu"), (40, "lisi")))

    import spark.implicits._

    val df: DataFrame = listRDD.toDF("age", "name")

    val user: Dataset[User] = df.as[User]

    user.show()

    //    df.show()

    df.createOrReplaceTempView("user")

    spark.sql("select * from user where age>=10").show()


    spark.stop()
  }

}

case class User(age:Int,name:String)*/
