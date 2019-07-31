package com.youfan.bigdata.day05

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Create by chenqinping on 2019/5/13 18:38
  */
object Scala_Set {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("Scala_Set").setMaster("local[*]")

    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._


    val pDS: Dataset[PersonSet] = Seq(PersonSet("daxianmg", 2)).toDS()

    pDS.createOrReplaceTempView("person")

    val sql: DataFrame = spark.sql("select * from person")

    sql.show()

  }

}

case class PersonSet(name: String, age: Long)