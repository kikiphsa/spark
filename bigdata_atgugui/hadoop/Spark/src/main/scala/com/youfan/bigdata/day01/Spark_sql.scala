package com.youfan.bigdata.day01

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Create by chenqinping on 2019/5/7 20:52
  */
object Spark_sql {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Spark_sql")
      .master("local[*]")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()


    import spark.implicits._

    val df: DataFrame = spark.read.json("C:\\D\\JAVA\\spark\\workbase\\spark\\hadoop\\Spark\\in/people.json")

    df.show()

    df.filter($"age" > 20).show()

    df.createOrReplaceTempView("People")

    val frame: DataFrame = spark.sql("select * from People where age>20")

    frame.show()

    spark.stop()
  }

}
