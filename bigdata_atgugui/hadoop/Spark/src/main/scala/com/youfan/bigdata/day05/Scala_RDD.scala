package com.youfan.bigdata.day05

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Create by chenqinping on 2019/5/13 18:25
  */
object Scala_RDD {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("Scala_RDD").setMaster("local[*]")

    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val listRDD: RDD[String] = spark.sparkContext.textFile("C:\\D\\JAVA\\spark\\workbase\\spark\\hadoop\\Spark\\in\\people.txt")

    val personRDD: RDD[Person] = listRDD.map (
      x => {
        val words: Array[String] = x.split(",")
        words.foreach(println)
        Person(words(0), words(1).trim.toLong)
      }
    )
    import spark.implicits._
    val personDF: DataFrame = personRDD.toDF()

    val person: Dataset[Person] = personDF.as[Person]

    person.show()
  }

}

case class Person(name: String, age: Long)