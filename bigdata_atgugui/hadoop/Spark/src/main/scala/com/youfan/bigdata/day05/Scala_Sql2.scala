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


    val userRDD: RDD[User] = listRDD.map {
      case (age, name) => {
        User(age, name)
      }
    }

    import spark.implicits._

    val frame: DataFrame = userRDD.toDF()

    //    df.show()

    frame.createOrReplaceTempView("user")

    spark.sql("select * from user where age>=10").show()


    spark.stop()
  }

}

//case class User(age:Int,name:String)