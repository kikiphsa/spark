/*
package com.atguigu.bigdata.day04

import java.sql.{Connection, DriverManager}

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Create by chenqinping on 2019/5/11 14:22
  */

import scala.util.parsing.json.JSON

object Scala_mysql {

  def main(args: Array[String]): Unit = {
/*

    val sparkConf: SparkConf = new SparkConf().setAppName("Scala_mysql").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    //3.定义连接mysql的参数
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://hadoop102:3306/rdd"
    val userName = "root"
    val passWd = "000000"

   /* val rdd = new JdbcRDD(
      sc,
      () => {
        Class.forName(driver)
        DriverManager.getConnection(url, userName, passWd)
      },
      "select * from rddtable where id >=? and id<=?",
      1,
      10,
      3,
      r => (r.getInt(1), r.getString(2),r.getInt(3))
    )
    println(rdd.count())

    rdd.foreach(println)*/


    Class.forName(driver)
    val connection: Connection = java.sql.DriverManager.getConnection(url,userName,passWd)

    sc.stop()
*/

  }
}
*/
