package com.youfan.bigdata.day04

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}
import java.sql.{Connection, DriverManager, PreparedStatement}

/**
  * Create by chenqinping on 2019/5/12 20:16
  */
object Scala_mysql1 {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("Scala_mysql1").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    //3.定义连接mysql的参数
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://hadoop102:3306/rdd"
    val userName = "root"
    val passWd = "000000"

    /*//查询数据
    val rdd = new JdbcRDD(
      sc,
      () => {
        Class.forName(driver)
        DriverManager.getConnection(url, userName, passWd)
      },
      "select * from rddtable where id >=? and id<=?",
      1,
      10,
      3,
      r => (r.getInt(1), r.getString(2), r.getInt(3))
    )
    println(rdd.count())
    rdd.foreach(println)
    Class.forName(driver)
    val connection: Connection = java.sql.DriverManager.getConnection(url, userName, passWd)*/

    // 保存数据
    val dataRDD: RDD[(String, Int)] = sc.makeRDD(List(("zhangsan", 20), ("lisi", 30), ("wangwu", 40)), 2)

    dataRDD.foreachPartition(datas=>{
      Class.forName(driver)
      val connection: Connection = java.sql.DriverManager.getConnection(url, userName, passWd)
      datas.foreach{
        case ( username, age ) => {
          val sql = "insert into rddtable (name, age) values (?, ?) "
          val statement: PreparedStatement = connection.prepareStatement(sql)
          statement.setString(1, username)
          statement.setInt(2, age)
          statement.executeUpdate()

          statement.close()
        }
      }
      connection.close()
    })

    sc.stop()
  }
}
