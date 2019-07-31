package com.atguigu.bigdata.zhaocheng.util

import java.sql
import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

/**
  * Create by chenqinping on 2019/6/10 9:25
  */

class MyjdbcSink(sql: String) extends RichSinkFunction[Array[Any]] {
  val driver = "com.mysql.jdbc.Driver"

  val url = "jdbc:mysql://hadoop2:3306/gmall1128?useSSL=false"

  val username = "root"

  val password = "000000"


  var connection: Connection = null;


  override def open(parameters: Configuration): Unit = {

    Class.forName(driver)

    connection = DriverManager.getConnection(url, username, password)
  }


  override def invoke(values: Array[Any]): Unit = {
    val preparedStatement: PreparedStatement = connection.prepareStatement(sql)

    for (i <- 0 to values.size - 1) {

      preparedStatement.setObject(i + 1, values(i))
    }
    preparedStatement.executeUpdate()
  }

  //关闭连接
  override def close(): Unit = {
    if (connection != null) {
      connection.close()
    }

  }

}

