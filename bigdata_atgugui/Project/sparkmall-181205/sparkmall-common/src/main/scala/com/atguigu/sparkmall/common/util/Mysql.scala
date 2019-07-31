package com.atguigu.sparkmall.common.util

import java.sql.{Connection, DriverManager}

/**
  * Create by chenqinping on 2019/5/20 10:01
  */
object Mysql {

  val driverClass = ConfigurationUtil.getValueFromConfig("jdbc.driver.class")
  val url = ConfigurationUtil.getValueFromConfig("jdbc.url")
  val user = ConfigurationUtil.getValueFromConfig("jdbc.user")
  val password = ConfigurationUtil.getValueFromConfig("jdbc.password")

  Class.forName(driverClass)

  val connection: Connection = DriverManager.getConnection(url, user, password)

}
