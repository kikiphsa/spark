package com.atguigu.sparkmall.common.util

import java.text.SimpleDateFormat
import java.util._

/**
  * Create by chenqinping on 2019/5/21 10:59
  */
object DateUtil {

  def main(args: Array[String]): Unit = {
    val l: Long = parseStringToLong("2019-05-07 23:22:00")
    println(l)
  }

  def parseDateToString(d: Date, f: String = "yyyy-MM-dd HH-mm-ss"): String = {

    val format = new SimpleDateFormat(f)
    format.format(d)
  }

  def parseStringToLong(d: String, f: String = "yyyy-MM-dd HH:mm:ss"): Long = {
    val format = new SimpleDateFormat(f)
    val time: Long = format.parse(d).getTime
    return time
  }

  def parseTimeToString(d: Long, f: String = "yyyy-MM-dd HH-mm-ss"): String = {
    parseDateToString(new Date(d), f)
  }

}
