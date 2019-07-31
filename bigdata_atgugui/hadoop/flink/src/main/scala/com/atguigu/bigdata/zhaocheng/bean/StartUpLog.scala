package com.atguigu.bigdata.zhaocheng.bean

/**
  * Create by chenqinping on 2019/5/15 19:56
  */
case class StartUpLog(mid: String,
                      uid: String,
                      appid: String,
                      area: String,
                      os: String,
                      ch: String,
                      logType: String,
                      vs: String,
                      var logDate: String,
                      var logHour: String,
                      var logHourMinute: String,
                      var ts: Long
                     ) {

}
