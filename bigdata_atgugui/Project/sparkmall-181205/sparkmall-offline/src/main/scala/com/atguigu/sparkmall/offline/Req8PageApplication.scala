package com.atguigu.sparkmall.offline

import com.atguigu.sparkmall.common.model.DateModel.UserVisitAction
import com.atguigu.sparkmall.common.util.{ConfigurationUtil, DateUtil, RedisUtil}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import redis.clients.jedis.Jedis

/**
  * Create by chenqinping on 2019/5/18 14:47
  * 网站的网页平均用户访问时间统计
  */
object Req8PageApplication {

  def main(args: Array[String]): Unit = {

    //创建sparkSession

    val sparkConf: SparkConf = new SparkConf().setAppName("Req8PageApplication").setMaster("local[*]")

    val spark: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    spark.sparkContext.setCheckpointDir("cp")

    import spark.implicits._


    //TODO 1 从hive中获取数据
    val startDate = ConfigurationUtil.getValueFromCondition("startDate")
    val endDate = ConfigurationUtil.getValueFromCondition("endDate")

    var sql = "select * from user_visit_action where 1=1 "
    if (startDate != null) {
      sql = sql + " and action_time >= '" + startDate + "' "
    }
    if (endDate != null) {
      sql = sql + " and action_time <= '" + endDate + "'"
    }
    spark.sql("use " + ConfigurationUtil.getValueFromConfig("hive.database"))

    val sqlDataFram: DataFrame = spark.sql(sql)

    println(sqlDataFram.count())


    val userDataSet: Dataset[UserVisitAction] = sqlDataFram.as[UserVisitAction]

    val actionRDD: RDD[UserVisitAction] = userDataSet.rdd

    //===========网站的页面平均访问时间===================


    //TODO 1. 获取kafka数据
    //TODO 2. 更具sessionid分组
    val sessionGroupByRDD: RDD[(String, Iterable[UserVisitAction])] = actionRDD.groupBy(action => action.session_id)

    //TODO 3.分组的数据进行排序,true(A,time1),(B,time2),(C,time3)

    val pageTimeRDD: RDD[(String, List[(Long, Long)])] = sessionGroupByRDD.mapValues(action => {
      val pageIdActionTimeRDD: List[(Long, String)] = action.toList.sortWith {
        case (r, l) => {
          r.action_time < l.action_time
        }
      }.map {
        case message => {
          (message.page_id, message.action_time)
        }
      }

      //TODO 4.0 排序后的数据进行拉链((A,time1),(B,time2))
      val zipList: List[((Long, String), (Long, String))] = pageIdActionTimeRDD.zip(pageIdActionTimeRDD.tail)

      //TODO 拉链后的数据进行筛选(A,(time2-time1))

      zipList.map {
        case (r, l) => {
          val time1: Long = DateUtil.parseStringToLong(r._2)
          val time2: Long = DateUtil.parseStringToLong(l._2)
          (r._1, time2 - time1)
        }
      }
    })

    //map
    val listRDD: RDD[List[(Long, Long)]] = pageTimeRDD.map {
      case (sessionid, list) => list
    }
    val pageAndTimeRdd: RDD[(Long, Long)] = listRDD.flatMap(list => list)


    //进行分组聚合
    val groupByPageTimeRDD: RDD[(Long, Iterable[Long])] = pageAndTimeRdd.groupByKey()


    //得到最终结果
    groupByPageTimeRDD.foreach {
      case (pageid, list) => {
        val list1: List[Long] = list.toList
        println(pageid + " = " + list1.sum.toDouble / list1.size)
      }
    }

    spark.stop()
  }

}
