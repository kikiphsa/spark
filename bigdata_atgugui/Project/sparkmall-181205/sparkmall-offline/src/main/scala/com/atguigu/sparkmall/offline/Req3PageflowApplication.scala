package com.atguigu.sparkmall.offline

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.UUID

import com.atguigu.sparkmall.common.model.DateModel.UserVisitAction
import com.atguigu.sparkmall.common.util.{ConfigurationUtil, Mysql}
import com.atguigu.sparkmall.common.util.ConfigurationUtil.getValueFromCondition
import org.apache.hadoop.hive.ql.udf.UDAFPercentile.MyComparator
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.util.AccumulatorV2
import redis.clients.jedis.Jedis

import scala.collection.{immutable, mutable}

/**
  * Create by chenqinping on 2019/5/18 14:47
  * 页面单跳转化率统计
  */
object Req3PageflowApplication {

  def main(args: Array[String]): Unit = {

    //创建sparkSession

    val sparkConf: SparkConf = new SparkConf().setAppName("Req3PageflowApplication").setMaster("local[*]")

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

    //TODO 设置缓存
    actionRDD.checkpoint()

    //TODO 1.获取用户数据 进行过滤,保留需要的数据
    val pageidList: String = ConfigurationUtil.getValueFromCondition("targetPageFlow")

    val pageids: Array[String] = pageidList.split(",")
    val zipPageIds: Array[String] = pageids.zip(pageids.tail) map {
      case (pid1, pid2) => {
        (pid1 + "-" + pid2)
      }
    }

    val pageIdUserRDD: RDD[UserVisitAction] = actionRDD.filter(action => {
      pageids.contains(action.page_id.toString)
    })
    println(pageIdUserRDD.count())

    //TODO 2.0 将每一个页面的点击进行聚合，获取结果的分母数据
    val pageIdRDD: RDD[(Long, Int)] = pageIdUserRDD.map {
      case action => {
        (action.page_id, 1)
      }
    }
    //reduce    (1,142)
    val pageIdReduceRDD: RDD[(Long, Int)] = pageIdRDD.reduceByKey(_ + _)


    //转化成topMap 后面获取
    val pageidMap: Map[Long, Int] = pageIdReduceRDD.collect.toMap

    //TODO 计算分子数据 将数据保存的检查点
    //TODO 3.0 取用户访问数据，对sessionid进行分组 (1aab3073-14d5-4c4c-a281-2f47f4c9ff58,CompactBuffer(UserVisitAction(2019-05-21,24,1aab3073-14d5-4c4c-a281-2f47f4c9ff58,5,2019-05-21 17:43:20,null,8,31,null,null,null,null,7), UserVisitAction(2019-05-21,24,1aab3073-14d5-4c4c-a281-2f47f4c9ff58,42,2019-05-21 17:51:09,null,7,37,null,null,null,null,17), UserVisitAction(2019-05-21,24,1aab3073-14d5-4c4c-a281-2f47f4c9ff58,8,2019-05-21 17:51:44,吃鸡,-1,-1,null,null,null,null,21), UserVisitAction(2019-05-21,24,1aab3073-14d5-4c4c-a281-2f47f4c9ff58,25,2019-05-21 17:55:00,null,13,77,null,null,null,null,8), UserVisitAction(2019-05-21,24,1aab3073-14d5-4c4c-a281-2f47f4c9ff58,31,2019-05-21 18:05:23,null,-1,-1,null,null,1,2,3,1,2,3,6), UserVisitAction(2019-05-21,24,1aab3073-14d5-4c4c-a281-2f47f4c9ff58,11,2019-05-21 18:05:31,null,4,22,null,null,null,null,11), UserVisitAction(2019-05-21,24,1aab3073-14d5-4c4c-a281-2f47f4c9ff58,49,2019-05-21 18:10:18,null,15,86,null,null,null,null,3), UserVisitAction(2019-05-21,24,1aab3073-14d5-4c4c-a281-2f47f4c9ff58,25,2019-05-21 18:13:06,null,17,60,null,null,null,null,14), UserVisitAction(2019-05-21,24,1aab3073-14d5-4c4c-a281-2f47f4c9ff58,23,2019-05-21 18:13:57,null,12,14,null,null,null,null,4), UserVisitAction(2019-05-21,24,1aab3073-14d5-4c4c-a281-2f47f4c9ff58,31,2019-05-21 18:14:22,null,13,76,null,null,null,null,3), UserVisitAction(2019-05-21,24,1aab3073-14d5-4c4c-a281-2f47f4c9ff58,33,2019-05-21 18:24:33,i7,-1,-1,null,null,null,null,19), UserVisitAction(2019-05-21,24,1aab3073-14d5-4c4c-a281-2f47f4c9ff58,32,2019-05-21 18:35:02,null,18,50,null,null,null,null,7), UserVisitAction(2019-05-21,24,1aab3073-14d5-4c4c-a281-2f47f4c9ff58,47,2019-05-21 18:38:07,null,-1,-1,null,null,1,2,3,1,2,3,12), UserVisitAction(2019-05-21,24,1aab3073-14d5-4c4c-a281-2f47f4c9ff58,42,2019-05-21 18:44:50,笔记本,-1,-1,null,null,null,null,8), UserVisitAction(2019-05-21,24,1aab3073-14d5-4c4c-a281-2f47f4c9ff58,14,2019-05-21 18:53:20,null,8,46,null,null,null,null,21), UserVisitAction(2019-05-21,24,1aab3073-14d5-4c4c-a281-2f47f4c9ff58,3,2019-05-21 19:02:13,null,20,1,null,null,null,null,26), UserVisitAction(2019-05-21,24,1aab3073-14d5-4c4c-a281-2f47f4c9ff58,19,2019-05-21 19:07:37,null,7,33,null,null,null,null,5), UserVisitAction(2019-05-21,24,1aab3073-14d5-4c4c-a281-2f47f4c9ff58,8,2019-05-21 19:09:35,null,2,9,null,null,null,null,3), UserVisitAction(2019-05-21,24,1aab3073-14d5-4c4c-a281-2f47f4c9ff58,33,2019-05-21 19:17:57,null,15,27,null,null,null,null,9), UserVisitAction(2019-05-21,24,1aab3073-14d5-4c4c-a281-2f47f4c9ff58,15,2019-05-21 19:22:33,吃鸡,-1,-1,null,null,null,null,18), UserVisitAction(2019-05-21,24,1aab3073-14d5-4c4c-a281-2f47f4c9ff58,38,2019-05-21 19:30:38,null,2,62,null,null,null,null,26), UserVisitAction(2019-05-21,24,1aab3073-14d5-4c4c-a281-2f47f4c9ff58,22,2019-05-21 19:37:37,null,2,53,null,null,null,null,25), UserVisitAction(2019-05-21,24,1aab3073-14d5-4c4c-a281-2f47f4c9ff58,20,2019-05-21 19:38:42,null,5,69,null,null,null,null,2), UserVisitAction(2019-05-21,24,1aab3073-14d5-4c4c-a281-2f47f4c9ff58,9,2019-05-21 19:41:29,null,9,7,null,null,null,null,18), UserVisitAction(2019-05-21,24,1aab3073-14d5-4c4c-a281-2f47f4c9ff58,34,2019-05-21 19:44:18,null,1,83,null,null,null,null,6), UserVisitAction(2019-05-21,24,1aab3073-14d5-4c4c-a281-2f47f4c9ff58,28,2019-05-21 19:53:31,内存,-1,-1,null,null,null,null,9), UserVisitAction(2019-05-21,24,1aab3073-14d5-4c4c-a281-2f47f4c9ff58,30,2019-05-21 20:02:45,null,14,5,null,null,null,null,21), UserVisitAction(2019-05-21,24,1aab3073-14d5-4c4c-a281-2f47f4c9ff58,46,2019-05-21 20:05:49,null,20,45,null,null,null,null,14)))
    val sessionidgroup: RDD[(String, Iterable[UserVisitAction])] = actionRDD.groupBy(action => action.session_id)

    //TODO 4 分组后的数据进行时间排序（升序）
    //(7b23194e-213d-4a5a-b963-82b1c5c7066b,List((48,48), (48,4), (4,45), (45,4), (4,22), (22,7), (7,45), (45,45), (45,30), (30,30), (30,22), (22,50), (50,26), (26,39), (39,20)))
    //    zipPageIds.foreach(println)
    val zipRDD: RDD[(String, List[(Long, Long)])] = sessionidgroup.mapValues(datas => {
      val actions: List[UserVisitAction] = datas.toList.sortWith {
        case (r, l) => {
          r.action_time < l.action_time
        }
      }
      val pageids: List[Long] = actions.map(_.page_id)
      val zipPageid: List[(Long, Long)] = pageids.zip(pageids.tail)
      zipPageid
    })
    //    zipRDD.foreach(println)

    //TODO  5将排序后的页面数据进行 拉练表处理 List((24,40), (40,2), (2,7)) （12,23,34
    val zipRDDs: RDD[List[(Long, Long)]] = zipRDD.map(_._2)

    //flatMap
    val zipMapRDD: RDD[(Long, Long)] = zipRDDs.flatMap(list => list)

    val filterZip: RDD[(Long, Long)] = zipMapRDD.filter {
      case (pid1, pid2) => {
        zipPageIds.contains(pid1 + "-" + pid2)
      }
    }

    //TODO 6  将拉链后的数据进行结构转换（  （12, 1）, （23, 1） ）
    //TODO 7 将转环后的数据进行reduceByKey
    val reducePageidSum: RDD[(String, Int)] = filterZip.map {
      case (pid1, pid2) => {
        ((pid1 + "-" + pid2), 1)
      }
    }.reduceByKey(_ + _)
    //    reducePageidSum.foreach(println)


    //TODO 8 将分子数据除以分母 获取最终结果
    reducePageidSum.foreach {
      case (pids, sum) => {
        val pidA: String = pids.split("-")(0)
        println(pids + " = " + (sum.toDouble / pageidMap(pidA.toLong)))
        val jedis = new Jedis("hadoop102", 6379)
        val d: Double = sum.toDouble / pageidMap(pidA.toLong)
        val key = "p" + d
        jedis.sadd(key, d.toString)
      }
    }

    spark.stop()
  }

}
