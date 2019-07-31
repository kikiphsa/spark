package com.atguigu.sparkmall.copy

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.UUID

import com.atguigu.sparkmall.common.model.DateModel.UserVisitAction
import com.atguigu.sparkmall.common.util.ConfigurationUtil
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.util.AccumulatorV2

import scala.collection.{immutable, mutable}

///获取点击、下单和支付数量排名前 10 的品类
/**
  * Create by chenqinping on 2019/5/18 14:47
  */
object CopyReq1CategoryTop10SessionApplication {

  def main(args: Array[String]): Unit = {

    //创建sparkSession
    val sparkConf: SparkConf = new SparkConf().setAppName("CopyReq1CategoryTop10SessionApplication").setMaster("local[*]")

    val spark: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    import spark.implicits._

    //TODO 1 从hive中获取数据
    val startDate: String = ConfigurationUtil.getValueFromCondition("startDate")
    val endDate: String = ConfigurationUtil.getValueFromCondition("endDate")
    var sql = "select * from user_visit_action where 1=1 "

    if (startDate != null) {
      sql = sql + "and action_time >='" + startDate + "'"
    }

    if (endDate != null) {
      sql = sql + "and action_time <='" + endDate + "'"
    }

    spark.sql("use " + ConfigurationUtil.getValueFromConfig("hive.database"))

    val sqlDataFrame: DataFrame = spark.sql(sql)

    println(sqlDataFrame.count())

    val dateSetUserVisitAction: Dataset[UserVisitAction] = sqlDataFrame.as[UserVisitAction]

    val actionRDD: RDD[UserVisitAction] = dateSetUserVisitAction.rdd

    //TODO 2申明累加器
    //注册累加器
    val acc = new CategoeyAccumulatorV2()

    actionRDD.sparkContext.register(acc)

    // 获取累加器的结果

    actionRDD.foreach(action => {
      if (action.click_category_id != -1) {
        acc.add(action.click_category_id + "_click")
      } else {
        if (action.order_category_ids != null) {
          val ids: Array[String] = action.order_category_ids.split(",")
          for (elem <- ids) {
            acc.add(elem + "_order")
          }
        } else {
          if (action.pay_category_ids != null) {
            val ids: Array[String] = action.pay_category_ids.split(",")
            for (elem <- ids) {
              acc.add(elem + "_pay")
            }
          }
        }
      }
    })

    //(9_click,1348)
    val categorySumCount: mutable.HashMap[String, Long] = acc.value
    println(categorySumCount.size)

    //聚合 (1,Map(1_order -> 2571, 1_click -> 1335, 1_pay -> 1756))
    val stringToStringToLong: Map[String, mutable.HashMap[String, Long]] = categorySumCount.groupBy {
      case (cagetory, sum) => {
        cagetory.split("_")(0)
      }
    }
    //    stringToStringToLong.foreach(println)

    //CategoryTop10(df0cbcec-f593-4a34-899f-95712b95e3cd,2,306,538,347)

    // TODO 4.3 将累加器的数据转化为单一的数据对象
    val taskId: String = UUID.randomUUID().toString
    val categoryTop10s: immutable.Iterable[CopyCategoryTop10] = stringToStringToLong.map {
      case (cagetoryId, groupMap) => {
        CopyCategoryTop10(taskId, cagetoryId, groupMap.getOrElse(cagetoryId + "_click", 0), groupMap.getOrElse(cagetoryId + "_order", 0),
          groupMap.getOrElse(cagetoryId + "_pay", 0))
      }
    }

    //    categoryTop10s.toList.foreach(println)

    // TODO 4.4 对转换后的数据进行排序（点击，下单，支付）
    val top10s: List[CopyCategoryTop10] = categoryTop10s.toList.sortWith {
      case (r, l) => {
        if (r.clickCount > l.clickCount) {
          true
        } else if (r.clickCount == l.clickCount) {
          if (r.orderCount > l.orderCount) {
            true
          } else if (r.clickCount == l.clickCount) {
            r.payCount > l.payCount
          } else {
            false
          }
        } else {
          false
        }
      }
    }


    // TODO 4.5 获取前10名的数据
    val top1s: List[CopyCategoryTop10] = top10s.take(10)

    //    top1s.foreach(println)


    //TODO ******************需求二 start*******************************
    //TODO ******************需求二 start*******************************
    //TODO ******************需求二 start*******************************
    //TODO ******************需求二 start*******************************
    /**
      * Top10 热门品类中 Top10 活跃 Session 统计
      * 1 需求简介
      * 对于排名前 10 的品类，分别获取其点击次数排名前 10 的 sessionId。
      */
    val ids: List[String] = top1s.map(action => action.categoryId)

    //TODO 2 对数据进行filter
    val filterUserVisitAction: RDD[UserVisitAction] = actionRDD.filter(action => {
      if (action.click_category_id != -1) {
        ids.contains(action.click_category_id.toString)
      } else {
        false
      }
    })
    println("count" + filterUserVisitAction.count())

    //TODO 3 将筛选过滤的数据进行结构的转换（categoryid, sessionid, click）( categoryid-sessionid,1 )
    val rddNumOne: RDD[(String, Int)] = filterUserVisitAction.map(action => {
      (action.click_category_id + "_" + action.session_id, 1)
    })

    //TODO 4 数据按照key进行聚合( categoryid-sessionid,1 ) ( categoryid-sessionid,sum)
    val rddReduceSum: RDD[(String, Int)] = rddNumOne.reduceByKey(_ + _)

    //TODO 将聚合后的数据进行结构转换( categoryid-sessionid,sum) ( categoryid, (sessionid,sum))
    val categoryIdSum: RDD[(String, (String, Int))] = rddReduceSum.map {
      case (k, sum) => {
        val spilt: Array[String] = k.split("_")
        (spilt(0), (spilt(1), sum))
      }
    }


    //TODO按照key进行分组
    val rddGroupByKey: RDD[(String, Iterable[(String, Int)])] = categoryIdSum.groupByKey()

    /*val value: RDD[(String, Iterable[(String, (String, Int))])] = categoryIdSum.groupBy {
      case (k, v) => {
        k
      }
    }*/
    //    groupBYRDD.foreach(println)
    //TODO 6.0 获取value 排序和take10
    val sortRDD: RDD[(String, List[(String, Int)])] = rddGroupByKey.mapValues(action => {
      action.toList.sortWith {
        case (r, l) => {
          r._2 > l._2
        }
      }.take(10)
    })

    //TODO 7.0 保存到bean
    sortRDD.map {
      case (caegoryId, list) => {
        list.map {
          case (sessionid, sum) => {
            CategoryTop10Session102(taskId, caegoryId, sessionid, sum)
          }
        }
      }
    }
    //扁平化

    //TODO 8.0 保存到mysql


    /* // TODO 4.6 将统计结果保存到Mysql中
     val driverClass = ConfigurationUtil.getValueFromConfig("jdbc.driver.class")
     val url = ConfigurationUtil.getValueFromConfig("jdbc.url")
     val user = ConfigurationUtil.getValueFromConfig("jdbc.user")
     val password = ConfigurationUtil.getValueFromConfig("jdbc.password")

     Class.forName(driverClass)

     val connection: Connection = DriverManager.getConnection(url, user, password)

     val statement: PreparedStatement = connection.prepareStatement("insert into category_top10 values(?,?,?,?,?)")

     top10s.foreach(data => {
       statement.setString(1, data.taskId)
       statement.setString(2, data.categoryId)
       statement.setLong(3, data.clickCount)
       statement.setLong(4, data.orderCount)
       statement.setLong(5, data.payCount)
       statement.executeUpdate()
     })
 */
    spark.stop()
  }

}

case class CopyCategoryTop10(taskId: String, categoryId: String, clickCount: Long, orderCount: Long, payCount: Long)

case class CategoryTop10Session102(taskId: String, categoryId: String, sessionId: String, clickCount: Int)

//申明累加器
import scala.collection.mutable

class CategoeyAccumulatorV2 extends AccumulatorV2[String, mutable.HashMap[String, Long]] {
  var map = new mutable.HashMap[String, Long]

  override def isZero: Boolean = {
    map.isEmpty
  }

  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = {
    new CategoeyAccumulatorV2
  }

  override def reset(): Unit = {
    map.clear()
  }

  override def add(v: String): Unit = {
    map(v) = map.getOrElse(v, 0L) + 1
  }

  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {
    var map1 = map
    var map2 = other.value

    map = map1.foldLeft(map2) {
      case (tempcagetoey, (k, sumCount)) => {
        tempcagetoey(k) = tempcagetoey.getOrElse(k, 0L) + sumCount
        tempcagetoey
      }
    }
  }

  override def value: mutable.HashMap[String, Long] = map


}