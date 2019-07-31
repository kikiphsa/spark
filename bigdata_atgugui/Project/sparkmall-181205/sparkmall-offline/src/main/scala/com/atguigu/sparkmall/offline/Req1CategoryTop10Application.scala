package com.atguigu.sparkmall.offline

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.UUID

import com.atguigu.sparkmall.common.model.DateModel.UserVisitAction
import com.atguigu.sparkmall.common.util.ConfigurationUtil
import com.atguigu.sparkmall.common.util.ConfigurationUtil.getValueFromCondition
import org.apache.hadoop.hive.ql.udf.UDAFPercentile.MyComparator
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.util.AccumulatorV2

import scala.collection.{immutable, mutable}

/**
  * Create by chenqinping on 2019/5/18 14:47
  */
object Req1CategoryTop10Application {

  def main(args: Array[String]): Unit = {

    //创建sparkSession

    val sparkConf: SparkConf = new SparkConf().setAppName("Req1CategoryTop10Application").setMaster("local[*]")

    val spark: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

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


    //TODO 2申明累加器


    //注册累加器

    val acc = new CategoryAccumulator()

    spark.sparkContext.register(acc)

    // 获取累加器的结果
    actionRDD.foreach(action => {
      if (action.click_category_id != -1) {
        acc.add(action.click_category_id + "_click")
      } else {
        if (action.order_product_ids != null) {

          val ids: Array[String] = action.order_product_ids.split(",")
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
    //    categorySumCount.foreach(println)
    val categoryGroup: Map[String, mutable.HashMap[String, Long]] = categorySumCount.groupBy {
      case (category, sum) => {
        category.split("_")(0)
      }
    }
    //    categoryGroup.foreach(println)
    //聚合 (1,Map(1_order -> 2571, 1_click -> 1335, 1_pay -> 1756))
    val taskid: String = UUID.randomUUID().toString

    //CategoryTop10(df0cbcec-f593-4a34-899f-95712b95e3cd,2,306,538,347)
    val categoryTop: immutable.Iterable[CategoryTop10] = categoryGroup.map {
      case (categoryId, groupMap) => {
        CategoryTop10(taskid, categoryId, groupMap.getOrElse(categoryId + "_click", 0), groupMap.getOrElse(categoryId + "_order", 0)
          , groupMap.getOrElse(categoryId + "_pay", 0))
      }
    }
    //    categoryTop.foreach(println)
    //TODO 4.对转换后的数据进行排序
    val top1s: List[CategoryTop10] = categoryTop.toList.sortWith {
      case (r, l) => {
        if (r.clickCount > l.clickCount) {
          true
        } else if (r.clickCount == l.clickCount) {
          if (r.orderCount > l.orderCount) {
            true
          } else if (r.orderCount == l.orderCount) {
            r.payCount > l.payCount
          } else {
            false
          }
        } else {
          false
        }
      }
    }


    //TODO 5.获取前10
    val top10s: List[CategoryTop10] = top1s.take(10)
    top10s.foreach(println)
    //TODO 6.将统计结果保存到mysql中

    val driverClass = ConfigurationUtil.getValueFromConfig("jdbc.driver.class")
    val url = ConfigurationUtil.getValueFromConfig("jdbc.url")
    val user = ConfigurationUtil.getValueFromConfig("jdbc.user")
    val password = ConfigurationUtil.getValueFromConfig("jdbc.password")

    Class.forName(driverClass)

    val connection: Connection = DriverManager.getConnection(url, user, password)

    val statement: PreparedStatement = connection.prepareStatement("insert into category_top10 values ( ?, ?, ?, ?, ? )")

    top10s.foreach(data => {
      statement.setString(1, data.taskId)
      statement.setString(2, data.categoryId)
      statement.setLong(3, data.clickCount)
      statement.setLong(4, data.orderCount)
      statement.setLong(5, data.payCount)
      statement.executeUpdate()
    })

    spark.stop()
  }

}

import scala.collection.mutable

//申明累加器

case class CategoryTop10(taskId: String, categoryId: String, clickCount: Long, orderCount: Long, payCount: Long)

class CategoryAccumulator extends AccumulatorV2[String, mutable.HashMap[String, Long]] {

  var map = new mutable.HashMap[String, Long]

  override def isZero: Boolean = {
    map.isEmpty
  }

  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = {
    new CategoryAccumulator()
  }

  override def reset(): Unit = {
    map.clear()
  }

  override def add(v: String): Unit = {
    map(v) = map.getOrElse(v, 0L) + 1
  }

  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {
    val map1 = map
    val map2 = other.value

    map = map1
      .foldLeft(map2) {
        case (tempMap, (k, sumCount)) => {
          tempMap(k) = tempMap.getOrElse(k, 0L) + sumCount
          tempMap
        }
      }
  }

  override def value: mutable.HashMap[String, Long] = map

}
