package com.youfan.gmall1018.dw.dw2es.app

import com.youfan.gmall1018.dw.common.constant.GmallConstant
import com.youfan.gmall1018.dw.common.util.MyEsUtil
import com.youfan.gmall1018.dw.dw2es.bean.SaleDetailDaycount
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

/**
  * Create by chenqinping on 2019/5/9 10:25
  */
object Dw2esSaleDetailApp {

  def main(args: Array[String]): Unit = {

    var date = ""

    if (args != null && args.length != 0) {
      date = args(0)
    } else {
      date = "2019-02-11"
    }

    val sparkConf: SparkConf = new SparkConf().setAppName("Dw2esSaleDetailApp").setMaster("local[*]")

    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    sparkSession.sql("use gmall")

    import sparkSession.implicits._

    val saleDetailRDD: RDD[SaleDetailDaycount] = sparkSession.sql(
      "select   user_id,sku_id,user_gender,cast(user_age as int) user_age,user_level," +
        "cast(order_price as double) order_price,sku_name,sku_tm_id, sku_category3_id,sku_category2_id,sku_category1_id,sku_category3_name," +
        "sku_category2_name,sku_category1_name,spu_id,sku_num,cast(order_count as bigint) order_count,cast(order_amount as double) order_amount," +
        "dt from dws_sale_detail_daycount where dt='"+date+"'").as[SaleDetailDaycount].rdd


    saleDetailRDD.foreachPartition { saleDetail =>

      val listBuffe = new ListBuffer[SaleDetailDaycount]

      for (sale <- saleDetail) {
        listBuffe += sale
        if (listBuffe.size == 100) {
          //到100条保存
          MyEsUtil.executeIndexBulk(GmallConstant.ES_INDEX_SALE, listBuffe.toList)
          listBuffe.clear()
        }
      }
      //保存剩余的数据
      MyEsUtil.executeIndexBulk(GmallConstant.ES_INDEX_SALE, listBuffe.toList)
    }
  }

}
