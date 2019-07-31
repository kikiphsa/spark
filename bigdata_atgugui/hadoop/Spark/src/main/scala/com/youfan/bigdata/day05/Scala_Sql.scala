package com.youfan.bigdata.day05

import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Create by chenqinping on 2019/5/13 14:39
  */
object Scala_Sql {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("Scala_Sql").setMaster("local[*]")

    //配置对象

    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()


/*    val jsonRDD: DataFrame = spark.read.json("C:\\D\\JAVA\\spark\\workbase\\spark\\hadoop\\Spark\\in\\user.json")
    val frame: DataFrame = jsonRDD.toDF("user_id", "shop")
    //    jsonRDD.show()


    frame.createOrReplaceTempView("Visit")

    //    spark.sql("select count(user_id) from Visit ").show()
    spark.sql(" select shop,count(user_id) as userCount, user_id from Visit group by shop,user_id order by userCount desc limit 3  ").show()*/



    val dataFrame: DataFrame = spark.read.json("C:\\\\D\\\\JAVA\\\\spark\\\\workbase\\\\spark\\\\hadoop\\\\Spark\\\\in\\\\user.json")
    val frame: DataFrame = dataFrame.toDF("uid","age")


//    frame.write.parquet()

    frame.createOrReplaceTempView("visit")

    import org.apache.spark.sql.SaveMode
    //jsonDF.show();//jsonDF.show();

//    jsonDF.coalesce(1).write.mode(SaveMode.Append).format("parquet").insertInto //按Parquet格式存储
//    props.getProperty("bj_bd_usergasorder_hivetable")



    spark.sql("select * from visit where age >=20").show()

    spark.stop()
  }

}
